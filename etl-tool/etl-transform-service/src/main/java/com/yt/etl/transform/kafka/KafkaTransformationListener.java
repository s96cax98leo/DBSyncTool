package com.yt.etl.transform.kafka;

import com.yt.etl.common.model.DataRecordBatch;
import com.yt.etl.common.model.JobTransformationConfig;
import com.yt.etl.transform.service.RecordTransformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
public class KafkaTransformationListener {

    private static final Logger logger = LoggerFactory.getLogger(KafkaTransformationListener.class);

    private final KafkaTemplate<String, DataRecordBatch> kafkaTemplate;
    private final RecordTransformer recordTransformer;

    @Value("${etl.topics.output.transformed}")
    private String outputTopic;

    @Autowired
    public KafkaTransformationListener(KafkaTemplate<String, DataRecordBatch> kafkaTemplate, RecordTransformer recordTransformer) {
        this.kafkaTemplate = kafkaTemplate;
        this.recordTransformer = recordTransformer;
    }

    @KafkaListener(topics = "${etl.topics.input.untransformed}", groupId = "${spring.kafka.consumer.group-id}")
    public void handleDataBatch(@Payload DataRecordBatch incomingBatch,
                                @Header(KafkaHeaders.RECEIVED_KEY) String key, // Use Kafka message key as JobId if not in payload
                                @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                @Header(KafkaHeaders.OFFSET) long offset) {

        logger.info("Received batch from topic {}: key='{}', jobId='{}', batchId='{}', tableId='{}', records_count={}, offset={}",
                topic, key, incomingBatch.getJobId(), incomingBatch.getBatchId(), incomingBatch.getTableId(),
                incomingBatch.getRecords() != null ? incomingBatch.getRecords().size() : 0, offset);

        if (incomingBatch.getRecords() == null || incomingBatch.getRecords().isEmpty()) {
            logger.warn("Received an empty or null record list in batchId {}. Skipping transformation.", incomingBatch.getBatchId());
            // Optionally send an empty batch downstream or a status update
            return;
        }

        JobTransformationConfig config = incomingBatch.getJobTransformationConfig();
        if (config == null || config.getRules() == null || config.getRules().isEmpty()) {
            logger.warn("No transformation rules found in batchId {}. Producing records as is to output topic.", incomingBatch.getBatchId());
            // If no rules, we might still want to change tableId or route it, or just pass through
            // For now, pass through with potentially new batchId.
            // This also implies the targetTableId should be the same as source if not specified by rules (which they can't be here)
             DataRecordBatch outgoingBatch = new DataRecordBatch(
                incomingBatch.getJobId(),
                UUID.randomUUID().toString(), // New batchId for the outgoing message
                incomingBatch.getTableId(),
                incomingBatch.getTargetTableId() != null ? incomingBatch.getTargetTableId() : incomingBatch.getTableId(), // Use target or default to source
                null, // No transformations were applied
                incomingBatch.getRecords(), // Original records
                incomingBatch.isLastBatch(),
                incomingBatch.getSequenceNumber()
            );
            kafkaTemplate.send(outputTopic, outgoingBatch.getJobId(), outgoingBatch);
            logger.info("Sent un-transformed batch (due to no rules) to {}: jobId='{}', new batchId='{}'", outputTopic, outgoingBatch.getJobId(), outgoingBatch.getBatchId());
            return;
        }


        List<Map<String, Object>> originalRecords = incomingBatch.getRecords();
        List<Map<String, Object>> transformedRecords = recordTransformer.transformRecords(originalRecords, config);

        // Determine target table ID. This could potentially be part of transformation config in more advanced scenarios.
        String targetTableId = incomingBatch.getTargetTableId() != null ? incomingBatch.getTargetTableId() : incomingBatch.getTableId();


        DataRecordBatch outgoingBatch = new DataRecordBatch(
                incomingBatch.getJobId(),
                UUID.randomUUID().toString(), // Generate a new batchId for the transformed batch
                incomingBatch.getTableId(),   // Original source tableId
                targetTableId,                // Target tableId
                config,                       // Could be null, or send the applied config, or a summary
                transformedRecords,
                incomingBatch.isLastBatch(),
                incomingBatch.getSequenceNumber()
        );

        try {
            kafkaTemplate.send(outputTopic, outgoingBatch.getJobId(), outgoingBatch); // Key by JobId for partitioning
            logger.info("Sent transformed batch to {}: jobId='{}', new batchId='{}', original_records={}, transformed_records={}, targetTableId='{}'",
                    outputTopic, outgoingBatch.getJobId(), outgoingBatch.getBatchId(), originalRecords.size(), transformedRecords.size(), targetTableId);
        } catch (Exception e) {
            logger.error("Error sending transformed batch to Kafka topic {}: {}", outputTopic, e.getMessage(), e);
            // Implement retry logic or send to a dead-letter topic (DLT) if needed
            // For now, just log the error.
        }
    }
}
