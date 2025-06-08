package com.yt.etl.extract.kafka;

import com.yt.etl.common.config.DatabaseConfig;
import com.yt.etl.common.db.DataReader;
import com.yt.etl.common.model.DataRecordBatch;
import com.yt.etl.common.model.DatabaseConnectionConfig;
import com.yt.etl.common.model.JobStatusUpdate;
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

import javax.sql.DataSource;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

@Service
public class ExtractionCommandListener {

    private static final Logger logger = LoggerFactory.getLogger(ExtractionCommandListener.class);

    private final KafkaTemplate<String, Object> kafkaTemplate; // Object for flexibility with DataRecordBatch and JobStatusUpdate
    private final DataReader dataReader; // Assuming JdbcDataReader is the implementation

    @Value("${etl.topics.output.untransformed}")
    private String untransformedDataTopic;

    @Value("${etl.topics.status.updates}")
    private String statusUpdatesTopic;

    @Value("${etl.extract.batchSize:1000}") // Default batch size from properties
    private int configuredBatchSize;

    @Autowired
    public ExtractionCommandListener(KafkaTemplate<String, Object> kafkaTemplate, DataReader dataReader) {
        this.kafkaTemplate = kafkaTemplate;
        this.dataReader = dataReader;
    }

    @KafkaListener(topics = "${etl.topics.command.extract}", groupId = "${spring.kafka.consumer.group-id}")
    public void handleExtractionCommand(@Payload DataRecordBatch command,
                                        @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                        @Header(KafkaHeaders.OFFSET) long offset) {

        logger.info("Received extraction command from topic {}: key='{}', offset={}, command='{}'",
                topic, key, offset, command);

        DatabaseConnectionConfig dbConnConfig = command.getSourceDbConfig();
        String table = command.getTableId();
        String jobId = command.getJobId();
        String executionId = command.getExecutionId();

        if (dbConnConfig == null || table == null || table.isEmpty()) {
            logger.error("Invalid command received: DB config or tableId is missing. Command: {}", command);
            sendStatusUpdate(jobId, executionId, table, JobStatusUpdate.Status.TABLE_EXTRACT_FAILED,
                    "Invalid command: DB config or tableId missing.", null, null, true);
            return;
        }

        sendStatusUpdate(jobId, executionId, table, JobStatusUpdate.Status.TABLE_EXTRACT_STARTED,
                "Extraction started for table " + table, null, null, false);

        File tempConfigFile = null;
        DatabaseConfig sourceDbRuntimeConfig = null;
        DataSource dataSource = null;
        AtomicLong totalRowsProcessed = new AtomicLong(0);
        long recordSequence = 0;

        try {
            tempConfigFile = createTempDbConfigFile(dbConnConfig);
            sourceDbRuntimeConfig = new DatabaseConfig(tempConfigFile);
            dataSource = sourceDbRuntimeConfig.getDataSource();

            long totalRows = dataReader.getRowCount(dataSource, table);
            logger.info("JobId: {}, ExecutionId: {}, Table: {}. Total rows to extract: {}", jobId, executionId, table, totalRows);
            sendStatusUpdate(jobId, executionId, table, JobStatusUpdate.Status.TABLE_EXTRACT_PROGRESS,
                    "Total rows to extract: " + totalRows, 0L, totalRows, false);


            try (Stream<Map<String, Object>> stream = dataReader.streamData(dataSource, table, null, configuredBatchSize)) {
                // Process the stream in batches manually for sending to Kafka
                List<Map<String, Object>> batch = new java.util.ArrayList<>(configuredBatchSize);
                for (Map<String, Object> row : (Iterable<Map<String, Object>>) stream::iterator) {
                    batch.add(row);
                    if (batch.size() >= configuredBatchSize) {
                        totalRowsProcessed.addAndGet(batch.size());
                        recordSequence++;
                        sendDataBatch(command, batch, false, recordSequence, totalRowsProcessed.get(), totalRows);
                        batch.clear();
                    }
                }
                // Send any remaining records in the last batch
                if (!batch.isEmpty()) {
                    totalRowsProcessed.addAndGet(batch.size());
                    recordSequence++;
                    sendDataBatch(command, batch, true, recordSequence, totalRowsProcessed.get(), totalRows);
                } else if (totalRows == 0 || totalRowsProcessed.get() == totalRows) {
                    // If totalRows is 0, or all rows processed and last batch was full
                     sendDataBatch(command, new java.util.ArrayList<>(), true, ++recordSequence, totalRowsProcessed.get(), totalRows);
                }
            }

            logger.info("Successfully extracted all data for JobId: {}, ExecutionId: {}, Table: {}. Total rows: {}", jobId, executionId, table, totalRowsProcessed.get());
            sendStatusUpdate(jobId, executionId, table, JobStatusUpdate.Status.TABLE_EXTRACT_COMPLETED,
                    "Extraction completed. Processed " + totalRowsProcessed.get() + " rows.", totalRowsProcessed.get(), totalRows, true);

        } catch (Exception e) {
            logger.error("Error during data extraction for JobId: {}, ExecutionId: {}, Table: {}: {}", jobId, executionId, table, e.getMessage(), e);
            sendStatusUpdate(jobId, executionId, table, JobStatusUpdate.Status.TABLE_EXTRACT_FAILED,
                    "Extraction failed: " + e.getMessage(), totalRowsProcessed.get(), null, true);
        } finally {
            if (sourceDbRuntimeConfig != null) {
                sourceDbRuntimeConfig.close();
            }
            if (tempConfigFile != null && tempConfigFile.exists()) {
                if (!tempConfigFile.delete()) {
                    logger.warn("Could not delete temporary config file: {}", tempConfigFile.getAbsolutePath());
                }
            }
        }
    }

    private void sendDataBatch(DataRecordBatch command, List<Map<String, Object>> records, boolean isLast, long sequence, long processed, long total) {
        DataRecordBatch dataMsg = new DataRecordBatch(
                command.getJobId(),
                UUID.randomUUID().toString(), // New unique batchId for this data batch
                command.getTableId(),
                command.getTargetTableId(),
                command.getJobTransformationConfig(),
                records,
                isLast,
                sequence, // Sequence number for this data batch
                command.getExecutionId(),
                command.getSourceDbConfig(), // Pass along source and target DB configs
                command.getTargetDbConfig()
        );
        kafkaTemplate.send(untransformedDataTopic, command.getJobId(), dataMsg);
        logger.debug("JobId: {}, ExecutionId: {}, Table: {}. Sent batch {} ({} records, isLast={}) to Kafka topic {}.",
                command.getJobId(), command.getExecutionId(), command.getTableId(), sequence, records.size(), isLast, untransformedDataTopic);

        sendStatusUpdate(command.getJobId(), command.getExecutionId(), command.getTableId(),
                JobStatusUpdate.Status.TABLE_EXTRACT_PROGRESS,
                "Processed " + processed + " of " + total + " rows.",
                (long) records.size(), total, isLast && (processed == total));
    }


    private void sendStatusUpdate(String jobId, String executionId, String tableId, JobStatusUpdate.Status status, String message, Long recordsProcessedInBatch, Long totalRecords, boolean isLastForTable) {
        JobStatusUpdate update = new JobStatusUpdate(jobId, executionId, "extract-service", status, message);
        update.setTableId(tableId);
        if (recordsProcessedInBatch != null) {
            update.setRecordsProcessed(recordsProcessedInBatch);
        }
        if (totalRecords != null){
            update.setTotalRecordsForTable(totalRecords);
        }
        update.setIsLastBatchForTable(isLastForTable); // This indicates if this *status update* is for the last batch of the table.
        kafkaTemplate.send(statusUpdatesTopic, jobId, update);
         logger.debug("Sent status update for JobId: {}, ExecutionId: {}, Table: {}, Status: {}", jobId, executionId, tableId, status);
    }

    private File createTempDbConfigFile(DatabaseConnectionConfig dbConfig) throws IOException {
        Properties props = new Properties();
        props.setProperty("url", dbConfig.getJdbcUrl());
        props.setProperty("username", dbConfig.getUsername());
        props.setProperty("password", dbConfig.getPassword());
        if (dbConfig.getDriverClassName() != null && !dbConfig.getDriverClassName().isEmpty()){
             // HikariCP usually infers driver from JDBC URL, but can be set if needed.
             // props.setProperty("driverClassName", dbConfig.getDriverClassName());
        }
        if (dbConfig.getAdditionalProperties() != null) {
            dbConfig.getAdditionalProperties().forEach(props::setProperty);
        }

        File tempFile = File.createTempFile("db_config_" + dbConfig.getConnectionName(), ".properties");
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            props.store(fos, "Temporary DB Config for " + dbConfig.getConnectionName());
        }
        return tempFile;
    }
}
