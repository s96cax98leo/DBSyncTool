package com.yt.etl.load.kafka;

import com.yt.etl.common.config.DatabaseConfig;
import com.yt.etl.common.db.DatabaseMetadataFetcher;
import com.yt.etl.common.db.DataWriter;
import com.yt.etl.common.model.DataRecordBatch;
import com.yt.etl.common.model.DatabaseConnectionConfig;
import com.yt.etl.common.model.JobStatusUpdate;
import com.yt.etl.common.model.TableMetadata;
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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class TransformedDataListener {

    private static final Logger logger = LoggerFactory.getLogger(TransformedDataListener.class);

    private final KafkaTemplate<String, JobStatusUpdate> kafkaStatusTemplate;
    private final DataWriter dataWriter; // Assuming JdbcDataWriter
    private final DatabaseMetadataFetcher metadataFetcher; // Assuming OracleMetadataFetcher

    @Value("${etl.topics.status.updates}")
    private String statusUpdatesTopic;

    // TODO: Make truncate configurable, perhaps from EtlJobConfig passed in DataRecordBatch
    // For now, simple boolean flag. This state needs to be managed per job execution + table.
    private final Set<String> truncatedTables = ConcurrentHashMap.newKeySet(); // executionId + tableId

    @Autowired
    public TransformedDataListener(KafkaTemplate<String, JobStatusUpdate> kafkaStatusTemplate,
                                   DataWriter dataWriter,
                                   DatabaseMetadataFetcher metadataFetcher) {
        this.kafkaStatusTemplate = kafkaStatusTemplate;
        this.dataWriter = dataWriter;
        this.metadataFetcher = metadataFetcher;
    }

    @KafkaListener(topics = "${etl.topics.input.transformed}", groupId = "${spring.kafka.consumer.group-id}")
    public void handleTransformedData(@Payload DataRecordBatch dataMessage,
                                      @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      @Header(KafkaHeaders.OFFSET) long offset) {

        logger.info("Received transformed data batch from topic {}: key='{}', offset={}, dataMessage='{}'",
                topic, key, offset, dataMessage);

        DatabaseConnectionConfig dbConnConfig = dataMessage.getTargetDbConfig();
        String targetTable = dataMessage.getTargetTableId() != null ? dataMessage.getTargetTableId() : dataMessage.getTableId();
        String jobId = dataMessage.getJobId();
        String executionId = dataMessage.getExecutionId();

        if (dbConnConfig == null || targetTable == null || targetTable.isEmpty()) {
            logger.error("Invalid data message received: Target DB config or targetTableId is missing. Message: {}", dataMessage);
            sendStatusUpdate(jobId, executionId, targetTable, JobStatusUpdate.Status.TABLE_LOAD_FAILED,
                    "Invalid data message: Target DB config or targetTableId missing.", null, true);
            return;
        }

        if (dataMessage.getRecords() == null) {
            logger.warn("Received null records list for JobId: {}, ExecutionId: {}, Table: {}. If this is the last batch, marking as complete.", jobId, executionId, targetTable);
             if (dataMessage.isLastBatch()) {
                sendStatusUpdate(jobId, executionId, targetTable, JobStatusUpdate.Status.TABLE_LOAD_COMPLETED,
                        "Load completed (no records in last batch).", 0L, true);
            }
            return;
        }


        sendStatusUpdate(jobId, executionId, targetTable, JobStatusUpdate.Status.TABLE_LOAD_STARTED,
                "Load started for table " + targetTable + ", batch " + dataMessage.getSequenceNumber(), (long) dataMessage.getRecords().size(), false);


        File tempConfigFile = null;
        DatabaseConfig targetDbRuntimeConfig = null;
        DataSource dataSource = null;

        try {
            tempConfigFile = createTempDbConfigFile(dbConnConfig);
            targetDbRuntimeConfig = new DatabaseConfig(tempConfigFile);
            dataSource = targetDbRuntimeConfig.getDataSource();

            String tableExecutionKey = executionId + ":" + targetTable;

            // Simplified table preparation:
            // On first batch for this table in this execution, check metadata and create/truncate.
            // A more robust solution would involve explicit "prepare table" commands or steps.
            if (dataMessage.getSequenceNumber() == 1 || !truncatedTables.contains(tableExecutionKey) ) { // Assuming sequence starts at 1 for first data batch
                TableMetadata tableMetadata;
                try {
                    tableMetadata = metadataFetcher.getTableMetadata(dataSource, targetTable);
                     // For now, if metadata fetch succeeds, we assume table exists.
                     // Add truncate logic here if etl.load.truncateBeforeLoad=true (needs config from job)
                     // boolean truncate = ... get from dataMessage or job config ...
                     boolean truncate = false; // Placeholder
                     if (truncate) {
                        dataWriter.truncateTable(dataSource, targetTable);
                        logger.info("JobId: {}, ExecutionId: {}, Table: {} truncated before load.", jobId, executionId, targetTable);
                     }
                     truncatedTables.add(tableExecutionKey); // Mark as prepared/truncated for this execution

                } catch (Exception e) { // Catch broad exception for table not found by metadata fetcher
                    logger.warn("JobId: {}, ExecutionId: {}, Table: {} - metadata not found or error fetching. Attempting to create.", jobId, executionId, targetTable, e);
                    // This assumes the transformation output structure matches what's needed for the new table.
                    // This is a big assumption. A schema mapping/evolution tool is better.
                    // For now, we need TableMetadata to create the table. If it's not in DataRecordBatch, this is tricky.
                    // The DataWriter's createTable expects TableMetadata.
                    // This part of the logic needs significant refinement: How does Load service get schema for a new table?
                    // Option 1: Orchestrator sends it. Option 2: Derive from first batch of data (risky).
                    // Option 3: Fail if table doesn't exist and no explicit "create" instruction.
                    // For now, we'll skip table creation if metadata is not readily available.
                    // This means table must exist for loading to proceed.
                    // throw new RuntimeException("Table metadata not available to create table " + targetTable, e);
                     logger.error("JobId: {}, ExecutionId: {}, Table: {} - Table does not exist and automatic creation is not yet implemented robustly.", jobId, executionId, targetTable);
                     throw new RuntimeException("Table " + targetTable + " does not exist and schema is not provided for creation.");
                }
                 // We need TableMetadata for the writeData method as well for type handling.
                 // This is a gap if we don't pass it in DataRecordBatch from transform to load,
                 // or if transform service doesn't have it.
                 // For now, assuming JdbcDataWriter.writeData can work without full metadata if types are simple,
                 // or we simplify the DataWriter interface.
                 // Let's assume for now that the TableMetadata is fetched once for the table.
                 TableMetadata currentTableMetadata = metadataFetcher.getTableMetadata(dataSource, targetTable); // Fetch it again or cache it.
                 dataWriter.writeData(dataSource, targetTable, dataMessage.getRecords(), currentTableMetadata);
            } else {
                 TableMetadata currentTableMetadata = metadataFetcher.getTableMetadata(dataSource, targetTable); // Fetch it again or cache it.
                 dataWriter.writeData(dataSource, targetTable, dataMessage.getRecords(), currentTableMetadata);
            }


            logger.info("JobId: {}, ExecutionId: {}, Table: {} - Successfully wrote {} records (batch {}).",
                    jobId, executionId, targetTable, dataMessage.getRecords().size(), dataMessage.getSequenceNumber());

            sendStatusUpdate(jobId, executionId, targetTable, JobStatusUpdate.Status.TABLE_LOAD_PROGRESS,
                    "Loaded " + dataMessage.getRecords().size() + " records.", (long) dataMessage.getRecords().size(), dataMessage.isLastBatch());

            if (dataMessage.isLastBatch()) {
                logger.info("JobId: {}, ExecutionId: {}, Table: {} - All batches loaded.", jobId, executionId, targetTable);
                sendStatusUpdate(jobId, executionId, targetTable, JobStatusUpdate.Status.TABLE_LOAD_COMPLETED,
                        "Load completed for table.", null, true);
                truncatedTables.remove(tableExecutionKey); // Clean up state for next execution
            }

        } catch (Exception e) {
            logger.error("Error during data loading for JobId: {}, ExecutionId: {}, Table: {}: {}", jobId, executionId, targetTable, e.getMessage(), e);
            sendStatusUpdate(jobId, executionId, targetTable, JobStatusUpdate.Status.TABLE_LOAD_FAILED,
                    "Load failed: " + e.getMessage(), null, true); // isLastBatchForTable = true because this batch failed, effectively ending table processing.
            truncatedTables.remove(tableExecutionKey); // Clean up state
        } finally {
            if (targetDbRuntimeConfig != null) {
                targetDbRuntimeConfig.close();
            }
            if (tempConfigFile != null && tempConfigFile.exists()) {
                if (!tempConfigFile.delete()) {
                    logger.warn("Could not delete temporary config file: {}", tempConfigFile.getAbsolutePath());
                }
            }
        }
    }

    private void sendStatusUpdate(String jobId, String executionId, String tableId, JobStatusUpdate.Status status, String message, Long recordsProcessed, boolean isLastForTable) {
        JobStatusUpdate update = new JobStatusUpdate(jobId, executionId, "load-service", status, message);
        update.setTableId(tableId);
        if (recordsProcessed != null) {
            update.setRecordsProcessed(recordsProcessed);
        }
        update.setIsLastBatchForTable(isLastForTable);
        kafkaStatusTemplate.send(statusUpdatesTopic, jobId, update);
        logger.debug("Sent status update for JobId: {}, ExecutionId: {}, Table: {}, Status: {}", jobId, executionId, tableId, status);
    }

    private File createTempDbConfigFile(DatabaseConnectionConfig dbConfig) throws IOException {
        Properties props = new Properties();
        props.setProperty("url", dbConfig.getJdbcUrl());
        props.setProperty("username", dbConfig.getUsername());
        props.setProperty("password", dbConfig.getPassword());
         if (dbConfig.getDriverClassName() != null && !dbConfig.getDriverClassName().isEmpty()){
             // props.setProperty("driverClassName", dbConfig.getDriverClassName());
        }
        if (dbConfig.getAdditionalProperties() != null) {
            dbConfig.getAdditionalProperties().forEach(props::setProperty);
        }

        File tempFile = File.createTempFile("db_config_load_" + dbConfig.getConnectionName(), ".properties");
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            props.store(fos, "Temporary DB Config for Load Service: " + dbConfig.getConnectionName());
        }
        return tempFile;
    }
}
