package com.yt.etl.orchestration.kafka;

import com.yt.etl.common.model.JobStatusUpdate;
import com.yt.etl.orchestration.entity.JobExecutionLogEntity;
import com.yt.etl.orchestration.entity.JobExecutionTableLogEntity; // Import new entity
import com.yt.etl.orchestration.repository.JobExecutionLogRepository;
import com.yt.etl.orchestration.repository.JobExecutionTableLogRepository; // Import new repository
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.time.Instant; // For converting statusUpdate timestamp
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

@Service
public class JobStatusListener {

    private static final Logger logger = LoggerFactory.getLogger(JobStatusListener.class);

    private final JobExecutionLogRepository jobExecutionLogRepository;
    private final JobExecutionTableLogRepository jobExecutionTableLogRepository; // Inject new repository

    @Autowired
    public JobStatusListener(JobExecutionLogRepository jobExecutionLogRepository,
                             JobExecutionTableLogRepository jobExecutionTableLogRepository) { // Add to constructor
        this.jobExecutionLogRepository = jobExecutionLogRepository;
        this.jobExecutionTableLogRepository = jobExecutionTableLogRepository; // Initialize
    }

    @KafkaListener(topics = "${etl.topics.status.updates}", groupId = "orchestration-status-group")
    @Transactional
    public void handleJobStatusUpdate(@Payload JobStatusUpdate statusUpdate,
                                      @Header(KafkaHeaders.RECEIVED_KEY) String key,
                                      @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                                      @Header(KafkaHeaders.OFFSET) long offset) {

        logger.info("Received job status update from topic {}: key='{}', offset={}, status='{}', service='{}', jobId='{}', executionId='{}', tableId='{}', message='{}'",
                topic, key, offset, statusUpdate.getStatus(), statusUpdate.getServiceName(),
                statusUpdate.getJobId(), statusUpdate.getExecutionId(), statusUpdate.getTableId(), statusUpdate.getMessage());

        if (statusUpdate.getExecutionId() == null) {
            logger.warn("Received status update without an executionId. Cannot process. JobId: {}", statusUpdate.getJobId());
            // Potentially send to a dead-letter queue or log more permanently
            return;
        }

        // Try to parse executionId to Long. If not possible, log error and return.
        long executionIdLong;
        try {
            executionIdLong = Long.parseLong(statusUpdate.getExecutionId());
        } catch (NumberFormatException e) {
            logger.error("Invalid executionId format '{}' in status update. Cannot process.", statusUpdate.getExecutionId(), e);
            return;
        }


        Optional<JobExecutionLogEntity> logOptional = jobExecutionLogRepository.findById(executionIdLong);

        if (logOptional.isEmpty()) {
            logger.warn("No JobExecutionLogEntity found for executionId {}. Status update from {} for jobId {} cannot be processed.",
                    executionIdLong, statusUpdate.getServiceName(), statusUpdate.getJobId());
            // This might happen if the status update arrives before the job log is committed,
            // or if the executionId is incorrect. Consider retry or DLQ.
            return;
        }

        JobExecutionLogEntity jobLog = logOptional.get();
        String tableId = statusUpdate.getTableId();
        JobExecutionTableLogEntity tableLog = null;

        if (tableId != null && !tableId.isEmpty()) {
            Optional<JobExecutionTableLogEntity> tableLogOptional = jobExecutionTableLogRepository.findByExecutionIdAndTableName(executionIdLong, tableId);
            if (tableLogOptional.isPresent()) {
                tableLog = tableLogOptional.get();
            } else {
                logger.warn("No JobExecutionTableLogEntity found for executionId {} and tableId {}. Creating one.", executionIdLong, tableId);
                // This case should ideally be rare if EtlJobController creates all table logs upfront.
                tableLog = new JobExecutionTableLogEntity(executionIdLong, tableId, JobStatusUpdate.Status.SERVICE_INFO.name()); // Default status
                tableLog.setStartTime(LocalDateTime.now()); // Approximate start time
            }
        }

        // Update table log if applicable
        if (tableLog != null) {
            updateTableLog(tableLog, statusUpdate);
        }

        // Aggregate and update main job log
        updateOverallJobLog(jobLog, statusUpdate, tableLog);

        if (tableLog != null) {
            jobExecutionTableLogRepository.save(tableLog);
            logger.debug("Updated JobExecutionTableLog for tableLogId {}: status='{}'", tableLog.getTableLogId(), tableLog.getStatus());
        }
        jobExecutionLogRepository.save(jobLog);
        logger.debug("Updated JobExecutionLog for executionId {}: status='{}'", executionIdLong, jobLog.getStatus());
    }

    private void updateTableLog(JobExecutionTableLogEntity tableLog, JobStatusUpdate statusUpdate) {
        // Map JobStatusUpdate.Status enum to String for table log status
        String newTableStatusStr = statusUpdate.getStatus().name();
        tableLog.setStatus(newTableStatusStr);
        tableLog.setErrorMessage(statusUpdate.getErrorMessage()); // Clears if null

        if (statusUpdate.getRecordsProcessed() != null) {
            switch (statusUpdate.getStatus()) {
                case TABLE_EXTRACT_PROGRESS:
                case TABLE_EXTRACT_COMPLETED:
                    tableLog.setRecordsExtracted(tableLog.getRecordsExtracted() + statusUpdate.getRecordsProcessed());
                    break;
                case TABLE_TRANSFORM_PROGRESS:
                case TABLE_TRANSFORM_COMPLETED:
                    tableLog.setRecordsTransformed(tableLog.getRecordsTransformed() + statusUpdate.getRecordsProcessed());
                    break;
                case TABLE_LOAD_PROGRESS:
                case TABLE_LOAD_COMPLETED:
                    tableLog.setRecordsLoaded(tableLog.getRecordsLoaded() + statusUpdate.getRecordsProcessed());
                    break;
                default:
                    break;
            }
        }

        // Set start time if this is the first "started" type status for the table
        if (tableLog.getStartTime() == null &&
            (newTableStatusStr.endsWith("_STARTED") || newTableStatusStr.contains("EXTRACTING") || newTableStatusStr.contains("LOADING"))) {
            tableLog.setStartTime(statusUpdate.getTimestamp() != null ? LocalDateTime.ofInstant(statusUpdate.getTimestamp(), java.time.ZoneId.systemDefault()) : LocalDateTime.now());
        }


        if (isTerminalTableStatus(statusUpdate.getStatus())) {
            tableLog.setEndTime(statusUpdate.getTimestamp() != null ? LocalDateTime.ofInstant(statusUpdate.getTimestamp(), java.time.ZoneId.systemDefault()) : LocalDateTime.now());
        }
    }

    private boolean isTerminalTableStatus(JobStatusUpdate.Status status) {
        return status == JobStatusUpdate.Status.TABLE_EXTRACT_COMPLETED ||
               status == JobStatusUpdate.Status.TABLE_TRANSFORM_COMPLETED ||
               status == JobStatusUpdate.Status.TABLE_LOAD_COMPLETED ||
               status == JobStatusUpdate.Status.TABLE_EXTRACT_FAILED ||
               status == JobStatusUpdate.Status.TABLE_TRANSFORM_FAILED ||
               status == JobStatusUpdate.Status.TABLE_LOAD_FAILED;
    }


    private void updateOverallJobLog(JobExecutionLogEntity jobLog, JobStatusUpdate statusUpdate, JobExecutionTableLogEntity tableLog) {
        jobLog.setLastMessage(String.format("Service: %s, Table: %s, Status: %s, Msg: %s",
                statusUpdate.getServiceName(), statusUpdate.getTableId(), statusUpdate.getStatus(),
                statusUpdate.getErrorMessage() != null ? statusUpdate.getErrorMessage() : statusUpdate.getMessage()));

        // Aggregate record counts from the status update if it represents final counts for a step
        // This assumes JobStatusUpdate.recordsProcessed for COMPLETED steps gives the total for that step for that table.
        if (statusUpdate.getRecordsProcessed() != null) {
             switch (statusUpdate.getStatus()) {
                case TABLE_EXTRACT_COMPLETED:
                    jobLog.setTotalRecordsExtracted(jobLog.getTotalRecordsExtracted() + statusUpdate.getRecordsProcessed());
                    break;
                case TABLE_TRANSFORM_COMPLETED:
                    jobLog.setTotalRecordsTransformed(jobLog.getTotalRecordsTransformed() + statusUpdate.getRecordsProcessed());
                    break;
                case TABLE_LOAD_COMPLETED:
                    jobLog.setTotalRecordsLoaded(jobLog.getTotalRecordsLoaded() + statusUpdate.getRecordsProcessed());
                    break;
                default: // For PROGRESS updates, we don't aggregate to main log here to avoid double counting.
                    break;
            }
        }


        // Determine overall job status
        boolean currentJobIsTerminal = "COMPLETED_SUCCESSFULLY".equals(jobLog.getStatus()) ||
                                     "FAILED".equals(jobLog.getStatus()) ||
                                     "PARTIALLY_COMPLETED_WITH_FAILURES".equals(jobLog.getStatus());

        if (currentJobIsTerminal && statusUpdate.getStatus() != JobStatusUpdate.Status.JOB_FAILED) {
            // If job is already in a final state, only a global JOB_FAILED can change it further.
            // Individual table updates are logged but might not change the overall job status.
            return;
        }

        if (statusUpdate.getStatus() == JobStatusUpdate.Status.JOB_FAILED ||
            (tableLog != null && tableLog.getStatus().endsWith("FAILED"))) {
            jobLog.setStatus("FAILED"); // If any table fails, or job explicitly fails, mark job as FAILED
            jobLog.setEndTime(LocalDateTime.now());
            jobLog.setTablesFailed(countFailedTables(jobLog.getExecutionId())); // Recount
        } else if (statusUpdate.getStatus() == JobStatusUpdate.Status.JOB_COMPLETED_SUCCESSFULLY) {
             jobLog.setStatus("COMPLETED_SUCCESSFULLY");
             jobLog.setEndTime(LocalDateTime.now());
        } else {
            // Check if all tables are completed
            long completedTables = countCompletedTables(jobLog.getExecutionId());
            jobLog.setTablesCompleted(completedTables);
            long failedTables = countFailedTables(jobLog.getExecutionId());
            jobLog.setTablesFailed(failedTables);

            if (completedTables + failedTables >= jobLog.getTotalTablesToProcess()) {
                if (failedTables > 0) {
                    jobLog.setStatus("PARTIALLY_COMPLETED_WITH_FAILURES");
                } else {
                    jobLog.setStatus("COMPLETED_SUCCESSFULLY");
                }
                jobLog.setEndTime(LocalDateTime.now());
            } else {
                jobLog.setStatus("RUNNING"); // Still tables being processed or pending
            }
        }
    }

    private long countCompletedTables(Long executionId) {
        return jobExecutionTableLogRepository.findByExecutionIdAndStatus(executionId, JobStatusUpdate.Status.TABLE_LOAD_COMPLETED.name()).size() +
               jobExecutionTableLogRepository.findByExecutionIdAndStatus(executionId, JobStatusUpdate.Status.TABLE_TRANSFORM_COMPLETED.name()).size() + // If transform is final step for some tables
               jobExecutionTableLogRepository.findByExecutionIdAndStatus(executionId, JobStatusUpdate.Status.TABLE_EXTRACT_COMPLETED.name()).size(); // If extract is final step
        // This logic needs to be more robust depending on the pipeline definition for each table.
        // For now, assuming TABLE_LOAD_COMPLETED is the primary success marker for a table.
        // A simpler approach: count tables where status is one of the "COMPLETED" states.
    }

    private long countFailedTables(Long executionId) {
         return jobExecutionTableLogRepository.findByExecutionIdAndStatus(executionId, JobStatusUpdate.Status.TABLE_LOAD_FAILED.name()).size() +
                jobExecutionTableLogRepository.findByExecutionIdAndStatus(executionId, JobStatusUpdate.Status.TABLE_TRANSFORM_FAILED.name()).size() +
                jobExecutionTableLogRepository.findByExecutionIdAndStatus(executionId, JobStatusUpdate.Status.TABLE_EXTRACT_FAILED.name()).size();
    }
}
