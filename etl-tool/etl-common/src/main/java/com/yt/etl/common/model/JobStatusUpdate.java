package com.yt.etl.common.model;

import java.time.Instant;
import java.util.Objects;

public class JobStatusUpdate {

    public enum Status {
        JOB_SUBMITTED, // Orchestration received job config
        JOB_STARTED,   // Orchestration initiated the first step (e.g., sent command to extractor)
        JOB_FAILED,
        JOB_COMPLETED_SUCCESSFULLY,

        TABLE_EXTRACT_STARTED,
        TABLE_EXTRACT_PROGRESS, // e.g. X records processed
        TABLE_EXTRACT_COMPLETED,
        TABLE_EXTRACT_FAILED,

        TABLE_TRANSFORM_STARTED,
        TABLE_TRANSFORM_PROGRESS,
        TABLE_TRANSFORM_COMPLETED,
        TABLE_TRANSFORM_FAILED,

        TABLE_LOAD_STARTED,
        TABLE_LOAD_PROGRESS,
        TABLE_LOAD_COMPLETED,
        TABLE_LOAD_FAILED,

        SERVICE_INFO, // Generic info message from a service
        SERVICE_WARN, // Generic warning message
        SERVICE_ERROR // Generic error from a service not tied to a specific table/job stage
    }

    private String jobId;
    private String executionId; // Unique ID for this particular run of the job
    private String batchId;     // Optional: specific to batch being processed by extract/transform/load
    private String tableId;     // Optional: specific table this status relates to
    private String serviceName; // "extract-service", "transform-service", "load-service", "orchestration-service"
    private Status status;
    private Instant timestamp;    // Using Instant for UTC timestamp
    private Long recordsProcessed; // Optional: number of records in this update (e.g. for a batch)
    private Long totalRecordsForTable; // Optional: total expected records for this table, if known
    private Boolean isLastBatchForTable; // Optional: relevant for batch-oriented services
    private String message;     // Human-readable message
    private String errorMessage; // Optional: details if status is FAILED

    public JobStatusUpdate() {
        this.timestamp = Instant.now();
    }

    public JobStatusUpdate(String jobId, String executionId, String serviceName, Status status, String message) {
        this();
        this.jobId = jobId;
        this.executionId = executionId;
        this.serviceName = serviceName;
        this.status = status;
        this.message = message;
    }

    // Getters and Setters
    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
        this.batchId = batchId;
    }

    public String getTableId() {
        return tableId;
    }

    public void setTableId(String tableId) {
        this.tableId = tableId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }

    public Long getRecordsProcessed() {
        return recordsProcessed;
    }

    public void setRecordsProcessed(Long recordsProcessed) {
        this.recordsProcessed = recordsProcessed;
    }

    public Long getTotalRecordsForTable() {
        return totalRecordsForTable;
    }

    public void setTotalRecordsForTable(Long totalRecordsForTable) {
        this.totalRecordsForTable = totalRecordsForTable;
    }

    public Boolean getIsLastBatchForTable() {
        return isLastBatchForTable;
    }

    public void setIsLastBatchForTable(Boolean lastBatchForTable) {
        isLastBatchForTable = lastBatchForTable;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobStatusUpdate that = (JobStatusUpdate) o;
        return Objects.equals(jobId, that.jobId) &&
               Objects.equals(executionId, that.executionId) &&
               Objects.equals(batchId, that.batchId) &&
               Objects.equals(tableId, that.tableId) &&
               Objects.equals(serviceName, that.serviceName) &&
               status == that.status &&
               Objects.equals(timestamp, that.timestamp) &&
               Objects.equals(recordsProcessed, that.recordsProcessed) &&
               Objects.equals(totalRecordsForTable, that.totalRecordsForTable) &&
               Objects.equals(isLastBatchForTable, that.isLastBatchForTable) &&
               Objects.equals(message, that.message) &&
               Objects.equals(errorMessage, that.errorMessage);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, executionId, batchId, tableId, serviceName, status, timestamp, recordsProcessed, totalRecordsForTable, isLastBatchForTable, message, errorMessage);
    }

    @Override
    public String toString() {
        return "JobStatusUpdate{" +
                "jobId='" + jobId + '\'' +
                ", executionId='" + executionId + '\'' +
                (batchId != null ? ", batchId='" + batchId + '\'' : "") +
                (tableId != null ? ", tableId='" + tableId + '\'' : "") +
                ", serviceName='" + serviceName + '\'' +
                ", status=" + status +
                ", timestamp=" + timestamp +
                (recordsProcessed != null ? ", recordsProcessed=" + recordsProcessed : "") +
                (totalRecordsForTable != null ? ", totalRecordsForTable=" + totalRecordsForTable : "") +
                (isLastBatchForTable != null ? ", isLastBatchForTable=" + isLastBatchForTable : "") +
                (message != null ? ", message='" + message + '\'' : "") +
                (errorMessage != null ? ", errorMessage='" + errorMessage + '\'' : "") +
                '}';
    }
}
