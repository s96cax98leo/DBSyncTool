package com.yt.etl.common.model;

import java.util.List;
import java.util.Map;
import java.util.Objects;

public class DataRecordBatch {

    private String jobId; // Identifies the overall ETL job
    private String batchId; // Unique ID for this specific batch of data
    private String tableId; // Source table name or identifier
    private String targetTableId; // Target table name or identifier (can be same as tableId)
    private JobTransformationConfig jobTransformationConfig; // Rules for transforming this batch
    private List<Map<String, Object>> records; // The actual data records
    private boolean isLastBatch; // Flag to indicate if this is the last batch for the given jobId/tableId
    private long sequenceNumber; // For ordering batches if necessary
    private String executionId; // Links back to a specific job execution run

    private DatabaseConnectionConfig sourceDbConfig;
    private DatabaseConnectionConfig targetDbConfig;

    public DataRecordBatch() {
    }

    public DataRecordBatch(String jobId, String batchId, String tableId, String targetTableId,
                           JobTransformationConfig jobTransformationConfig, List<Map<String, Object>> records,
                           boolean isLastBatch, long sequenceNumber, String executionId,
                           DatabaseConnectionConfig sourceDbConfig, DatabaseConnectionConfig targetDbConfig) {
        this.jobId = jobId;
        this.batchId = batchId;
        this.tableId = tableId;
        this.targetTableId = targetTableId;
        this.jobTransformationConfig = jobTransformationConfig;
        this.records = records;
        this.isLastBatch = isLastBatch;
        this.sequenceNumber = sequenceNumber;
        this.executionId = executionId;
        this.sourceDbConfig = sourceDbConfig;
        this.targetDbConfig = targetDbConfig;
    }

    // Getters and Setters
    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
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

    public String getTargetTableId() {
        return targetTableId;
    }

    public void setTargetTableId(String targetTableId) {
        this.targetTableId = targetTableId;
    }

    public JobTransformationConfig getJobTransformationConfig() {
        return jobTransformationConfig;
    }

    public void setJobTransformationConfig(JobTransformationConfig jobTransformationConfig) {
        this.jobTransformationConfig = jobTransformationConfig;
    }

    public List<Map<String, Object>> getRecords() {
        return records;
    }

    public void setRecords(List<Map<String, Object>> records) {
        this.records = records;
    }

    public boolean isLastBatch() {
        return isLastBatch;
    }

    public void setLastBatch(boolean lastBatch) {
        isLastBatch = lastBatch;
    }

    public long getSequenceNumber() {
        return sequenceNumber;
    }

    public void setSequenceNumber(long sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public String getExecutionId() {
        return executionId;
    }

    public void setExecutionId(String executionId) {
        this.executionId = executionId;
    }

    public DatabaseConnectionConfig getSourceDbConfig() {
        return sourceDbConfig;
    }

    public void setSourceDbConfig(DatabaseConnectionConfig sourceDbConfig) {
        this.sourceDbConfig = sourceDbConfig;
    }

    public DatabaseConnectionConfig getTargetDbConfig() {
        return targetDbConfig;
    }

    public void setTargetDbConfig(DatabaseConnectionConfig targetDbConfig) {
        this.targetDbConfig = targetDbConfig;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataRecordBatch that = (DataRecordBatch) o;
        return isLastBatch == that.isLastBatch &&
               sequenceNumber == that.sequenceNumber &&
               Objects.equals(jobId, that.jobId) &&
               Objects.equals(batchId, that.batchId) &&
               Objects.equals(tableId, that.tableId) &&
               Objects.equals(targetTableId, that.targetTableId) &&
               Objects.equals(jobTransformationConfig, that.jobTransformationConfig) &&
               Objects.equals(records, that.records) &&
               Objects.equals(executionId, that.executionId) &&
               Objects.equals(sourceDbConfig, that.sourceDbConfig) &&
               Objects.equals(targetDbConfig, that.targetDbConfig);
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, batchId, tableId, targetTableId, jobTransformationConfig, records, isLastBatch, sequenceNumber, executionId, sourceDbConfig, targetDbConfig);
    }

    @Override
    public String toString() {
        return "DataRecordBatch{" +
                "jobId='" + jobId + '\'' +
                ", executionId='" + executionId + '\'' +
                ", batchId='" + batchId + '\'' +
                ", tableId='" + tableId + '\'' +
                ", targetTableId='" + targetTableId + '\'' +
                ", sourceDbConfig_name=" + (sourceDbConfig != null ? sourceDbConfig.getConnectionName() : "null") +
                ", targetDbConfig_name=" + (targetDbConfig != null ? targetDbConfig.getConnectionName() : "null") +
                ", jobTransformationConfig_rules=" + (jobTransformationConfig != null && jobTransformationConfig.getRules() != null ? jobTransformationConfig.getRules().size() : "null") +
                ", records_count=" + (records != null ? records.size() : "null") +
                ", isLastBatch=" + isLastBatch +
                ", sequenceNumber=" + sequenceNumber +
                '}';
    }
}
