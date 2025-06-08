package com.yt.etl.orchestration.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.Objects;

@Entity
@Table(name = "job_execution_logs", indexes = {
    @Index(name = "idx_job_execution_logs_job_config_id", columnList = "job_config_id"),
    @Index(name = "idx_job_execution_logs_status", columnList = "status")
})
public class JobExecutionLogEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long executionId;

    @Column(nullable = false, length = 128,  name = "job_config_id") // Renamed to avoid confusion with EtlJobConfigEntity.jobId
    private String jobConfigId; // References EtlJobConfigEntity.jobId

    @Column(nullable = false)
    private LocalDateTime startTime;

    private LocalDateTime endTime;

    @Column(nullable = false, length = 50)
    private String status; // e.g., PENDING, RUNNING, COMPLETED, FAILED, PARTIALLY_COMPLETED

    @Lob // For potentially long messages or stack traces
    @Column(columnDefinition = "TEXT")
    private String lastMessage;

    private long totalTablesToProcess;
    private long tablesCompleted;
    private long tablesFailed;

    @ColumnDefault("0") // For databases that support it via DDL, otherwise handled by Java default
    private long totalRecordsExtracted;
    @ColumnDefault("0")
    private long totalRecordsTransformed;
    @ColumnDefault("0")
    private long totalRecordsLoaded;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;


    public JobExecutionLogEntity() {
        // startTime is set by @PrePersist or in service layer if more complex logic needed
    }

    // Constructor might be simplified or removed if using builder pattern or service layer population
    public JobExecutionLogEntity(String jobConfigId, String status, String lastMessage) {
        this.jobConfigId = jobConfigId;
        this.status = status;
        this.lastMessage = lastMessage;
        this.startTime = LocalDateTime.now(); // Set startTime on creation
    }

    @PrePersist
    protected void onCreate() {
        if (startTime == null) {
            startTime = LocalDateTime.now();
        }
    }


    // Getters and Setters
    public Long getExecutionId() {
        return executionId;
    }

    public void setExecutionId(Long executionId) {
        this.executionId = executionId;
    }

    public String getJobConfigId() {
        return jobConfigId;
    }

    public void setJobConfigId(String jobConfigId) {
        this.jobConfigId = jobConfigId;
    }

    public LocalDateTime getStartTime() {
        return startTime;
    }

    public void setStartTime(LocalDateTime startTime) {
        this.startTime = startTime;
    }

    public LocalDateTime getEndTime() {
        return endTime;
    }

    public void setEndTime(LocalDateTime endTime) {
        this.endTime = endTime;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getLastMessage() {
        return lastMessage;
    }

    public void setLastMessage(String lastMessage) {
        this.lastMessage = lastMessage;
    }

    public long getTotalTablesToProcess() {
        return totalTablesToProcess;
    }

    public void setTotalTablesToProcess(long totalTablesToProcess) {
        this.totalTablesToProcess = totalTablesToProcess;
    }

    public long getTablesCompleted() {
        return tablesCompleted;
    }

    public void setTablesCompleted(long tablesCompleted) {
        this.tablesCompleted = tablesCompleted;
    }

    public long getTablesFailed() {
        return tablesFailed;
    }

    public void setTablesFailed(long tablesFailed) {
        this.tablesFailed = tablesFailed;
    }

    public long getTotalRecordsExtracted() {
        return totalRecordsExtracted;
    }

    public void setTotalRecordsExtracted(long totalRecordsExtracted) {
        this.totalRecordsExtracted = totalRecordsExtracted;
    }

    public long getTotalRecordsTransformed() {
        return totalRecordsTransformed;
    }

    public void setTotalRecordsTransformed(long totalRecordsTransformed) {
        this.totalRecordsTransformed = totalRecordsTransformed;
    }

    public long getTotalRecordsLoaded() {
        return totalRecordsLoaded;
    }

    public void setTotalRecordsLoaded(long totalRecordsLoaded) {
        this.totalRecordsLoaded = totalRecordsLoaded;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        JobExecutionLogEntity that = (JobExecutionLogEntity) o;
        return Objects.equals(executionId, that.executionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(executionId);
    }

    @Override
    public String toString() {
        return "JobExecutionLogEntity{" +
                "executionId=" + executionId +
                ", jobConfigId='" + jobConfigId + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", status='" + status + '\'' +
                ", tablesCompleted=" + tablesCompleted + "/" + totalTablesToProcess +
                (tablesFailed > 0 ? ", tablesFailed=" + tablesFailed : "") +
                ", records(E/T/L)= " + totalRecordsExtracted + "/" + totalRecordsTransformed + "/" + totalRecordsLoaded +
                ", lastMessage_length=" + (lastMessage != null ? lastMessage.length() : "null") +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }
}
