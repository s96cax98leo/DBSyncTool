package com.yt.etl.orchestration.entity;

import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.Objects;

@Entity
@Table(name = "job_execution_table_logs", indexes = {
    @Index(name = "idx_jetl_execution_id", columnList = "execution_id"),
    @Index(name = "idx_jetl_table_name", columnList = "table_name"),
    @Index(name = "idx_jetl_status", columnList = "status")
})
public class JobExecutionTableLogEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long tableLogId;

    // Using execution_id directly to avoid fetching JobExecutionLogEntity unless needed
    @Column(name = "execution_id", nullable = false)
    private Long executionId;
    // If you prefer a direct association:
    // @ManyToOne(fetch = FetchType.LAZY)
    // @JoinColumn(name = "execution_id", nullable = false)
    // private JobExecutionLogEntity jobExecutionLog;

    @Column(nullable = false)
    private String tableName;

    @Column(nullable = false, length = 50)
    private String status; // PENDING, EXTRACTING, TRANSFORMING, LOADING, COMPLETED_SUCCESSFULLY, FAILED

    private LocalDateTime startTime;
    private LocalDateTime endTime;

    @ColumnDefault("0")
    private long recordsExtracted;
    @ColumnDefault("0")
    private long recordsTransformed;
    @ColumnDefault("0")
    private long recordsLoaded;

    @Lob
    @Column(columnDefinition = "TEXT")
    private String errorMessage;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    public JobExecutionTableLogEntity() {
    }

    public JobExecutionTableLogEntity(Long executionId, String tableName, String status) {
        this.executionId = executionId;
        this.tableName = tableName;
        this.status = status;
    }

    // Getters and Setters
    public Long getTableLogId() {
        return tableLogId;
    }

    public void setTableLogId(Long tableLogId) {
        this.tableLogId = tableLogId;
    }

    public Long getExecutionId() {
        return executionId;
    }

    public void setExecutionId(Long executionId) {
        this.executionId = executionId;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
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

    public long getRecordsExtracted() {
        return recordsExtracted;
    }

    public void setRecordsExtracted(long recordsExtracted) {
        this.recordsExtracted = recordsExtracted;
    }

    public long getRecordsTransformed() {
        return recordsTransformed;
    }

    public void setRecordsTransformed(long recordsTransformed) {
        this.recordsTransformed = recordsTransformed;
    }

    public long getRecordsLoaded() {
        return recordsLoaded;
    }

    public void setRecordsLoaded(long recordsLoaded) {
        this.recordsLoaded = recordsLoaded;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
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
        JobExecutionTableLogEntity that = (JobExecutionTableLogEntity) o;
        return Objects.equals(tableLogId, that.tableLogId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(tableLogId);
    }

    @Override
    public String toString() {
        return "JobExecutionTableLogEntity{" +
                "tableLogId=" + tableLogId +
                ", executionId=" + executionId +
                ", tableName='" + tableName + '\'' +
                ", status='" + status + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", recordsExtracted=" + recordsExtracted +
                ", recordsTransformed=" + recordsTransformed +
                ", recordsLoaded=" + recordsLoaded +
                ", errorMessage_present=" + (errorMessage != null && !errorMessage.isEmpty()) +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }
}
