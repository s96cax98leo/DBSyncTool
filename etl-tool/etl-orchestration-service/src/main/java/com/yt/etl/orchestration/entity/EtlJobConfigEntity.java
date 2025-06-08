package com.yt.etl.orchestration.entity;

import com.yt.etl.common.model.JobTransformationConfig; // Used by the converter
import jakarta.persistence.*;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Entity
@Table(name = "etl_job_configs")
public class EtlJobConfigEntity {

    @Id
    @Column(length = 128) // Increased length for potentially longer UUIDs or generated IDs
    private String jobId;

    @Column(nullable = false, length = 255)
    private String jobName;

    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "connectionName", column = @Column(name = "source_db_conn_name")),
            @AttributeOverride(name = "jdbcUrl", column = @Column(name = "source_db_jdbc_url", length = 1024)),
            @AttributeOverride(name = "username", column = @Column(name = "source_db_username")),
            @AttributeOverride(name = "password", column = @Column(name = "source_db_password", length = 512)),
            @AttributeOverride(name = "driverClassName", column = @Column(name = "source_db_driver_class_name")),
            @AttributeOverride(name = "additionalPropertiesJson", column = @Column(name = "source_db_additional_props_json", columnDefinition = "TEXT"))
    })
    private DatabaseConnectionConfigEmbeddable sourceDbConfig;

    @Embedded
    @AttributeOverrides({
            @AttributeOverride(name = "connectionName", column = @Column(name = "target_db_conn_name")),
            @AttributeOverride(name = "jdbcUrl", column = @Column(name = "target_db_jdbc_url", length = 1024)),
            @AttributeOverride(name = "username", column = @Column(name = "target_db_username")),
            @AttributeOverride(name = "password", column = @Column(name = "target_db_password", length = 512)),
            @AttributeOverride(name = "driverClassName", column = @Column(name = "target_db_driver_class_name")),
            @AttributeOverride(name = "additionalPropertiesJson", column = @Column(name = "target_db_additional_props_json", columnDefinition = "TEXT"))
    })
    private DatabaseConnectionConfigEmbeddable targetDbConfig;

    // @ElementCollection for List<String> is simple for direct storage if supported well by provider for simple types.
    // For JSON storage, a custom converter is more explicit and portable.
    @Column(name = "tables_to_process", columnDefinition = "TEXT")
    @Convert(converter = StringListConverter.class)
    private List<String> tablesToProcess;

    @Column(name = "table_transformation_configs_json", columnDefinition = "TEXT") // Name matches V1 script
    @Convert(converter = MapJobTransformationConfigConverter.class)
    private Map<String, JobTransformationConfig> tableTransformationConfigs;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    // Getters and Setters
    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
    }

    public DatabaseConnectionConfigEmbeddable getSourceDbConfig() {
        return sourceDbConfig;
    }

    public void setSourceDbConfig(DatabaseConnectionConfigEmbeddable sourceDbConfig) {
        this.sourceDbConfig = sourceDbConfig;
    }

    public DatabaseConnectionConfigEmbeddable getTargetDbConfig() {
        return targetDbConfig;
    }

    public void setTargetDbConfig(DatabaseConnectionConfigEmbeddable targetDbConfig) {
        this.targetDbConfig = targetDbConfig;
    }

    public List<String> getTablesToProcess() {
        return tablesToProcess;
    }

    public void setTablesToProcess(List<String> tablesToProcess) {
        this.tablesToProcess = tablesToProcess;
    }

    public Map<String, JobTransformationConfig> getTableTransformationConfigs() {
        return tableTransformationConfigs;
    }

    public void setTableTransformationConfigs(Map<String, JobTransformationConfig> tableTransformationConfigs) {
        this.tableTransformationConfigs = tableTransformationConfigs;
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
        EtlJobConfigEntity that = (EtlJobConfigEntity) o;
        return Objects.equals(jobId, that.jobId); // Primary key is sufficient for equality
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId); // Primary key is sufficient for hashCode
    }

    @Override
    public String toString() {
        return "EtlJobConfigEntity{" +
                "jobId='" + jobId + '\'' +
                ", jobName='" + jobName + '\'' +
                ", sourceDbConfig=" + sourceDbConfig +
                ", targetDbConfig=" + targetDbConfig +
                ", tablesToProcess_count=" + (tablesToProcess != null ? tablesToProcess.size() : "null") +
                ", tableTransformationConfigs_count=" + (tableTransformationConfigs != null ? tableTransformationConfigs.size() : "null") +
                ", createdAt=" + createdAt +
                ", updatedAt=" + updatedAt +
                '}';
    }
}
