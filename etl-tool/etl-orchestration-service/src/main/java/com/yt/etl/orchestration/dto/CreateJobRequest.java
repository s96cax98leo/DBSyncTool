package com.yt.etl.orchestration.dto;

import com.yt.etl.common.model.DatabaseConnectionConfig;
import com.yt.etl.common.model.JobTransformationConfig;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;

import java.util.List;
import java.util.Map;

// Using EtlJobConfig from common directly for request body is also an option,
// but a DTO allows for specific validation annotations and API shaping.
public class CreateJobRequest {

    @NotBlank(message = "Job name cannot be blank")
    private String jobName;

    @NotNull(message = "Source database configuration cannot be null")
    @Valid
    private DatabaseConnectionConfig sourceDbConfig;

    @NotNull(message = "Target database configuration cannot be null")
    @Valid
    private DatabaseConnectionConfig targetDbConfig;

    @NotEmpty(message = "Tables to process list cannot be empty")
    private List<String> tablesToProcess;

    // Optional: transformations can be null or empty if no transformations are needed
    @Valid
    private Map<String, JobTransformationConfig> tableTransformationConfigs;


    // Getters and Setters
    public String getJobName() {
        return jobName;
    }

    public void setJobName(String jobName) {
        this.jobName = jobName;
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
}
