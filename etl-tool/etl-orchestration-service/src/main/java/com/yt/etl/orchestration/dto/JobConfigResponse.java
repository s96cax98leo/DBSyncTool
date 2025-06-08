package com.yt.etl.orchestration.dto;

import com.yt.etl.common.model.DatabaseConnectionConfig;
import com.yt.etl.common.model.JobTransformationConfig;

import java.util.List;
import java.util.Map;

// Mirrors EtlJobConfig from common for now.
// Could be different if we want to hide/transform certain fields for API responses.
public class JobConfigResponse {

    private String jobId;
    private String jobName;
    private DatabaseConnectionConfig sourceDbConfig;
    private DatabaseConnectionConfig targetDbConfig;
    private List<String> tablesToProcess;
    private Map<String, JobTransformationConfig> tableTransformationConfigs;

    public JobConfigResponse() {
    }

    public JobConfigResponse(String jobId, String jobName, DatabaseConnectionConfig sourceDbConfig, DatabaseConnectionConfig targetDbConfig, List<String> tablesToProcess, Map<String, JobTransformationConfig> tableTransformationConfigs) {
        this.jobId = jobId;
        this.jobName = jobName;
        this.sourceDbConfig = sourceDbConfig;
        this.targetDbConfig = targetDbConfig;
        this.tablesToProcess = tablesToProcess;
        this.tableTransformationConfigs = tableTransformationConfigs;
    }

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
