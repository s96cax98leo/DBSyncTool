package com.yt.etl.orchestration.dto;

public class StartJobResponse {
    private String jobId;
    private Long executionId;
    private String message;
    private String status; // e.g., "SUBMITTED", "STARTED", "FAILED_PRECONDITION"

    public StartJobResponse() {
    }

    public StartJobResponse(String jobId, Long executionId, String message, String status) {
        this.jobId = jobId;
        this.executionId = executionId;
        this.message = message;
        this.status = status;
    }

    // Getters and Setters
    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public Long getExecutionId() {
        return executionId;
    }

    public void setExecutionId(Long executionId) {
        this.executionId = executionId;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
