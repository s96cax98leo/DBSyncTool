package com.yt.etl.common.exception;

public class DBSyncException extends RuntimeException {
    private final ErrorCode errorCode;

    public DBSyncException(ErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public DBSyncException(ErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ErrorCode getErrorCode() {
        return errorCode;
    }

    public enum ErrorCode {
        CONFIGURATION_ERROR("Configuration Error"), // Descriptions updated to English for broader usability
        CONNECTION_ERROR("Database Connection Error"),
        TABLE_NOT_FOUND("Table Not Found"),
        SCHEMA_METADATA_ERROR("Schema Metadata Error"), // More specific than structure mismatch
        DATA_READ_ERROR("Data Read Error"), // More specific for ETL context
        DATA_WRITE_ERROR("Data Write Error"), // More specific for ETL context
        DATA_TRANSFORMATION_ERROR("Data Transformation Error"),
        VALIDATION_ERROR("Data Validation Error"),
        SERVICE_UNAVAILABLE("Service Unavailable"), // For inter-service communication issues
        UNSUPPORTED_OPERATION("Unsupported Operation"),
        UNKNOWN_ERROR("Unknown Error");

        private final String description;

        ErrorCode(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }

        @Override
        public String toString() {
            return name() + ": " + description;
        }
    }
}
