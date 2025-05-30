package com.yt.exception;

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
        CONFIGURATION_ERROR("配置錯誤"),
        CONNECTION_ERROR("數據庫連接錯誤"),
        TABLE_NOT_FOUND("表不存在"),
        STRUCTURE_MISMATCH("表結構不匹配"),
        DATA_SYNC_ERROR("數據同步錯誤"),
        VALIDATION_ERROR("數據驗證錯誤"),
        UNKNOWN_ERROR("未知錯誤");

        private final String description;

        ErrorCode(String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }
} 