-- For EtlJobConfigEntity
CREATE TABLE etl_job_configs (
    job_id VARCHAR(128) PRIMARY KEY, -- Matched length from EtlJobConfigEntity
    job_name VARCHAR(255) NOT NULL,  -- Added NOT NULL constraint

    source_db_conn_name VARCHAR(255),
    source_db_jdbc_url TEXT,
    source_db_username VARCHAR(255),
    source_db_password VARCHAR(512), -- Storing encrypted password is best practice
    source_db_driver_class_name VARCHAR(255),
    source_db_additional_props_json TEXT, -- Storing Map as JSON string

    target_db_conn_name VARCHAR(255),
    target_db_jdbc_url TEXT,
    target_db_username VARCHAR(255),
    target_db_password VARCHAR(512),
    target_db_driver_class_name VARCHAR(255),
    target_db_additional_props_json TEXT, -- Storing Map as JSON string

    -- Storing List<String> as JSON string requires an AttributeConverter in the entity
    -- or handle serialization/deserialization in service layer.
    -- TEXT is a generic choice. For PostgreSQL, JSONB would be better if using native JSON features.
    tables_to_process TEXT,

    -- Storing Map<String, JobTransformationConfig> as JSON string
    table_transformation_configs_json TEXT, -- Matched name from EtlJobConfigEntity

    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE UNIQUE INDEX idx_etl_job_configs_job_name ON etl_job_configs(job_name);


-- For JobExecutionLogEntity
CREATE TABLE job_execution_logs (
    execution_id BIGSERIAL PRIMARY KEY,
    job_config_id VARCHAR(128) NOT NULL REFERENCES etl_job_configs(job_id) ON DELETE CASCADE, -- Matched name from entity
    start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    status VARCHAR(50) NOT NULL, -- PENDING, RUNNING, COMPLETED_SUCCESSFULLY, FAILED, PARTIALLY_COMPLETED_WITH_FAILURES
    last_message TEXT,

    total_tables_to_process INTEGER DEFAULT 0,
    tables_completed INTEGER DEFAULT 0,
    tables_failed INTEGER DEFAULT 0,

    -- These granular counts might be better suited for JobExecutionTableLogEntity
    -- Or they represent the sum totals for the entire job run. Let's assume sum totals here.
    total_records_extracted BIGINT DEFAULT 0,
    total_records_transformed BIGINT DEFAULT 0,
    total_records_loaded BIGINT DEFAULT 0,

    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_job_execution_logs_job_config_id ON job_execution_logs(job_config_id);
CREATE INDEX idx_job_execution_logs_status ON job_execution_logs(status);


-- For JobExecutionTableLogEntity (New Detail Table)
CREATE TABLE job_execution_table_logs (
    table_log_id BIGSERIAL PRIMARY KEY,
    execution_id BIGINT NOT NULL REFERENCES job_execution_logs(execution_id) ON DELETE CASCADE,
    table_name VARCHAR(255) NOT NULL,
    status VARCHAR(50) NOT NULL, -- PENDING, EXTRACTING, TRANSFORMING, LOADING, COMPLETED_SUCCESSFULLY, FAILED
    start_time TIMESTAMP WITHOUT TIME ZONE,
    end_time TIMESTAMP WITHOUT TIME ZONE,
    records_extracted BIGINT DEFAULT 0,
    records_transformed BIGINT DEFAULT 0,
    records_loaded BIGINT DEFAULT 0,
    error_message TEXT, -- Store detailed error messages for table-specific failures
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_job_execution_table_logs_execution_id ON job_execution_table_logs(execution_id);
CREATE INDEX idx_job_execution_table_logs_table_name ON job_execution_table_logs(table_name);
CREATE INDEX idx_job_execution_table_logs_status ON job_execution_table_logs(status);
