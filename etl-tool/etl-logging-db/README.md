# ETL Logging DB Module

This module is responsible for managing the database schema and migrations for the ETL logging database.

Flyway or Liquibase scripts will be added here to manage schema evolution.

The orchestration service will use this database to store job status, logs, and other metadata.
