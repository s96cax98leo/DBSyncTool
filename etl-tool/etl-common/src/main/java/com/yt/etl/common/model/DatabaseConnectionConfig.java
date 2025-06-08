package com.yt.etl.common.model;

import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.Map;
import java.util.Objects;

// This class might be annotated with @Embeddable if used directly in an @Embedded JPA context
// For now, keeping it as a plain POJO. A separate Embeddable DTO might be created in orchestration service if needed.
public class DatabaseConnectionConfig {

    private String connectionName; // e.g., "sourceOraclePROD", "targetPostgresDEV"
    private String jdbcUrl;
    private String username;
    private String password; // Sensitive field
    private String driverClassName;
    private Map<String, String> additionalProperties; // For HikariCP or other driver-specific settings

    public DatabaseConnectionConfig() {
    }

    public DatabaseConnectionConfig(String connectionName, String jdbcUrl, String username, String password, String driverClassName, Map<String, String> additionalProperties) {
        this.connectionName = connectionName;
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
        this.driverClassName = driverClassName;
        this.additionalProperties = additionalProperties;
    }

    // Getters and Setters
    public String getConnectionName() {
        return connectionName;
    }

    public void setConnectionName(String connectionName) {
        this.connectionName = connectionName;
    }

    public String getJdbcUrl() {
        return jdbcUrl;
    }

    public void setJdbcUrl(String jdbcUrl) {
        this.jdbcUrl = jdbcUrl;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    @JsonIgnore // Example: prevent password from being easily serialized to logs/UI if this object is passed around
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getDriverClassName() {
        return driverClassName;
    }

    public void setDriverClassName(String driverClassName) {
        this.driverClassName = driverClassName;
    }

    public Map<String, String> getAdditionalProperties() {
        return additionalProperties;
    }

    public void setAdditionalProperties(Map<String, String> additionalProperties) {
        this.additionalProperties = additionalProperties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseConnectionConfig that = (DatabaseConnectionConfig) o;
        return Objects.equals(connectionName, that.connectionName) &&
               Objects.equals(jdbcUrl, that.jdbcUrl) &&
               Objects.equals(username, that.username) &&
               Objects.equals(password, that.password) && // Note: equality on password field
               Objects.equals(driverClassName, that.driverClassName) &&
               Objects.equals(additionalProperties, that.additionalProperties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionName, jdbcUrl, username, password, driverClassName, additionalProperties);
    }

    @Override
    public String toString() {
        return "DatabaseConnectionConfig{" +
                "connectionName='" + connectionName + '\'' +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                ", username='" + username + '\'' +
                // Password intentionally omitted from toString for security
                ", driverClassName='" + driverClassName + '\'' +
                ", additionalProperties=" + additionalProperties +
                '}';
    }
}
