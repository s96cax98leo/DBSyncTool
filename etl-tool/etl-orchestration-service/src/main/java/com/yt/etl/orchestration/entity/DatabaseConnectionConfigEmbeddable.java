package com.yt.etl.orchestration.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Lob;
import java.util.Map;
import java.util.Objects;

@Embeddable
public class DatabaseConnectionConfigEmbeddable {

    @Column(name = "db_conn_name")
    private String connectionName;

    @Column(name = "db_jdbc_url", length = 1024)
    private String jdbcUrl;

    @Column(name = "db_username")
    private String username;

    @Column(name = "db_password", length = 512) // Store encrypted in a real scenario
    private String password;

    @Column(name = "db_driver_class_name")
    private String driverClassName;

    // Using @Lob for Map can be problematic. A better way is a custom converter or a separate table.
    // For simplicity with H2, a String might work if we serialize Map to String.
    // However, direct Map support varies by JPA provider and DB.
    // For now, let's assume this will be handled by a converter in EtlJobConfigEntity if needed,
    // or simplify to a few common properties if a map is too complex for @Embedded.
    // Let's use a String to store JSON for now, and the entity will handle conversion.
    @Lob
    @Column(name = "db_additional_properties_json")
    private String additionalPropertiesJson;


    public DatabaseConnectionConfigEmbeddable() {
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

    public String getAdditionalPropertiesJson() {
        return additionalPropertiesJson;
    }

    public void setAdditionalPropertiesJson(String additionalPropertiesJson) {
        this.additionalPropertiesJson = additionalPropertiesJson;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DatabaseConnectionConfigEmbeddable that = (DatabaseConnectionConfigEmbeddable) o;
        return Objects.equals(connectionName, that.connectionName) &&
               Objects.equals(jdbcUrl, that.jdbcUrl) &&
               Objects.equals(username, that.username) &&
               Objects.equals(password, that.password) &&
               Objects.equals(driverClassName, that.driverClassName) &&
               Objects.equals(additionalPropertiesJson, that.additionalPropertiesJson);
    }

    @Override
    public int hashCode() {
        return Objects.hash(connectionName, jdbcUrl, username, password, driverClassName, additionalPropertiesJson);
    }

    @Override
    public String toString() {
        return "DatabaseConnectionConfigEmbeddable{" +
                "connectionName='" + connectionName + '\'' +
                ", jdbcUrl='" + jdbcUrl + '\'' +
                ", username='" + username + '\'' +
                ", driverClassName='" + driverClassName + '\'' +
                ", additionalPropertiesJson='" + (additionalPropertiesJson != null ? additionalPropertiesJson.length() + " chars" : "null") + '\'' +
                '}';
    }
}
