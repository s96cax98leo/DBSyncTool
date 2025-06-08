package com.yt.etl.common.config;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class DatabaseConfig {
    private static final Logger logger = LoggerFactory.getLogger(DatabaseConfig.class);
    private final HikariDataSource dataSource;

    public DatabaseConfig(File configFile) {
        Properties props = loadProperties(configFile);
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(props.getProperty("url"));
        config.setUsername(props.getProperty("username"));
        config.setPassword(props.getProperty("password"));

        // Connection pool configuration
        config.setMaximumPoolSize(Integer.parseInt(props.getProperty("db.connection.maxPoolSize", "10")));
        config.setMinimumIdle(Integer.parseInt(props.getProperty("db.connection.minIdle", "5")));
        config.setIdleTimeout(Long.parseLong(props.getProperty("db.connection.idleTimeout", "300000")));
        config.setConnectionTimeout(Long.parseLong(props.getProperty("db.connection.connectionTimeout", "20000")));
        config.setValidationTimeout(Long.parseLong(props.getProperty("db.connection.validationTimeout", "5000")));
        config.setMaxLifetime(Long.parseLong(props.getProperty("db.connection.maxLifetime", "1200000")));

        // Oracle specific configuration - fanEnabled removed/commented
        // config.addDataSourceProperty("oracle.jdbc.fanEnabled", "false");
        config.addDataSourceProperty("cachePrepStmts", props.getProperty("db.oracle.cachePrepStmts", "true"));
        config.addDataSourceProperty("prepStmtCacheSize", props.getProperty("db.oracle.prepStmtCacheSize", "250"));
        config.addDataSourceProperty("prepStmtCacheSqlLimit", props.getProperty("db.oracle.prepStmtCacheSqlLimit", "2048"));

        dataSource = new HikariDataSource(config);
        logger.info("Database connection pool initialized using configuration from {}", configFile.getName());
    }

    private Properties loadProperties(File configFile) {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(configFile)) {
            props.load(fis);
        } catch (IOException e) {
            logger.error("Error loading database configuration from {}", configFile.getName(), e);
            // Consider a more specific runtime exception, perhaps from etl.common.exception
            throw new RuntimeException("Failed to load database configuration file: " + configFile.getName(), e);
        }
        return props;
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void close() {
        if (dataSource != null && !dataSource.isClosed()) {
            dataSource.close();
            logger.info("Database connection pool closed.");
        }
    }
}
