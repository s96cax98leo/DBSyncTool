package com.yt.etl.load.db;

import com.yt.etl.common.db.DataWriter;
import com.yt.etl.common.exception.DBSyncException;
import com.yt.etl.common.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class JdbcDataWriter implements DataWriter {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDataWriter.class);

    @Override
    public void writeData(DataSource dataSource, String tableName, List<Map<String, Object>> data, TableMetadata metadata) throws DBSyncException {
        if (data == null || data.isEmpty()) {
            logger.info("No data provided to write for table {}", tableName);
            return;
        }

        List<TableMetadata.ColumnMetadata> columns = metadata.getColumns();
        if (columns == null || columns.isEmpty()) {
            throw new DBSyncException(DBSyncException.ErrorCode.SCHEMA_METADATA_ERROR, "No column metadata available for table " + tableName);
        }

        String columnNames = columns.stream().map(TableMetadata.ColumnMetadata::getName).collect(Collectors.joining(","));
        String placeholders = columns.stream().map(c -> "?").collect(Collectors.joining(","));
        String sql = "INSERT INTO " + tableName + " (" + columnNames + ") VALUES (" + placeholders + ")";

        try (Connection conn = dataSource.getConnection()) {
            conn.setAutoCommit(false); // Batch processing
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                for (Map<String, Object> row : data) {
                    for (int i = 0; i < columns.size(); i++) {
                        TableMetadata.ColumnMetadata column = columns.get(i);
                        Object value = row.get(column.getName().toUpperCase()); // Assuming keys in map are uppercase
                        if (value == null && !column.isNullable()) {
                             logger.warn("Null value for non-nullable column {} in table {}. Skipping row or using default if applicable.", column.getName(), tableName);
                             // Decide on handling: throw error, skip row, or use a default. For now, let JDBC driver handle it or use setObject with null.
                        }
                        // pstmt.setObject(i + 1, value, mapOracleType(column.getDataType())); // For more precise type handling
                        pstmt.setObject(i + 1, value);
                    }
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                conn.commit();
                logger.debug("Successfully wrote {} rows to table {}", data.size(), tableName);
            } catch (SQLException e) {
                conn.rollback();
                logger.error("Error writing data to table {}: {}", tableName, e.getMessage(), e);
                throw new DBSyncException(DBSyncException.ErrorCode.DATA_WRITE_ERROR, "Error writing data to table " + tableName, e);
            }
        } catch (SQLException e) {
            logger.error("Failed to obtain or manage connection for table {}: {}", tableName, e.getMessage(), e);
            throw new DBSyncException(DBSyncException.ErrorCode.CONNECTION_ERROR, "Failed to obtain or manage connection for table " + tableName, e);
        }
    }

    @Override
    public void createTable(DataSource dataSource, TableMetadata metadata) throws DBSyncException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(metadata.getTableName()).append(" (");

        for (TableMetadata.ColumnMetadata column : metadata.getColumns()) {
            sql.append(column.getName()).append(" ")
               .append(getColumnDefinition(column)); // Oracle specific definition
            if (column.isNullable()) {
                sql.append(" NULL");
            } else {
                sql.append(" NOT NULL");
            }
            sql.append(", ");
        }

        if (!metadata.getPrimaryKeys().isEmpty()) {
            sql.append("CONSTRAINT PK_").append(metadata.getTableName())
               .append(" PRIMARY KEY (")
               .append(String.join(",", metadata.getPrimaryKeys()))
               .append(")");
        } else {
            if (!metadata.getColumns().isEmpty()) {
                 sql.setLength(sql.length() - 2); // Remove last comma and space if there are columns
            } else {
                // Handle case with no columns and no PKs (though unusual)
                // This might result in "CREATE TABLE FOO ()" which is invalid.
                // A table must have at least one column.
                if (metadata.getColumns().isEmpty()){
                    throw new DBSyncException(DBSyncException.ErrorCode.SCHEMA_METADATA_ERROR, "Cannot create table " + metadata.getTableName() + " with no columns.");
                }
            }
        }
        sql.append(")");

        if (metadata.getTableComment() != null && !metadata.getTableComment().isEmpty()) {
            // This is a separate statement for Oracle
            // sql.append("\nCOMMENT ON TABLE ").append(metadata.getTableName()).append(" IS '").append(metadata.getTableComment().replace("'", "''")).append("'");
            // For simplicity, we'll execute create table first, then comments.
        }


        logger.debug("Executing create table statement: {}", sql.toString());
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.execute();
            logger.info("Table {} created successfully.", metadata.getTableName());

            // Add table comment if present
            if (metadata.getTableComment() != null && !metadata.getTableComment().isEmpty()) {
                String commentSql = "COMMENT ON TABLE " + metadata.getTableName() + " IS ?";
                try (PreparedStatement commentStmt = conn.prepareStatement(commentSql)) {
                    commentStmt.setString(1, metadata.getTableComment());
                    commentStmt.execute();
                    logger.info("Comment added to table {}.", metadata.getTableName());
                } catch (SQLException e) {
                     logger.warn("Failed to add comment to table {}: {}", metadata.getTableName(), e.getMessage());
                }
            }

            // Add column comments if present
            for(TableMetadata.ColumnMetadata column : metadata.getColumns()){
                if(column.getComment() != null && !column.getComment().isEmpty()){
                    String columnCommentSql = "COMMENT ON COLUMN " + metadata.getTableName() + "." + column.getName() + " IS ?";
                     try (PreparedStatement colCommentStmt = conn.prepareStatement(columnCommentSql)) {
                        colCommentStmt.setString(1, column.getComment());
                        colCommentStmt.execute();
                    } catch (SQLException e) {
                        logger.warn("Failed to add comment to column {} on table {}: {}", column.getName(), metadata.getTableName(), e.getMessage());
                    }
                }
            }

        } catch (SQLException e) {
            logger.error("Error creating table {}: {}", metadata.getTableName(), e.getMessage(), e);
            throw new DBSyncException(DBSyncException.ErrorCode.SCHEMA_METADATA_ERROR, "Error creating table " + metadata.getTableName(), e);
        }
    }

    // Adapted from SyncService, Oracle specific
    private String getColumnDefinition(TableMetadata.ColumnMetadata column) {
        StringBuilder def = new StringBuilder();
        String dataType = column.getDataType().toUpperCase();

        switch (dataType) {
            case "VARCHAR2":
            case "CHAR":
                def.append(dataType).append("(").append(column.getDataLength()).append(" BYTE)"); // Specify BYTE for char length semantics often desired in migration
                break;
            case "NVARCHAR2":
                 def.append(dataType).append("(").append(column.getDataLength() / 2).append(")"); // Assuming dataLength is byte length for NVARCHAR2
                 break;
            case "NUMBER":
                if (column.getDataPrecision() > 0) {
                    def.append(dataType).append("(").append(column.getDataPrecision());
                    if (column.getDataScale() > 0) {
                        def.append(",").append(column.getDataScale());
                    }
                    def.append(")");
                } else {
                     // NUMBER without precision/scale is a floating point number
                    def.append(dataType);
                }
                break;
            case "DATE":
            case "TIMESTAMP":
            case "CLOB":
            case "BLOB":
            case "RAW": // For RAW, DATA_LENGTH is important
                 if ("RAW".equals(dataType)) {
                    def.append(dataType).append("(").append(column.getDataLength()).append(")");
                 } else {
                    def.append(dataType);
                 }
                 break;
            default:
                // For types like TIMESTAMP(6) WITH TIME ZONE, etc.
                // The original type from USER_TAB_COLUMNS.DATA_TYPE is usually sufficient.
                def.append(dataType);
                logger.warn("Using direct data type '{}' for column {}. Review if specific formatting is needed.", column.getDataType(), column.getName());
                break;
        }
        return def.toString();
    }


    @Override
    public void truncateTable(DataSource dataSource, String tableName) throws DBSyncException {
        String sql = "TRUNCATE TABLE " + tableName;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement stmt = conn.prepareStatement(sql)) {
            stmt.execute();
            logger.info("Table {} truncated successfully.", tableName);
        } catch (SQLException e) {
            logger.error("Error truncating table {}: {}", tableName, e.getMessage(), e);
            // Fallback to DELETE if TRUNCATE fails (e.g., due to FK constraints if not handled)
            // For now, just throw. Could add a property to control this behavior.
            /*
            logger.warn("Truncate failed for table {}, attempting DELETE FROM.", tableName);
            try (Connection connFallback = dataSource.getConnection();
                 PreparedStatement deleteStmt = connFallback.prepareStatement("DELETE FROM " + tableName)) {
                deleteStmt.executeUpdate();
                logger.info("Table {} cleared with DELETE FROM.", tableName);
            } catch (SQLException deleteEx) {
                 logger.error("Error clearing table {} with DELETE FROM: {}", tableName, deleteEx.getMessage(), deleteEx);
                 throw new DBSyncException(DBSyncException.ErrorCode.DATA_WRITE_ERROR, "Error clearing table " + tableName, deleteEx);
            }
            */
            throw new DBSyncException(DBSyncException.ErrorCode.DATA_WRITE_ERROR, "Error truncating table " + tableName, e);
        }
    }
}
