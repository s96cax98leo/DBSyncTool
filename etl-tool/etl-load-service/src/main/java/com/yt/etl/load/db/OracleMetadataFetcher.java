package com.yt.etl.load.db;

import com.yt.etl.common.db.DatabaseMetadataFetcher;
import com.yt.etl.common.exception.DBSyncException;
import com.yt.etl.common.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class OracleMetadataFetcher implements DatabaseMetadataFetcher {
    private static final Logger logger = LoggerFactory.getLogger(OracleMetadataFetcher.class);

    @Override
    public TableMetadata getTableMetadata(DataSource dataSource, String tableName) throws DBSyncException {
        TableMetadata metadata = new TableMetadata(tableName.toUpperCase()); // Oracle stores table names in uppercase by default
        String sqlColumns = "SELECT COLUMN_NAME, DATA_TYPE, DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE, COLUMN_ID, " +
                            "(SELECT COMMENTS FROM USER_COL_COMMENTS UCC WHERE UCC.TABLE_NAME = UTC.TABLE_NAME AND UCC.COLUMN_NAME = UTC.COLUMN_NAME) AS COLUMN_COMMENT " +
                            "FROM USER_TAB_COLUMNS UTC WHERE TABLE_NAME = ? ORDER BY COLUMN_ID";
        String sqlPrimaryKeys = "SELECT COLUMN_NAME FROM USER_CONS_COLUMNS UCC JOIN USER_CONSTRAINTS UC " +
                                "ON UCC.CONSTRAINT_NAME = UC.CONSTRAINT_NAME " +
                                "WHERE UC.TABLE_NAME = ? AND UC.CONSTRAINT_TYPE = 'P' ORDER BY UCC.POSITION";
        String sqlTableComment = "SELECT COMMENTS FROM USER_TAB_COMMENTS WHERE TABLE_NAME = ?";


        try (Connection conn = dataSource.getConnection()) {
            // Fetch column metadata
            try (PreparedStatement pstmt = conn.prepareStatement(sqlColumns)) {
                pstmt.setString(1, tableName.toUpperCase());
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        TableMetadata.ColumnMetadata column = new TableMetadata.ColumnMetadata(
                                rs.getString("COLUMN_NAME"),
                                rs.getString("DATA_TYPE"),
                                rs.getInt("DATA_LENGTH"),
                                rs.getInt("DATA_PRECISION"),
                                rs.getInt("DATA_SCALE"),
                                "Y".equals(rs.getString("NULLABLE")),
                                rs.getInt("COLUMN_ID")
                        );
                        column.setComment(rs.getString("COLUMN_COMMENT"));
                        metadata.addColumn(column);
                    }
                }
            }

            if (metadata.getColumns().isEmpty()) {
                logger.warn("No columns found for table {} in schema. It might not exist or is not accessible.", tableName);
                 if (!tableExists(dataSource, tableName)) {
                    throw new DBSyncException(DBSyncException.ErrorCode.TABLE_NOT_FOUND, "Table " + tableName + " not found.");
                }
                // If table exists but no columns, it's strange, but proceed. Or throw an error.
                // For now, let's assume this means an empty (definition-wise) table if tableExists is true.
            }

            // Fetch primary key metadata
            try (PreparedStatement pstmt = conn.prepareStatement(sqlPrimaryKeys)) {
                pstmt.setString(1, tableName.toUpperCase());
                try (ResultSet rs = pstmt.executeQuery()) {
                    while (rs.next()) {
                        metadata.addPrimaryKey(rs.getString("COLUMN_NAME"));
                    }
                }
            }

            // Fetch table comment
            try (PreparedStatement pstmt = conn.prepareStatement(sqlTableComment)) {
                pstmt.setString(1, tableName.toUpperCase());
                try (ResultSet rs = pstmt.executeQuery()) {
                    if (rs.next()) {
                        metadata.setTableComment(rs.getString("COMMENTS"));
                    }
                }
            }

        } catch (SQLException e) {
            logger.error("Error fetching metadata for table {}: {}", tableName, e.getMessage(), e);
            throw new DBSyncException(DBSyncException.ErrorCode.SCHEMA_METADATA_ERROR,
                    "Error fetching metadata for table " + tableName, e);
        }
        return metadata;
    }

    @Override
    public List<String> listTableNames(DataSource dataSource) throws DBSyncException {
        List<String> tableNames = new ArrayList<>();
        // Lists tables in the current user's schema
        String sql = "SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            while (rs.next()) {
                tableNames.add(rs.getString("TABLE_NAME"));
            }
        } catch (SQLException e) {
            logger.error("Error listing table names: {}", e.getMessage(), e);
            throw new DBSyncException(DBSyncException.ErrorCode.SCHEMA_METADATA_ERROR,
                    "Error listing table names", e);
        }
        return tableNames;
    }

    @Override
    public boolean tableExists(DataSource dataSource, String tableName) throws DBSyncException {
        // For Oracle, table names are typically stored in uppercase unless quoted
        String checkTableName = tableName.toUpperCase();
        String sql = "SELECT COUNT(*) FROM USER_TABLES WHERE TABLE_NAME = ?";
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, checkTableName);
            try (ResultSet rs = pstmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1) > 0;
                }
            }
        } catch (SQLException e) {
            logger.error("Error checking if table {} exists: {}", tableName, e.getMessage(), e);
            throw new DBSyncException(DBSyncException.ErrorCode.SCHEMA_METADATA_ERROR,
                    "Error checking if table " + tableName + " exists", e);
        }
        return false;
    }
}
