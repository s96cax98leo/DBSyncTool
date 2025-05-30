package com.yt.service;

import com.yt.config.DatabaseConfig;
import com.yt.exception.DBSyncException;
import com.yt.model.TableMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class SyncService {
    private static final Logger logger = LoggerFactory.getLogger(SyncService.class);
    private final DatabaseConfig sourceDb;
    private final DatabaseConfig targetDb;
    private final int fetchSize;
    private final ExecutorService executorService;
    private final int tableThreads;
    private final boolean truncateTarget;

    public SyncService(DatabaseConfig sourceDb, DatabaseConfig targetDb, 
                      int fetchSize, int dbThreads, int tableThreads, 
                      boolean truncateTarget) {
        this.sourceDb = sourceDb;
        this.targetDb = targetDb;
        this.fetchSize = fetchSize;
        this.executorService = Executors.newFixedThreadPool(dbThreads);
        this.tableThreads = tableThreads;
        this.truncateTarget = truncateTarget;
    }

    public void syncTables(List<String> tables) {
        List<Future<?>> futures = new ArrayList<>();
        
        for (String table : tables) {
            futures.add(executorService.submit(() -> syncTable(table)));
        }

        // 等待所有表同步完成
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("表同步失敗", e);
                throw new DBSyncException(DBSyncException.ErrorCode.DATA_SYNC_ERROR, 
                    "表同步過程中發生錯誤", e);
            }
        }
    }

    private void syncTable(String tableName) {
        try (Connection sourceConn = sourceDb.getConnection();
             Connection targetConn = targetDb.getConnection()) {
            
            // 驗證表結構
            TableMetadata sourceMetadata = getTableMetadata(sourceConn, tableName);
            validateAndSyncStructure(sourceMetadata, targetConn);

            // 如果需要，清空目標表
            if (truncateTarget) {
                truncateTable(targetConn, tableName);
            }

            // 獲取源表數據總量
            int totalRows = getTableRowCount(sourceConn, tableName);
            logger.info("開始同步表 {}, 總行數: {}", tableName, totalRows);

            // 分批同步數據
            int batchSize = fetchSize;
            int totalBatches = (totalRows + batchSize - 1) / batchSize;
            
            List<Future<?>> batchFutures = new ArrayList<>();
            ExecutorService tableExecutor = Executors.newFixedThreadPool(tableThreads);

            for (int i = 0; i < totalBatches; i++) {
                final int batchIndex = i;
                batchFutures.add(tableExecutor.submit(() -> 
                    syncBatch(tableName, batchIndex * batchSize, batchSize)));
            }

            // 等待所有批次完成
            for (Future<?> future : batchFutures) {
                future.get();
            }

            tableExecutor.shutdown();
            logger.info("表 {} 同步完成", tableName);

        } catch (Exception e) {
            logger.error("同步表 {} 時發生錯誤", tableName, e);
            throw new DBSyncException(DBSyncException.ErrorCode.DATA_SYNC_ERROR, 
                "同步表 " + tableName + " 時發生錯誤", e);
        }
    }

    private TableMetadata getTableMetadata(Connection conn, String tableName) throws SQLException {
        TableMetadata metadata = new TableMetadata(tableName);
        
        // 獲取列信息
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT Column_Name, Data_Type, Data_Length, DATA_PRECISION, " +
                "Nullable, COLUMN_ID FROM user_tab_columns WHERE table_name = ? " +
                "ORDER BY COLUMN_ID")) {
            stmt.setString(1, tableName.toUpperCase());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    metadata.addColumn(new TableMetadata.ColumnMetadata(
                        rs.getString("Column_Name"),
                        rs.getString("Data_Type"),
                        rs.getInt("Data_Length"),
                        rs.getInt("DATA_PRECISION"),
                        "Y".equals(rs.getString("Nullable")),
                        rs.getInt("COLUMN_ID")
                    ));
                }
            }
        }

        // 獲取主鍵信息
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT column_name FROM user_cons_columns WHERE constraint_name IN " +
                "(SELECT constraint_name FROM user_constraints WHERE table_name = ? " +
                "AND constraint_type = 'P')")) {
            stmt.setString(1, tableName.toUpperCase());
            try (ResultSet rs = stmt.executeQuery()) {
                while (rs.next()) {
                    metadata.addPrimaryKey(rs.getString("column_name"));
                }
            }
        }

        return metadata;
    }

    private void validateAndSyncStructure(TableMetadata sourceMetadata, 
                                        Connection targetConn) throws SQLException {
        String tableName = sourceMetadata.getTableName();
        
        // 檢查目標表是否存在
        try (PreparedStatement stmt = targetConn.prepareStatement(
                "SELECT COUNT(*) FROM user_tables WHERE table_name = ?")) {
            stmt.setString(1, tableName.toUpperCase());
            try (ResultSet rs = stmt.executeQuery()) {
                if (!rs.next() || rs.getInt(1) == 0) {
                    // 如果表不存在，創建表
                    createTable(targetConn, sourceMetadata);
                    return;
                }
            }
        }

        // 如果表存在，驗證結構
        TableMetadata targetMetadata = getTableMetadata(targetConn, tableName);
        if (!sourceMetadata.equals(targetMetadata)) {
            throw new DBSyncException(DBSyncException.ErrorCode.STRUCTURE_MISMATCH,
                "表 " + tableName + " 的結構在源和目標數據庫中不匹配");
        }
    }

    private void createTable(Connection conn, TableMetadata metadata) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(metadata.getTableName()).append(" (");

        // 添加列定義
        for (TableMetadata.ColumnMetadata column : metadata.getColumns()) {
            sql.append(column.getName()).append(" ")
               .append(getColumnDefinition(column))
               .append(column.isNullable() ? " NULL" : " NOT NULL")
               .append(", ");
        }

        // 添加主鍵約束
        if (!metadata.getPrimaryKeys().isEmpty()) {
            sql.append("CONSTRAINT PK_").append(metadata.getTableName())
               .append(" PRIMARY KEY (")
               .append(String.join(",", metadata.getPrimaryKeys()))
               .append(")");
        } else {
            // 移除最後的逗號和空格
            sql.setLength(sql.length() - 2);
        }

        sql.append(")");

        try (PreparedStatement stmt = conn.prepareStatement(sql.toString())) {
            stmt.execute();
        }
    }

    private String getColumnDefinition(TableMetadata.ColumnMetadata column) {
        StringBuilder def = new StringBuilder(column.getDataType());
        
        if (column.getDataType().equals("NUMBER")) {
            if (column.getDataPrecision() > 0) {
                def.append("(").append(column.getDataPrecision()).append(")");
            }
        } else if (column.getDataType().equals("VARCHAR2") || 
                   column.getDataType().equals("CHAR")) {
            def.append("(").append(column.getDataLength()).append(")");
        }
        
        return def.toString();
    }

    private void truncateTable(Connection conn, String tableName) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "TRUNCATE TABLE " + tableName)) {
            stmt.execute();
        }
    }

    private int getTableRowCount(Connection conn, String tableName) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(
                "SELECT COUNT(*) FROM " + tableName)) {
            try (ResultSet rs = stmt.executeQuery()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
        }
        return 0;
    }

    private void syncBatch(String tableName, int offset, int batchSize) {
        try (Connection sourceConn = sourceDb.getConnection();
             Connection targetConn = targetDb.getConnection()) {
            
            // 獲取列名列表
            List<String> columns = new ArrayList<>();
            try (PreparedStatement stmt = sourceConn.prepareStatement(
                    "SELECT column_name FROM user_tab_columns WHERE table_name = ? " +
                    "ORDER BY column_id")) {
                stmt.setString(1, tableName.toUpperCase());
                try (ResultSet rs = stmt.executeQuery()) {
                    while (rs.next()) {
                        columns.add(rs.getString("column_name"));
                    }
                }
            }

            // 構建查詢和插入語句
            String selectSql = "SELECT " + String.join(",", columns) + 
                             " FROM " + tableName + 
                             " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";
            
            String insertSql = "INSERT INTO " + tableName + 
                             " (" + String.join(",", columns) + ") VALUES (" +
                             "?,".repeat(columns.size() - 1) + "?)";

            // 執行批量同步
            targetConn.setAutoCommit(false);
            try (PreparedStatement selectStmt = sourceConn.prepareStatement(selectSql);
                 PreparedStatement insertStmt = targetConn.prepareStatement(insertSql)) {
                
                selectStmt.setInt(1, offset);
                selectStmt.setInt(2, batchSize);
                
                try (ResultSet rs = selectStmt.executeQuery()) {
                    while (rs.next()) {
                        for (int i = 0; i < columns.size(); i++) {
                            insertStmt.setObject(i + 1, rs.getObject(i + 1));
                        }
                        insertStmt.addBatch();
                    }
                    insertStmt.executeBatch();
                }
                
                targetConn.commit();
            } catch (SQLException e) {
                targetConn.rollback();
                throw e;
            }
        } catch (SQLException e) {
            logger.error("同步表 {} 的批次數據時發生錯誤 (offset: {}, size: {})", 
                        tableName, offset, batchSize, e);
            throw new DBSyncException(DBSyncException.ErrorCode.DATA_SYNC_ERROR,
                "同步批次數據時發生錯誤", e);
        }
    }

    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
} 