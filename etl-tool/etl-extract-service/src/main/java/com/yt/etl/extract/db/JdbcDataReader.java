package com.yt.etl.extract.db;

import com.yt.etl.common.db.DataReader;
import com.yt.etl.common.exception.DBSyncException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;

public class JdbcDataReader implements DataReader {
    private static final Logger logger = LoggerFactory.getLogger(JdbcDataReader.class);

    @Override
    public List<Map<String, Object>> readData(DataSource dataSource, String tableName, List<String> columns, int batchSize, long offset) throws DBSyncException {
        List<Map<String, Object>> results = new ArrayList<>();
        String columnsToSelect = (columns == null || columns.isEmpty()) ? "*" : String.join(",", columns);
        // Assuming Oracle syntax for now, this will need to be abstracted later
        String sql = "SELECT " + columnsToSelect + " FROM " + tableName + " OFFSET ? ROWS FETCH NEXT ? ROWS ONLY";

        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql)) {

            pstmt.setLong(1, offset);
            pstmt.setInt(2, batchSize);

            logger.debug("Executing readData query: {} with offset={}, batchSize={}", sql, offset, batchSize);

            try (ResultSet rs = pstmt.executeQuery()) {
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();
                while (rs.next()) {
                    Map<String, Object> row = new HashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        row.put(metaData.getColumnLabel(i).toUpperCase(), rs.getObject(i));
                    }
                    results.add(row);
                }
            }
        } catch (SQLException e) {
            logger.error("Error reading data from table {}: {}", tableName, e.getMessage(), e);
            throw new DBSyncException(DBSyncException.ErrorCode.DATA_READ_ERROR,
                    "Error reading data from table " + tableName, e);
        }
        return results;
    }

    @Override
    public Stream<Map<String, Object>> streamData(DataSource dataSource, String tableName, List<String> columns, int fetchSize) throws DBSyncException {
        String columnsToSelect = (columns == null || columns.isEmpty()) ? "*" : String.join(",", columns);
        String sql = "SELECT " + columnsToSelect + " FROM " + tableName;

        try {
            Connection conn = dataSource.getConnection(); // Will be closed by the stream's onClose handler
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setFetchSize(fetchSize); // Hint for JDBC driver
            logger.debug("Executing streamData query: {}", sql);
            ResultSet rs = pstmt.executeQuery(); // Will be closed by the stream's onClose handler

            Spliterator<Map<String, Object>> spliterator = new Spliterators.AbstractSpliterator<Map<String, Object>>(Long.MAX_VALUE, Spliterator.ORDERED) {
                @Override
                public boolean tryAdvance(Consumer<? super Map<String, Object>> action) {
                    try {
                        if (!rs.next()) {
                            return false;
                        }
                        Map<String, Object> row = new HashMap<>();
                        ResultSetMetaData metaData = rs.getMetaData();
                        int columnCount = metaData.getColumnCount();
                        for (int i = 1; i <= columnCount; i++) {
                            row.put(metaData.getColumnLabel(i).toUpperCase(), rs.getObject(i));
                        }
                        action.accept(row);
                        return true;
                    } catch (SQLException e) {
                        logger.error("Error streaming data from table {}: {}", tableName, e.getMessage(), e);
                        throw new RuntimeException(new DBSyncException(DBSyncException.ErrorCode.DATA_READ_ERROR,
                                "Error streaming data from table " + tableName, e));
                    }
                }
            };

            return StreamSupport.stream(spliterator, false).onClose(() -> {
                try {
                    logger.debug("Closing ResultSet, PreparedStatement, and Connection for table {}", tableName);
                    if (rs != null) rs.close();
                    if (pstmt != null) pstmt.close();
                    if (conn != null) conn.close();
                } catch (SQLException e) {
                    logger.warn("Error closing resources for table {}: {}", tableName, e.getMessage(), e);
                }
            });

        } catch (SQLException e) {
            logger.error("Error preparing stream for table {}: {}", tableName, e.getMessage(), e);
            throw new DBSyncException(DBSyncException.ErrorCode.DATA_READ_ERROR,
                    "Error preparing stream for table " + tableName, e);
        }
    }


    @Override
    public long getRowCount(DataSource dataSource, String tableName) throws DBSyncException {
        String sql = "SELECT COUNT(*) FROM " + tableName;
        try (Connection conn = dataSource.getConnection();
             PreparedStatement pstmt = conn.prepareStatement(sql);
             ResultSet rs = pstmt.executeQuery()) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            logger.error("Error getting row count for table {}: {}", tableName, e.getMessage(), e);
            throw new DBSyncException(DBSyncException.ErrorCode.DATA_READ_ERROR,
                    "Error getting row count for table " + tableName, e);
        }
        return 0;
    }
}
