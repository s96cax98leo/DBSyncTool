package com.yt.etl.common.db;

import com.yt.etl.common.exception.DBSyncException;
import com.yt.etl.common.model.TableMetadata;

import javax.sql.DataSource;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Interface for reading data from a database.
 */
public interface DataReader {

    /**
     * Reads data from the specified table in batches.
     *
     * @param dataSource The DataSource to connect to the database.
     * @param tableName The name of the table to read from.
     * @param columns The list of columns to select. If null or empty, all columns are selected.
     * @param batchSize The number of rows to fetch in each batch.
     * @param offset The starting row offset (0-based).
     * @return A List of Maps, where each Map represents a row (column name -> value).
     * @throws DBSyncException if there is an error reading data.
     */
    List<Map<String, Object>> readData(DataSource dataSource, String tableName, List<String> columns, int batchSize, long offset) throws DBSyncException;

    /**
     * Reads all data from the specified table as a stream.
     * This is suitable for large datasets that may not fit in memory.
     * The caller is responsible for closing the stream, which in turn should handle underlying resources like Connections or ResultSets.
     *
     * @param dataSource The DataSource to connect to the database.
     * @param tableName The name of the table to read from.
     * @param columns The list of columns to select. If null or empty, all columns are selected.
     * @param fetchSize JDBC fetch size hint for the ResultSet.
     * @return A Stream of Maps, where each Map represents a row.
     * @throws DBSyncException if there is an error preparing the stream.
     */
    Stream<Map<String, Object>> streamData(DataSource dataSource, String tableName, List<String> columns, int fetchSize) throws DBSyncException;


    /**
     * Gets the total row count of a table.
     *
     * @param dataSource The DataSource to connect to the database.
     * @param tableName The name of the table.
     * @return The total number of rows in the table.
     * @throws DBSyncException if there is an error counting rows.
     */
    long getRowCount(DataSource dataSource, String tableName) throws DBSyncException;

}
