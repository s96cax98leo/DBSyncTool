package com.yt.etl.common.db;

import com.yt.etl.common.model.TableMetadata;
import com.yt.etl.common.exception.DBSyncException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Interface for writing data to a database.
 */
public interface DataWriter {

    /**
     * Writes a batch of data to the specified table.
     *
     * @param dataSource The DataSource to connect to the database.
     * @param tableName The name of the table to write to.
     * @param data A list of Maps, where each Map represents a row (column name -> value).
     * @param metadata The metadata of the table, used for type casting and statement preparation.
     * @throws DBSyncException if there is an error writing data.
     */
    void writeData(DataSource dataSource, String tableName, List<Map<String, Object>> data, TableMetadata metadata) throws DBSyncException;

    /**
     * Creates a table in the database based on the provided metadata.
     *
     * @param dataSource The DataSource to connect to the database.
     * @param metadata The metadata defining the table structure.
     * @throws DBSyncException if there is an error creating the table.
     */
    void createTable(DataSource dataSource, TableMetadata metadata) throws DBSyncException;

    /**
     * Truncates (deletes all data from) a table.
     *
     * @param dataSource The DataSource to connect to the database.
     * @param tableName The name of the table to truncate.
     * @throws DBSyncException if there is an error truncating the table.
     */
    void truncateTable(DataSource dataSource, String tableName) throws DBSyncException;

    /**
     * Prepares the target table for data loading.
     * This could involve disabling constraints/indexes before load and re-enabling them after.
     * Default implementation does nothing.
     *
     * @param dataSource The DataSource for the target database.
     * @param tableName The name of the target table.
     * @param preLoad True if called before data loading, false if called after.
     * @throws DBSyncException if there is an error during preparation.
     */
    default void prepareTableForLoad(DataSource dataSource, String tableName, boolean preLoad) throws DBSyncException {
        // Default implementation does nothing
    }
}
