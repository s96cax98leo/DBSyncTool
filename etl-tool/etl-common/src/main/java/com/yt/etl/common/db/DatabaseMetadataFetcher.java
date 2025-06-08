package com.yt.etl.common.db;

import com.yt.etl.common.model.TableMetadata;
import com.yt.etl.common.exception.DBSyncException;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.List;

/**
 * Interface for fetching database metadata.
 */
public interface DatabaseMetadataFetcher {

    /**
     * Retrieves metadata for a specific table.
     *
     * @param dataSource The DataSource to connect to the database.
     * @param tableName The name of the table.
     * @return TableMetadata object containing the table's schema information.
     * @throws DBSyncException if there is an error fetching metadata or the table is not found.
     */
    TableMetadata getTableMetadata(DataSource dataSource, String tableName) throws DBSyncException;

    /**
     * Lists all table names in the database/schema.
     *
     * @param dataSource The DataSource to connect to the database.
     * @return A list of table names.
     * @throws DBSyncException if there is an error listing tables.
     */
    List<String> listTableNames(DataSource dataSource) throws DBSyncException;

    /**
     * Checks if a table exists.
     *
     * @param dataSource The DataSource to connect to the database.
     * @param tableName The name of the table.
     * @return true if the table exists, false otherwise.
     * @throws DBSyncException if there is an error during the check.
     */
    boolean tableExists(DataSource dataSource, String tableName) throws DBSyncException;
}
