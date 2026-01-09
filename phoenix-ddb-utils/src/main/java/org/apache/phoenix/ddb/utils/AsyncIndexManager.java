package org.apache.phoenix.ddb.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class AsyncIndexManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncIndexManager.class);

    private static final String SELECT_CREATE_DISABLE_INDEX = "SELECT DISTINCT TABLE_SCHEM, TABLE_NAME "
            + "FROM SYSTEM.CATALOG "
            + "WHERE TABLE_SCHEM='DDB'"
            + "AND COLUMN_NAME IS NULL "
            + "AND COLUMN_FAMILY IS NULL "
            + "AND TABLE_TYPE = 'i'"
            + "AND INDEX_STATE = 'c' "
            + "AND TENANT_ID IS NULL "
            + "AND (TO_NUMBER(CURRENT_TIME()) - LAST_DDL_TIMESTAMP) > %d";

    private static final String SELECT_BUILDING_INDEX = "SELECT DISTINCT TABLE_SCHEM, TABLE_NAME "
            + "FROM SYSTEM.CATALOG "
            + "WHERE TABLE_SCHEM='DDB'"
            + "AND COLUMN_NAME IS NULL "
            + "AND COLUMN_FAMILY IS NULL "
            + "AND TABLE_TYPE = 'i'"
            + "AND INDEX_STATE = 'b' "
            + "AND TENANT_ID IS NULL "
            + "AND (TO_NUMBER(CURRENT_TIME()) - LAST_DDL_TIMESTAMP) > %d";

    private static final String SELECT_DISABLED_INDEX = "SELECT DISTINCT TABLE_SCHEM, TABLE_NAME "
            + "FROM SYSTEM.CATALOG "
            + "WHERE TABLE_SCHEM='DDB'"
            + "AND COLUMN_NAME IS NULL "
            + "AND COLUMN_FAMILY IS NULL "
            + "AND TABLE_TYPE = 'i'"
            + "AND INDEX_STATE = 'x' "
            + "AND TENANT_ID IS NULL "
            + "AND (TO_NUMBER(CURRENT_TIME()) - LAST_DDL_TIMESTAMP) > %d";

    public static void run(Connection connection) throws SQLException {
        activateIndexesForBuilding(connection, 1800010);
        dropDisabledIndexes(connection, 1800010);
        runIndexTool(connection, 1860000);
    }

    public static void activateIndexesForBuilding(Connection conn, int minAgeMs) throws SQLException {
        ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_CREATE_DISABLE_INDEX, minAgeMs));
        while (rs.next()) {
            String schemaName = rs.getString(1);
            String tableName = rs.getString(2);
            String fullName = SchemaUtil.getTableName(schemaName, tableName);
            LOGGER.info("Found index " + fullName  + " to activate");
            IndexUtil.updateIndexState(conn.unwrap(PhoenixConnection.class),
                    fullName,
                    PIndexState.BUILDING, EnvironmentEdgeManager.currentTimeMillis());
        }
    }

    public static void runIndexTool(Connection conn, int minAgeMs) throws SQLException {
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_BUILDING_INDEX, minAgeMs));
            while (rs.next()) {
                String schemaName = rs.getString(1);
                String indexName = rs.getString(2);
                String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
                PTable ptable = conn.unwrap(PhoenixConnection.class).getTable(fullIndexName);
                String tableName = PhoenixUtils.getTableNameFromFullName(ptable.getParentName().getString(), false);
                LOGGER.info("Found index " + fullIndexName + " to build");
                Configuration conf = conn.unwrap(PhoenixConnection.class).getQueryServices().getConfiguration();
                PhoenixUtils.runIndexTool(conf, false, "DDB",
                        PhoenixUtils.getEscapedArgument(tableName),
                        PhoenixUtils.getEscapedArgument(indexName),
                        null, IndexTool.IndexVerifyType.NONE);
            }
        } catch (Exception e) {
            LOGGER.error("Error in building indexes using IndexTool", e);
        }
    }

    public static void dropDisabledIndexes(Connection conn, int minAgeMs) throws SQLException {
        try {
            ResultSet rs = conn.createStatement().executeQuery(String.format(SELECT_DISABLED_INDEX, minAgeMs));
            while (rs.next()) {
                String schemaName = rs.getString(1);
                String indexName = rs.getString(2);
                String fullIndexName = SchemaUtil.getTableName(schemaName, indexName);
                PTable ptable = conn.unwrap(PhoenixConnection.class).getTable(fullIndexName);
                String parentTable = PhoenixUtils.getFullTableName(ptable.getParentTableName().getString(), true);
                conn.createStatement().execute("DROP INDEX \"" + indexName + "\" ON " + parentTable);
            }
        } catch (Exception e) {
            LOGGER.error("Error in dropping disabled indexes.", e);
        }
    }
}
