package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.IndexUtil;
import org.apache.phoenix.util.SchemaUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class IndexBuildingActivator {

    private static final Logger LOGGER = LoggerFactory.getLogger(IndexBuildingActivator.class);

    private static final String SELECT_CREATE_DISABLE_INDEX = "SELECT DISTINCT TABLE_SCHEM, TABLE_NAME "
            + "FROM SYSTEM.CATALOG "
            + "WHERE COLUMN_NAME IS NULL "
            + "AND COLUMN_FAMILY IS NULL "
            + "AND TABLE_TYPE = 'i'"
            + "AND INDEX_STATE = 'c' "
            + "AND TENANT_ID IS NULL "
            + "AND TO_NUMBER(CURRENT_TIME()) - LAST_DDL_TIMESTAMP > %d";

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
}
