package org.apache.phoenix.ddb.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListTablesService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListTablesService.class);
    // TODO: we will use TABLE_SCHEM later on to differentiate ddb tables
    private static String SYSCAT_QUERY =
            "SELECT TABLE_NAME FROM SYSTEM.CATALOG WHERE TENANT_ID IS NULL AND TABLE_SCHEM = 'DDB'"
                    + " AND COLUMN_NAME IS NULL AND COLUMN_FAMILY IS NULL AND "
                    + "TABLE_TYPE = 'u' %s LIMIT %d";

    public static Map<String, Object> listTables(Map<String, Object> request, String connectionUrl) {
        String exclusiveStartTableName = (String) request.getOrDefault("ExclusiveStartTableName", null);
        int limit = (int) request.getOrDefault("Limit", 100);
        String exclusiveStartTableNameClause = exclusiveStartTableName == null
                ? ""
                : " AND TABLE_NAME > '" + exclusiveStartTableName + "'";
        String query = String.format(SYSCAT_QUERY, exclusiveStartTableNameClause, limit);
        LOGGER.info("Query for List Tables: {}", query);
        List<String> tableNames = new ArrayList<>();
        String lastEvaluatedTableName = null;
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            ResultSet rs = connection.createStatement().executeQuery(query);
            while (rs.next()) {
                lastEvaluatedTableName = rs.getString(1);
                tableNames.add(lastEvaluatedTableName);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> response = new HashMap<>();
        response.put("TableNames", tableNames);
        response.put("LastEvaluatedTableName", lastEvaluatedTableName);
        return response;
    }
}
