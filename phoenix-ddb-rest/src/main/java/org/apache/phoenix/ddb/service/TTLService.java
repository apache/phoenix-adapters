package org.apache.phoenix.ddb.service;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class TTLService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TTLService.class);
    private static final String TTL_EXPRESSION = "TO_NUMBER(CURRENT_TIME()) > BSON_VALUE(COL, ''%s'', ''BIGINT'')";
    private static final String ALTER_TTL_STMT = "ALTER TABLE %s SET TTL = '%s'";

    public static Map<String, Object> updateTimeToLive(Map<String, Object> request,
                                                       String connectionUrl) {
        String tableName = (String) request.get("TableName");
        Map<String, Object> ttlSpec = (Map<String, Object>) request.get("TimeToLiveSpecification");
        String colName = (String) ttlSpec.get("AttributeName");
        Boolean enabled = (Boolean) ttlSpec.get("Enabled");
        String alterStmt;
        if (enabled) {
            String ttlExpression = String.format(TTL_EXPRESSION, colName);
            alterStmt = String.format(ALTER_TTL_STMT, tableName, ttlExpression);
        } else {
            alterStmt = String.format(ALTER_TTL_STMT, tableName, HConstants.FOREVER);
        }
        LOGGER.info("SQL for UpdateTimeToLive: " + alterStmt);
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.createStatement().execute(alterStmt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> response = new HashMap<>();
        response.put("TimeToLiveSpecification", ttlSpec);
        return response;
    }

    public static Map<String, Object> describeTimeToLive(Map<String, Object> request,
                                                         String connectionUrl) {
        String tableName = (String) request.get("TableName");
        Map<String, Object> ttlDesc = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            PTable pTable = connection.unwrap(PhoenixConnection.class).getTable(tableName);
            String ttlExpression = pTable.getTTLExpression().toString().trim();
            if (ttlExpression.startsWith("TO_NUMBER(CURRENT_TIME())")) {
                ttlDesc.put("TimeToLiveStatus", TimeToLiveStatus.ENABLED);
                ttlDesc.put("AttributeName", PhoenixUtils.extractAttributeFromTTLExpression(ttlExpression));
            } else {
                ttlDesc.put("TimeToLiveStatus", TimeToLiveStatus.DISABLED);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> response = new HashMap<>();
        response.put("TimeToLiveDescription", ttlDesc);
        return response;
    }
}
