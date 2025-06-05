package org.apache.phoenix.ddb.service;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.ddb.service.utils.ApiMetadata;
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
    private static final String ALTER_TTL_STMT = "ALTER TABLE %s.\"%s\" SET TTL = '%s'";

    public static Map<String, Object> updateTimeToLive(Map<String, Object> request,
                                                       String connectionUrl) {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        Map<String, Object> ttlSpec = (Map<String, Object>) request.get(ApiMetadata.TIME_TO_LIVE_SPECIFICATION);
        String colName = (String) ttlSpec.get(ApiMetadata.ATTRIBUTE_NAME);
        Boolean enabled = (Boolean) ttlSpec.get(ApiMetadata.TIME_TO_LIVE_ENABLED);
        String alterStmt;
        if (enabled) {
            String ttlExpression = String.format(PhoenixUtils.TTL_EXPRESSION, colName, colName);
            alterStmt = String.format(ALTER_TTL_STMT, "DDB", tableName, ttlExpression);
        } else {
            alterStmt = String.format(ALTER_TTL_STMT, "DDB", tableName, HConstants.FOREVER);
        }
        LOGGER.info("SQL for UpdateTimeToLive: " + alterStmt);
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.createStatement().execute(alterStmt);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> response = new HashMap<>();
        response.put(ApiMetadata.TIME_TO_LIVE_SPECIFICATION, ttlSpec);
        return response;
    }

    public static Map<String, Object> describeTimeToLive(Map<String, Object> request,
                                                         String connectionUrl) {
        String tableName = "DDB." + request.get(ApiMetadata.TABLE_NAME);
        Map<String, Object> ttlDesc = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            PTable pTable = connection.unwrap(PhoenixConnection.class).getTable(tableName);
            String ttlExpression = pTable.getTTLExpression().toString().trim();
            if (ttlExpression.contains("BSON_VALUE")) {
                ttlDesc.put(ApiMetadata.TIME_TO_LIVE_STATUS, TimeToLiveStatus.ENABLED);
                ttlDesc.put(ApiMetadata.ATTRIBUTE_NAME, PhoenixUtils.extractAttributeFromTTLExpression(ttlExpression));
            } else {
                ttlDesc.put(ApiMetadata.TIME_TO_LIVE_STATUS, TimeToLiveStatus.DISABLED);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> response = new HashMap<>();
        response.put(ApiMetadata.TIME_TO_LIVE_DESCRIPTION, ttlDesc);
        return response;
    }
}
