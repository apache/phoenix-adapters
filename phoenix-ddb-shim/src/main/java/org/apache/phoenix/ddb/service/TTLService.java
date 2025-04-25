package org.apache.phoenix.ddb.service;

import org.apache.hadoop.hbase.HConstants;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveDescription;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveStatus;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.regex.Pattern;

public class TTLService {

    private static final Logger LOGGER = LoggerFactory.getLogger(TTLService.class);
    private static final String TTL_EXPRESSION = "TO_NUMBER(CURRENT_TIME()) > BSON_VALUE(COL, ''%s'', ''BIGINT'')";
    private static final String ALTER_TTL_STMT = "ALTER TABLE %s SET TTL = '%s'";

    public static UpdateTimeToLiveResponse updateTimeToLive(UpdateTimeToLiveRequest request,
                                                            String connectionUrl) {
        String tableName = request.tableName();
        String colName = request.timeToLiveSpecification().attributeName();
        String alterStmt;
        if (request.timeToLiveSpecification().enabled()) {
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
        return UpdateTimeToLiveResponse.builder()
                .timeToLiveSpecification(request.timeToLiveSpecification()).build();
    }

    public static DescribeTimeToLiveResponse describeTimeToLive(DescribeTimeToLiveRequest request,
                                                                String connectionUrl) {
        String tableName = request.tableName();
        TimeToLiveDescription.Builder desc = TimeToLiveDescription.builder();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            PTable pTable = connection.unwrap(PhoenixConnection.class).getTable(tableName);
            String ttlExpression = pTable.getTTLExpression().toString().trim();
            if (ttlExpression.startsWith("TO_NUMBER(CURRENT_TIME())")) {
                desc.timeToLiveStatus(TimeToLiveStatus.ENABLED);
                desc.attributeName(PhoenixUtils.extractAttributeFromTTLExpression(ttlExpression));
            } else {
                desc.timeToLiveStatus(TimeToLiveStatus.DISABLED);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return DescribeTimeToLiveResponse.builder().timeToLiveDescription(desc.build()).build();
    }
}
