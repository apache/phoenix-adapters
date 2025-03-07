package org.apache.phoenix.ddb.service;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.DMLUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class UpdateItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemService.class);

    private static final String UPDATE_WITH_HASH_KEY = "UPSERT INTO %s VALUES (?) " +
            " ON DUPLICATE KEY UPDATE\n" +
            " COL = BSON_UPDATE_EXPRESSION(COL,'%s')";

    private static final String UPDATE_WITH_HASH_SORT_KEY = "UPSERT INTO %s VALUES (?,?) " +
            " ON DUPLICATE KEY UPDATE\n" +
            " COL = BSON_UPDATE_EXPRESSION(COL,'%s')";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_KEY = "UPSERT INTO %s VALUES (?) " +
            " ON DUPLICATE KEY UPDATE\n" +
            " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') " +
            " THEN BSON_UPDATE_EXPRESSION(COL,'%s') \n" +
            " ELSE COL END";

    private static final String
            CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY = "UPSERT INTO %s VALUES (?,?) " +
            " ON DUPLICATE KEY UPDATE\n" +
            " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') " +
            " THEN BSON_UPDATE_EXPRESSION(COL,'%s') \n" +
            " ELSE COL END";

    public static UpdateItemResponse updateItem(UpdateItemRequest request, String connectionUrl) {
        UpdateItemResponse.Builder result = UpdateItemResponse.builder();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            // get PTable and PK PColumns
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTable(
                    new PTableKey(phoenixConnection.getTenantId(), request.tableName()));
            List<PColumn> pkCols = table.getPKColumns();

            //create statement based on PKs and conditional expression
            PreparedStatement stmt = getPreparedStatement(connection, request, pkCols.size());

            // extract PKs from item
            DMLUtils.setKeysOnStatement(stmt, pkCols, request.key());

            //execute, auto commit is on
            LOGGER.info("Upsert Query for UpdateItem: {}", stmt);
            Map<String, AttributeValue> returnAttrs
                    = DMLUtils.executeUpdate(stmt, request.returnValues(),
                    request.returnValuesOnConditionCheckFailure(),
                    request.conditionExpression(), pkCols, false);
            result.attributes(returnAttrs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result.build();
    }

    private static PreparedStatement getPreparedStatement(Connection conn,
                                                          UpdateItemRequest request,
                                                          int numPKs)
            throws SQLException {
        PreparedStatement stmt;
        String tableName = request.tableName();
        String condExpr = request.conditionExpression();
        Map<String, String> exprAttrNames =  request.expressionAttributeNames();
        Map<String, AttributeValue> exprAttrVals =  request.expressionAttributeValues();
        BsonDocument updateExpr = CommonServiceUtils
                .getBsonUpdateExpression(request.updateExpression(), exprAttrNames, exprAttrVals);
        if (!StringUtils.isEmpty(condExpr)) {
            String bsonCondExpr = CommonServiceUtils
                    .getBsonConditionExpression(condExpr, exprAttrNames, exprAttrVals);
            String QUERY_FORMAT = (numPKs == 1)
                    ? CONDITIONAL_UPDATE_WITH_HASH_KEY : CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(
                    String.format(QUERY_FORMAT, tableName, bsonCondExpr, updateExpr));
        } else {
            String QUERY_FORMAT = (numPKs == 1)
                    ? UPDATE_WITH_HASH_KEY : UPDATE_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(String.format(QUERY_FORMAT, tableName, updateExpr));
        }
        return stmt;
    }
}
