package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.MapToBsonDocument;
import org.apache.phoenix.ddb.service.utils.DMLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;

public class PutItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PutItemService.class);

    private static final String PUT_WITH_HASH_KEY = "UPSERT INTO %s VALUES (?,?)";
    private static final String PUT_WITH_HASH_SORT_KEY = "UPSERT INTO %s VALUES (?,?,?)";

    private static final String CONDITIONAL_PUT_WITH_HASH_KEY = "UPSERT INTO %s VALUES (?) " +
            " ON DUPLICATE KEY UPDATE\n" +
            " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') THEN ? \n" +
            " ELSE COL END";

    private static final String CONDITIONAL_PUT_WITH_HASH_SORT_KEY
            = "UPSERT INTO %s VALUES (?, ?) " + " ON DUPLICATE KEY UPDATE\n" +
            " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') THEN ? \n" +
            " ELSE COL END";

    public static Map<String, Object> putItem(Map<String, Object> request, String connectionUrl) {
        Map<String, Object> result;
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            result = putItemWithConn(connection, request);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static Map<String, Object> putItemWithConn(Connection connection,
            Map<String, Object> request) throws SQLException {
        Map<String, Object> item = (Map<String, Object>) request.get("Item");
        BsonDocument bsonDoc = MapToBsonDocument.getBsonDocument(item);
        // get PTable and PK PColumns
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        PTable table = phoenixConnection.getTable(
                new PTableKey(phoenixConnection.getTenantId(), (String) request.get("TableName")));
        List<PColumn> pkCols = table.getPKColumns();

        //create statement based on PKs and conditional expression
        PreparedStatement stmt = getPreparedStatement(connection, request, pkCols.size());
        // extract PKs from item
        DMLUtils.setKeysOnStatement(stmt, pkCols, item);
        // set bson document of entire item
        stmt.setObject(pkCols.size()+1, bsonDoc);

        //execute, auto commit is on
        LOGGER.info("Upsert Query for PutItem: {}", stmt);
        return DMLUtils.executeUpdate(stmt, (String) request.get("ReturnValues"),
                (String) request.get("ReturnValuesOnConditionCheckFailure"),
                (String) request.get("ConditionExpression"), pkCols, false);
    }

    /**
     * Return the corresponding PreparedStatement based on number of
     * Primary Key columns and conditional expression.
     */
    private static PreparedStatement getPreparedStatement(Connection conn,
            Map<String, Object> request, int numPKs) throws SQLException {
        PreparedStatement stmt;
        String tableName = (String) request.get("TableName");
        String condExpr = (String) request.get("ConditionExpression");
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get("ExpressionAttributeNames");
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get("ExpressionAttributeValues");
        if (!StringUtils.isEmpty(condExpr)) {
            String bsonCondExpr =
                    CommonServiceUtils.getBsonConditionExpressionFromMap(condExpr, exprAttrNames,
                            exprAttrVals);
            String QUERY_FORMAT = (numPKs == 1)
                    ? CONDITIONAL_PUT_WITH_HASH_KEY : CONDITIONAL_PUT_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(
                    String.format(QUERY_FORMAT, tableName, bsonCondExpr));
        } else {
            String QUERY_FORMAT = (numPKs == 1)
                    ? PUT_WITH_HASH_KEY : PUT_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(String.format(QUERY_FORMAT, tableName));
        }
        return stmt;
    }
}
