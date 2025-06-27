package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
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

    private static final String PUT_WITH_HASH_KEY = "UPSERT INTO %s.\"%s\" VALUES (?,?)";
    private static final String PUT_WITH_HASH_SORT_KEY = "UPSERT INTO %s.\"%s\" VALUES (?,?,?)";

    private static final String CONDITIONAL_PUT_WITH_HASH_KEY = "UPSERT INTO %s.\"%s\" VALUES (?) " +
            " ON DUPLICATE KEY UPDATE_ONLY\n" +
            " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') THEN ? \n" +
            " ELSE COL END";

    private static final String CONDITIONAL_PUT_WITH_HASH_SORT_KEY
            = "UPSERT INTO %s.\"%s\" VALUES (?, ?) " + " ON DUPLICATE KEY UPDATE_ONLY\n" +
            " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') THEN ? \n" +
            " ELSE COL END";

    private static final String CONDITIONAL_PUT_IGNORE_WITH_HASH_KEY
            = "UPSERT INTO %s.\"%s\" VALUES (?, ?) ON DUPLICATE KEY IGNORE ";

    private static final String CONDITIONAL_PUT_IGNORE_WITH_HASH_SORT_KEY
            = "UPSERT INTO %s.\"%s\" VALUES (?, ?, ?) ON DUPLICATE KEY IGNORE ";

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
        ValidationUtil.validatePutItemRequest(request);
        Map<String, Object> item = (Map<String, Object>) request.get(ApiMetadata.ITEM);
        BsonDocument bsonDoc = MapToBsonDocument.getBsonDocument(item);
        // get PTable and PK PColumns
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        PTable table = phoenixConnection.getTable(new PTableKey(phoenixConnection.getTenantId(),
                "DDB." + request.get(ApiMetadata.TABLE_NAME)));
        List<PColumn> pkCols = table.getPKColumns();

        //create statement based on PKs and conditional expression
        PreparedStatement stmt = getPreparedStatement(connection, request, pkCols);
        // extract PKs from item
        DMLUtils.setKeysOnStatement(stmt, pkCols, item);
        // set bson document of entire item
        stmt.setObject(pkCols.size()+1, bsonDoc);

        //execute, auto commit is on
        LOGGER.info("Upsert Query for PutItem: {}", stmt);
        return DMLUtils.executeUpdate(stmt, (String) request.get(ApiMetadata.RETURN_VALUES),
                (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                (String) request.get(ApiMetadata.CONDITION_EXPRESSION), pkCols, false);
    }

    /**
     * Return the corresponding PreparedStatement based on number of
     * Primary Key columns and conditional expression.
     */
    private static PreparedStatement getPreparedStatement(Connection conn,
            Map<String, Object> request, List<PColumn> pkCols) throws SQLException {
        PreparedStatement stmt;
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String condExpr = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);
        if (!StringUtils.isEmpty(condExpr)) {
            String bsonCondExpr =
                    CommonServiceUtils.getBsonConditionExpressionFromMap(condExpr, exprAttrNames,
                            exprAttrVals);
            String replacedAliasCondExpr =
                    CommonServiceUtils.replaceExpressionAttributeNames(condExpr, exprAttrNames);
            // we do not want upsert to be performed if row already exists
            if (shouldUseIgnoreForAtomicUpsert(replacedAliasCondExpr, pkCols)) {
                String QUERY_FORMAT = (pkCols.size() == 1)
                        ? CONDITIONAL_PUT_IGNORE_WITH_HASH_KEY : CONDITIONAL_PUT_IGNORE_WITH_HASH_SORT_KEY;
                stmt = conn.prepareStatement(String.format(QUERY_FORMAT, "DDB", tableName));
            } else {
                String QUERY_FORMAT = (pkCols.size() == 1)
                        ? CONDITIONAL_PUT_WITH_HASH_KEY : CONDITIONAL_PUT_WITH_HASH_SORT_KEY;
                stmt = conn.prepareStatement(
                        String.format(QUERY_FORMAT, "DDB", tableName, bsonCondExpr));
            }
        } else {
            String QUERY_FORMAT = (pkCols.size() == 1)
                    ? PUT_WITH_HASH_KEY : PUT_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(String.format(QUERY_FORMAT, "DDB", tableName));
        }
        return stmt;
    }

    private static boolean shouldUseIgnoreForAtomicUpsert(String condExpr, List<PColumn> pkCols) {
            return condExpr.contains("attribute_not_exists(" + pkCols.get(0).getName().toString() + ")")
                     || (pkCols.size() == 2 && condExpr.contains("attribute_not_exists(" + pkCols.get(1).getName().toString() + ")"));
    }
}
