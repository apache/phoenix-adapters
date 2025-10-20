package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.ddb.rest.metrics.ApiOperation;
import org.apache.phoenix.expression.util.bson.SQLComparisonExpressionUtils;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.MapToBsonDocument;
import org.apache.phoenix.ddb.service.utils.DMLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.schema.PColumn;

public class PutItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PutItemService.class);

    private static final String PUT_WITH_HASH_KEY = "UPSERT INTO %s VALUES (?,?)";
    private static final String PUT_WITH_HASH_SORT_KEY = "UPSERT INTO %s VALUES (?,?,?)";

    private static final String CONDITIONAL_PUT_UPDATE_ONLY_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) THEN ? \n"
                    + " ELSE COL END";

    private static final String CONDITIONAL_PUT_UPDATE_ONLY_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?, ?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) THEN ? \n"
                    + " ELSE COL END";

    private static final String CONDITIONAL_PUT_UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?, ?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) THEN ? \n"
                    + " ELSE COL END";

    private static final String CONDITIONAL_PUT_UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?, ?, ?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) THEN ? \n"
                    + " ELSE COL END";

    private static final BsonDocument EMPTY_BSON_DOC = new BsonDocument();
    private static final RawBsonDocument EMPTY_RAW_BSON_DOC = RawBsonDocument.parse("{}");

    public static Map<String, Object> putItem(Map<String, Object> request, String connectionUrl) {
        Map<String, Object> result;
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            result = putItemWithConn(connection, request);
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
        return result;
    }

    public static Map<String, Object> putItemWithConn(Connection connection,
            Map<String, Object> request) throws SQLException {
        ValidationUtil.validatePutItemRequest(request);
        Map<String, Object> item = (Map<String, Object>) request.get(ApiMetadata.ITEM);

        // get PK PColumns
        List<PColumn> pkCols = PhoenixUtils.getPKColumns(connection, (String)request.get(ApiMetadata.TABLE_NAME));

        //create statement based on PKs and conditional expression
        StatementInfo stmtInfo = getPreparedStatement(connection, request, pkCols);
        setValuesOnPreparedStatement(stmtInfo, pkCols, item);

        //execute, auto commit is on
        LOGGER.debug("Upsert Query for PutItem: {}", stmtInfo.stmt);

        boolean hasCondExp = (request.get(ApiMetadata.CONDITION_EXPRESSION) != null) || (
                request.get(ApiMetadata.EXPECTED) != null);
        return DMLUtils.executeUpdate(stmtInfo.stmt,
                (String) request.get(ApiMetadata.RETURN_VALUES),
                (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                hasCondExp, pkCols, ApiOperation.PUT_ITEM);
    }

    private static StatementInfo getPreparedStatement(Connection conn, Map<String, Object> request,
            List<PColumn> pkCols) throws SQLException {
        PreparedStatement stmt;
        boolean setItemTwice = false;
        BsonDocument conditionDoc = null;
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String condExpr = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);

        // Handle legacy Expected parameter conversion to ConditionExpression
        Map<String, Object> expected = (Map<String, Object>) request.get(ApiMetadata.EXPECTED);
        String conditionalOperator = (String) request.get(ApiMetadata.CONDITIONAL_OPERATOR);
        if (condExpr == null && expected != null) {
            // Initialize maps if they don't exist (for Expected conversion)
            if (exprAttrNames == null) {
                exprAttrNames = new HashMap<>();
            }
            if (exprAttrVals == null) {
                exprAttrVals = new HashMap<>();
            }
            // Convert Expected to ConditionExpression
            condExpr = CommonServiceUtils.convertExpectedToConditionExpression(expected,
                    conditionalOperator, exprAttrNames, exprAttrVals);
        }

        String queryFormat;
        if (!StringUtils.isEmpty(condExpr)) {
            conditionDoc =
                    CommonServiceUtils.getBsonConditionExpressionDoc(condExpr, exprAttrNames,
                            exprAttrVals);
            BsonDocument exprAttrNamesDoc =
                    CommonServiceUtils.getExpressionAttributeNamesDoc(exprAttrNames);

            if (shouldUseUpdateForAtomicPut(condExpr, exprAttrNamesDoc)) {
                queryFormat = (pkCols.size() == 1) ?
                        CONDITIONAL_PUT_UPDATE_WITH_HASH_KEY :
                        CONDITIONAL_PUT_UPDATE_WITH_HASH_SORT_KEY;
                setItemTwice = true;
            } else {
                queryFormat = (pkCols.size() == 1) ?
                        CONDITIONAL_PUT_UPDATE_ONLY_WITH_HASH_KEY :
                        CONDITIONAL_PUT_UPDATE_ONLY_WITH_HASH_SORT_KEY;
            }
        } else {
            queryFormat = (pkCols.size() == 1) ? PUT_WITH_HASH_KEY : PUT_WITH_HASH_SORT_KEY;
        }
        stmt = conn.prepareStatement(String.format(queryFormat, PhoenixUtils.getFullTableName(tableName, true)));
        return new StatementInfo(setItemTwice, stmt, conditionDoc);
    }

    /**
     * Use UPDATE if condition expression can evaluate to true on an empty document
     * (which is equivalent to item not already existing) because the insert should happen in that case.
     * If it evaluates to false, we will use UPDATE_ONLY.
     */
    private static boolean shouldUseUpdateForAtomicPut(String condExpr,
            BsonDocument exprAttrNamesDoc) {
        return SQLComparisonExpressionUtils.evaluateConditionExpression(condExpr,
                EMPTY_RAW_BSON_DOC, EMPTY_BSON_DOC, exprAttrNamesDoc);
    }

    /**
     * Set keys, condition expression BSON document and the item on the statement.
     */
    private static void setValuesOnPreparedStatement(StatementInfo statementInfo,
            List<PColumn> pkCols, Map<String, Object> item) throws SQLException {

        // set keys
        DMLUtils.setKeysOnStatement(statementInfo.stmt, pkCols, item);
        int paramIndex = pkCols.size() + 1;

        BsonDocument bsonDoc = MapToBsonDocument.getBsonDocument(item);
        if (statementInfo.setItemTwice) {
            // this is done for UPDATE where we provide full row in VALUES(), not for UPDATE_ONLY
            statementInfo.stmt.setObject(paramIndex++, bsonDoc);
        }

        // set condition expression BSON document
        if (statementInfo.conditionDoc != null) {
            statementInfo.stmt.setObject(paramIndex++, statementInfo.conditionDoc);
        }

        // set bson document of entire item for THEN clause
        statementInfo.stmt.setObject(paramIndex, bsonDoc);

    }

    /**
     * Helper class to return statement information including setItemTwice flag and condition document.
     */
    private static class StatementInfo {
        final boolean setItemTwice;
        final PreparedStatement stmt;
        final BsonDocument conditionDoc;

        StatementInfo(boolean setItemTwice, PreparedStatement stmt, BsonDocument conditionDoc) {
            this.setItemTwice = setItemTwice;
            this.stmt = stmt;
            this.conditionDoc = conditionDoc;
        }
    }
}
