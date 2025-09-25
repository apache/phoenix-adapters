package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.ConditionCheckFailedException;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.rest.metrics.ApiOperation;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.service.utils.DMLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;

public class DeleteItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteItemService.class);

    private static final String DELETE_QUERY = "DELETE FROM %s.\"%s\" WHERE %s = ? ";
    private static final String DELETE_QUERY_WITH_SORT =
            "DELETE FROM %s.\"%s\" WHERE %s = ? AND %s = ?";
    private static final String DELETE_QUERY_NO_SORT_WITH_COND_EXPR =
            "DELETE FROM %s.\"%s\" WHERE %s = ? AND BSON_CONDITION_EXPRESSION(COL,?)";
    private static final String DELETE_QUERY_SORT_WITH_COND_EXPR =
            "DELETE FROM %s.\"%s\" WHERE %s = ? AND %s = ? AND BSON_CONDITION_EXPRESSION(COL,?)";

    public static Map<String, Object> deleteItem(Map<String, Object> request,
            String connectionUrl) {
        ValidationUtil.validateDeleteItemRequest(request);
        Map<String, Object> result;
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            result = deleteItemWithConn(connection, request);
        } catch (ConditionCheckFailedException e) {
            if (ApiMetadata.ALL_OLD.equals(
                    request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE))) {
                Map<String, Object> item = GetItemService.getItem(request, connectionUrl);
                e.setItem((Map<String, Object>)item.get(ApiMetadata.ITEM));
            }
            throw e;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static Map<String, Object> deleteItemWithConn(Connection connection,
            Map<String, Object> request) throws SQLException {
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        PTable table = phoenixConnection.getTable(new PTableKey(phoenixConnection.getTenantId(),
                "DDB." + request.get(ApiMetadata.TABLE_NAME)));
        // get PKs from phoenix
        List<PColumn> pkCols = table.getPKColumns();
        //build prepared statement and execute
        StatementInfo stmtInfo = getPreparedStatement(connection, request, pkCols);
        setValuesOnPreparedStatement(stmtInfo, pkCols, (Map<String, Object>) request.get(ApiMetadata.KEY));

        LOGGER.debug("Delete Query for DeleteItem: {}", stmtInfo.stmt);

        boolean hasCondExp = (request.get(ApiMetadata.CONDITION_EXPRESSION) != null) || (
                request.get(ApiMetadata.EXPECTED) != null);
        return DMLUtils.executeUpdate(stmtInfo.stmt,
                (String) request.get(ApiMetadata.RETURN_VALUES),
                (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                hasCondExp, pkCols, ApiOperation.DELETE_ITEM);
    }

    /**
     * Build the DELETE query based on the request parameters.
     * Return a StatementInfo with PreparedStatement and condition document.
     */
    public static StatementInfo getPreparedStatement(Connection conn,
            Map<String, Object> request, List<PColumn> pkCols) throws SQLException {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String condExpr = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        String partitionKeyPKCol = pkCols.get(0).toString();
        String sortKeyPKCol = null;
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
        
        PreparedStatement stmt;
        BsonDocument conditionDoc = null;

        if (pkCols.size() > 1) {
            sortKeyPKCol = pkCols.get(1).toString();
        }
        if (!StringUtils.isEmpty(condExpr)) {
            conditionDoc = CommonServiceUtils.getBsonConditionExpressionDoc(condExpr, exprAttrNames,
                            exprAttrVals);
            if (sortKeyPKCol != null) {
                stmt = conn.prepareStatement(
                        String.format(DELETE_QUERY_SORT_WITH_COND_EXPR, "DDB", tableName,
                                CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                                CommonServiceUtils.getEscapedArgument(sortKeyPKCol)));
            } else {
                stmt = conn.prepareStatement(
                        String.format(DELETE_QUERY_NO_SORT_WITH_COND_EXPR, "DDB", tableName,
                                CommonServiceUtils.getEscapedArgument(partitionKeyPKCol)));
            }
        } else {
            if (sortKeyPKCol != null) {
                stmt = conn.prepareStatement(String.format(DELETE_QUERY_WITH_SORT, "DDB", tableName,
                        CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                        CommonServiceUtils.getEscapedArgument(sortKeyPKCol)));
            } else {
                stmt = conn.prepareStatement(String.format(DELETE_QUERY, "DDB", tableName,
                        CommonServiceUtils.getEscapedArgument(partitionKeyPKCol)));
            }
        }
        return new StatementInfo(stmt, conditionDoc);
    }

    /**
     * Set keys and condition expression BSON document on the statement.
     */
    private static void setValuesOnPreparedStatement(StatementInfo statementInfo,
            List<PColumn> pkCols, Map<String, Object> keyMap) throws SQLException {
        
        // set keys
        DMLUtils.setKeysOnStatement(statementInfo.stmt, pkCols, keyMap);
        
        // set condition expression BSON document if present
        if (statementInfo.conditionDoc != null) {
            int paramIndex = pkCols.size() + 1;
            statementInfo.stmt.setObject(paramIndex, statementInfo.conditionDoc);
        }
    }

    /**
     * Helper class to return statement information including condition document.
     */
    private static class StatementInfo {
        final PreparedStatement stmt;
        final BsonDocument conditionDoc;

        StatementInfo(PreparedStatement stmt, BsonDocument conditionDoc) {
            this.stmt = stmt;
            this.conditionDoc = conditionDoc;
        }
    }
}
