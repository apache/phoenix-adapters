package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.service.utils.ApiMetadata;
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
            "DELETE FROM %s.\"%s\" WHERE %s = ? AND BSON_CONDITION_EXPRESSION(COL,'%s')";
    private static final String DELETE_QUERY_SORT_WITH_COND_EXPR =
            "DELETE FROM %s.\"%s\" WHERE %s = ? AND %s = ? AND BSON_CONDITION_EXPRESSION(COL,'%s')";

    public static Map<String, Object> deleteItem(Map<String, Object> request,
            String connectionUrl) {
        Map<String, Object> result;
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            result = deleteItemWithConn(connection, request);
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
        PreparedStatement stmt = getPreparedStatement(connection, request, pkCols);

        DMLUtils.setKeysOnStatement(stmt, pkCols, (Map<String, Object>) request.get(ApiMetadata.KEY));
        LOGGER.info("Delete Query for DeleteItem: {}", stmt);
        return DMLUtils.executeUpdate(stmt, (String) request.get(ApiMetadata.RETURN_VALUES),
                (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                (String) request.get(ApiMetadata.CONDITION_EXPRESSION), pkCols, true);
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection conn,
            Map<String, Object> request, List<PColumn> pkCols) throws SQLException {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String condExpr = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        String partitionKeyPKCol = pkCols.get(0).toString();
        String sortKeyPKCol = null;
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);
        PreparedStatement stmt;

        if (pkCols.size() > 1) {
            sortKeyPKCol = pkCols.get(1).toString();
        }
        if (!StringUtils.isEmpty(condExpr)) {
            String bsonCondExpr =
                    CommonServiceUtils.getBsonConditionExpressionFromMap(condExpr, exprAttrNames,
                            exprAttrVals);
            if (sortKeyPKCol != null) {
                stmt = conn.prepareStatement(
                        String.format(DELETE_QUERY_SORT_WITH_COND_EXPR, "DDB", tableName,
                                CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                                CommonServiceUtils.getEscapedArgument(sortKeyPKCol), bsonCondExpr));
            } else {
                stmt = conn.prepareStatement(
                        String.format(DELETE_QUERY_NO_SORT_WITH_COND_EXPR, "DDB", tableName,
                                CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                                bsonCondExpr));
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
        return stmt;
    }
}
