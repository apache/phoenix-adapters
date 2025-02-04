package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.DMLUtils;
import org.apache.phoenix.ddb.utils.DQLUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DeleteItemService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteItemService.class);
    private static final String DELETE_QUERY = "DELETE FROM %s WHERE %s = ? ";
    private static final String DELETE_QUERY_WITH_SORT = "DELETE FROM %s WHERE %s = ? AND %s = ?";
    private static final String DELETE_QUERY_NO_SORT_WITH_COND_EXPR =
            "DELETE FROM %s WHERE %s = ? AND BSON_CONDITION_EXPRESSION(COL,'%s')";
    private static final String DELETE_QUERY_SORT_WITH_COND_EXPR =
            "DELETE FROM %s WHERE %s = ? AND %s = ? AND BSON_CONDITION_EXPRESSION(COL,'%s')";


    public static DeleteItemResult deleteItem(DeleteItemRequest request, String connectionUrl)  {
        DeleteItemResult result;
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            result = deleteItemWithConn(connection, request);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    public static DeleteItemResult deleteItemWithConn(Connection connection, DeleteItemRequest request)
            throws SQLException {
        DeleteItemResult result = new DeleteItemResult();
        PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
        PTable table = phoenixConnection.getTable(
                new PTableKey(phoenixConnection.getTenantId(), request.getTableName()));
        // get PKs from phoenix
        List<PColumn> pkCols = table.getPKColumns();
        //build prepared statement and execute
        PreparedStatement stmt =
                getPreparedStatement(connection, request, pkCols);

        DMLUtils.setKeysOnStatement(stmt, pkCols, request.getKey());
        LOGGER.info("Delete Query for DeleteItem: {}", stmt);
        Map<String, AttributeValue> returnAttrs
                = DMLUtils.executeUpdate(stmt, request.getReturnValues(),
                request.getReturnValuesOnConditionCheckFailure(),
                request.getConditionExpression(), pkCols, true);
        result.setAttributes(returnAttrs);
        return result;
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection conn, DeleteItemRequest request,
                                                         List<PColumn> pkCols)
            throws SQLException{
        String tableName = request.getTableName();
        String condExpr = request.getConditionExpression();
        String partitionKeyPKCol = pkCols.get(0).toString();
        String sortKeyPKCol = null;
        Map<String, String> exprAttrNames = request.getExpressionAttributeNames();
        Map<String, AttributeValue> exprAttrVals = request.getExpressionAttributeValues();
        PreparedStatement stmt;

        if (pkCols.size() > 1) {
            sortKeyPKCol = pkCols.get(1).toString();
        }
        if (!StringUtils.isEmpty(condExpr)) {
            String bsonCondExpr = CommonServiceUtils
                    .getBsonConditionExpression(condExpr, exprAttrNames, exprAttrVals);
            if (sortKeyPKCol != null) {
                stmt = conn.prepareStatement(String.format(DELETE_QUERY_SORT_WITH_COND_EXPR,
                        tableName, CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                        CommonServiceUtils.getEscapedArgument(sortKeyPKCol), bsonCondExpr));
            } else {
                stmt = conn.prepareStatement(String.format(DELETE_QUERY_NO_SORT_WITH_COND_EXPR,
                        tableName, CommonServiceUtils.getEscapedArgument(partitionKeyPKCol), bsonCondExpr));
            }
        } else {
            if (sortKeyPKCol != null) {
                stmt = conn.prepareStatement(String.format(DELETE_QUERY_WITH_SORT,
                        tableName, CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                        CommonServiceUtils.getEscapedArgument(sortKeyPKCol)));
            } else {
                stmt = conn.prepareStatement(String.format(DELETE_QUERY,
                        tableName, CommonServiceUtils.getEscapedArgument(partitionKeyPKCol)));
            }
        }
        return stmt;
    }
}
