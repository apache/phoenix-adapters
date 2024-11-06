package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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
    private static final String CLAUSE_FOR_SORT_COL = "AND %s = ?";

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
        Map<String, AttributeValue> returnAttrs
                = DMLUtils.executeUpdate(stmt, request.getReturnValues(),
                request.getReturnValuesOnConditionCheckFailure(),
                request.getConditionExpression(), table, pkCols);
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
        String partitionKeyPKCol = pkCols.get(0).toString();
        StringBuilder queryBuilder = new StringBuilder(String.format(DELETE_QUERY, tableName,
                CommonServiceUtils.getEscapedArgument(partitionKeyPKCol)));
        if(pkCols.size() > 1){
            String sortKeyPKCol = pkCols.get(1).toString();
            queryBuilder.append(String.format(CLAUSE_FOR_SORT_COL,
                    CommonServiceUtils.getEscapedArgument(sortKeyPKCol)));
        }
        LOGGER.info("DELETE Query: " + queryBuilder);
        PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, pkCols);
        return stmt;
    }

    /**
     * Set all the values on the PreparedStatement:
     * - 1 value for partitionKey,
     * - 1 or 2 values for sortKey, if present
     */
    private static void setPreparedStatementValues(PreparedStatement stmt, DeleteItemRequest request,
                                                   List<PColumn> pkCols)
            throws SQLException {
        String partitionKeyPKCol = pkCols.get(0).toString();
        DQLUtils.setKeyValueOnStatement(stmt, 1,
                request.getKey().get(partitionKeyPKCol), false);
        if(pkCols.size() > 1){
            String sortKeyPKCol = pkCols.get(1).toString();
            DQLUtils.setKeyValueOnStatement(stmt, 2,
                    request.getKey().get(sortKeyPKCol), false);
        }
    }
}
