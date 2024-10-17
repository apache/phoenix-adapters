package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.DQLUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetItemService {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetItemService.class);
    private static final String SELECT_QUERY = "SELECT COL FROM %s WHERE %s = ? ";
    private static final String CLAUSE_FOR_SORT_COL = "AND %s = ?";


    public static GetItemResult getItem(GetItemRequest request, String connectionUrl)  {
        String tableName = request.getTableName();
        List<PColumn> tablePKCols = null;
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            // get PKs from phoenix
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            String partitionKeyPKCol = tablePKCols.get(0).toString();
            String sortKeyPKCol = null;
            if(tablePKCols.size() > 1){
                sortKeyPKCol = tablePKCols.get(1).toString();
            }

            //build prepared statement and execute
            PreparedStatement stmt =
                    getPreparedStatement(connection, request, partitionKeyPKCol, sortKeyPKCol);
            return executeQuery(stmt, request);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection conn, GetItemRequest request,
                                                         String partitionKeyPKCol, String sortKeyPKCol)
            throws SQLException{
        String tableName = request.getTableName();
        StringBuilder queryBuilder = new StringBuilder(String.format(SELECT_QUERY, tableName,
                CommonServiceUtils.getEscapedArgument(partitionKeyPKCol)));
        if(sortKeyPKCol != null){
            queryBuilder.append(String.format(CLAUSE_FOR_SORT_COL,
                    CommonServiceUtils.getEscapedArgument(sortKeyPKCol)));
        }
        LOGGER.info("SELECT Query: " + queryBuilder);
        PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, partitionKeyPKCol, sortKeyPKCol);
        return stmt;
    }

    /**
     * Set all the values on the PreparedStatement:
     * - 1 value for partitionKey,
     * - 1 or 2 values for sortKey, if present
     */
    private static void setPreparedStatementValues(PreparedStatement stmt, GetItemRequest request,
                                                   String partitionKeyPKCol, String sortKeyPKCol)
            throws SQLException {
        DQLUtils.setKeyValueOnStatement(stmt, 1,
                request.getKey().get(partitionKeyPKCol), false);
        if (sortKeyPKCol != null) {
            DQLUtils.setKeyValueOnStatement(stmt, 2,
                    request.getKey().get(sortKeyPKCol), false);
        }
    }

    /**
     * Execute the given PreparedStatement, collect the returned item with projected attributes
     * and return GetItemResult.
     */
    private static GetItemResult executeQuery(PreparedStatement stmt, GetItemRequest request)
            throws SQLException{
        GetItemResult finalResult = new GetItemResult();
        ResultSet rs  = stmt.executeQuery();
        if(rs.next()) {
            Map<String, AttributeValue> item = BsonDocumentToDdbAttributes.getProjectedItem(
                    (RawBsonDocument) rs.getObject(1), getProjectionAttributes(request));
            return finalResult.withItem(item);
        }
        return finalResult;
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(GetItemRequest request) {
        List<String> attributesToGet = request.getAttributesToGet();
        String projExpr = request.getProjectionExpression();
        Map<String, String> exprAttrNames = request.getExpressionAttributeNames();
        return DQLUtils.getProjectionAttributes(attributesToGet, projExpr, exprAttrNames);
    }
}
