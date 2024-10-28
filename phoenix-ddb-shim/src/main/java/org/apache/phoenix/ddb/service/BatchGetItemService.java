package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
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
import java.util.*;

public class BatchGetItemService {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchGetItemService.class);
    private static final String SELECT_QUERY_WITH_SORT_COL = "SELECT COL FROM %s WHERE (%s,%s) IN (%s)";
    private static final String SELECT_QUERY_WITH_ONLY_PARTITION_COL = "SELECT COL FROM %s WHERE (%s) IN (%s)";
    private static final String PARAMETER_CLAUSE_IF_ONLY_PARTITION_COL = "(?)";
    private static final String PARAMETER_CLAUSE_IF_BOTH_COLS = "(?,?)";
    private static final String COMMA = ",";
    private static final int QUERY_LIMIT = 100;

    public static BatchGetItemResult batchGetItem(BatchGetItemRequest request, String connectionUrl) {

        int numTablesToQuery = request.getRequestItems().keySet().size();
        List<PColumn> tablePKCols = null;
        BatchGetItemResult finalResult = new BatchGetItemResult();
        List<String> fullyProcessedTables = new ArrayList<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            //iterates over each table and executes SQL for all items to query per table
            for (String tableName : request.getRequestItems().keySet()) {
                tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
                String partitionKeyPKCol = tablePKCols.get(0).toString();
                String sortKeyPKCol = null;
                if(tablePKCols.size() > 1){
                    sortKeyPKCol = tablePKCols.get(1).toString();
                }

                //map which contains all keys to get extract values of
                KeysAndAttributes requestItemMap = request.getRequestItems().get(tableName);
                int numKeysToQuery = requestItemMap.getKeys().size();
                //creating PreparedStatement and setting values on it
                PreparedStatement stmt =
                        getPreparedStatement(connection, tableName, partitionKeyPKCol, sortKeyPKCol,
                                Integer.min(numKeysToQuery,QUERY_LIMIT));
                setPreparedStatementValues(stmt, partitionKeyPKCol, sortKeyPKCol, requestItemMap,
                        Integer.min(numKeysToQuery,QUERY_LIMIT));
                //executing the sql query to get response
                executeQueryAndPopulateResponses(stmt, requestItemMap, finalResult, tableName);

                //putting unexecuted keys for unprocessed key set
                if(numKeysToQuery > QUERY_LIMIT){
                    List<Map<String, AttributeValue>> unprocessedKeyList =
                            requestItemMap.getKeys().subList(QUERY_LIMIT, requestItemMap.getKeys().size());
                    requestItemMap.setKeys(unprocessedKeyList);
                } else{
                    fullyProcessedTables.add(tableName);
                }

            }

            //removing fully processed tables from the unprocessed key set
            for(String tableName : fullyProcessedTables){
                request.getRequestItems().remove(tableName);
            }
            finalResult.setUnprocessedKeys(request.getRequestItems());
            return finalResult;
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection connection, String tableName,
                                                         String partitionKeyPKCol, String sortKeyPKCol,
                                                         int numKeysToQuery)
            throws SQLException{

        StringBuilder queryBuilder;
        Boolean isSortKeyPresent = false;
        if(sortKeyPKCol != null){
            isSortKeyPresent = true;
            queryBuilder = new StringBuilder(String.format(SELECT_QUERY_WITH_SORT_COL, tableName,
                    CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                    CommonServiceUtils.getEscapedArgument(sortKeyPKCol),
                    buildSQLQueryClause(numKeysToQuery,isSortKeyPresent)));
        }
        else{
            queryBuilder = new StringBuilder(String.format(SELECT_QUERY_WITH_ONLY_PARTITION_COL, tableName,
                    CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                    buildSQLQueryClause(numKeysToQuery,isSortKeyPresent)));

        }
        LOGGER.info("SELECT Query: " + queryBuilder );
        PreparedStatement stmt = connection.prepareStatement(queryBuilder.toString());
        return stmt;
    }

    /**
     * Place ? in the SQL at the right places
     * @param numKeysToQuery
     * @param isSortKeyPresent
     * @return SQL Query
     */
    private static String buildSQLQueryClause(int numKeysToQuery, Boolean isSortKeyPresent){
        StringBuilder temp = new StringBuilder();
        for(int k = 0; k < numKeysToQuery; k++){
            if(isSortKeyPresent) {
                temp.append(PARAMETER_CLAUSE_IF_BOTH_COLS);
            }
            else{
                temp.append(PARAMETER_CLAUSE_IF_ONLY_PARTITION_COL);
            }
            if(k != numKeysToQuery-1){
                temp.append(COMMA);
            }
        }
        return temp.toString();
    }

    /**
     * Set all the values on the PreparedStatement:
     * - 1 value for partitionKey,
     * - 1 or 2 values for sortKey, if present
     */
    private static void setPreparedStatementValues(PreparedStatement stmt,
                                                   String partitionKeyPKCol, String sortKeyPKCol,
                                                   KeysAndAttributes requestItemMap, int numKeysToQuery)
            throws SQLException {
        int index = 1;
        //iterates over the request map to get all the keys to query
        for(int j = 0; j < numKeysToQuery; j++){
            AttributeValue valueForPartitionCol = requestItemMap.getKeys().get(j).get(partitionKeyPKCol);
            DQLUtils.setKeyValueOnStatement(stmt, index++, valueForPartitionCol, false);
            if(sortKeyPKCol != null){
                AttributeValue valueForSortCol = requestItemMap.getKeys().get(j).get(sortKeyPKCol);
                DQLUtils.setKeyValueOnStatement(stmt, index++, valueForSortCol, false);
            }
        }

    }

    /**
     * Execute the given PreparedStatement, collect the returned item with projected attributes
     * and return GetItemResult.
     */
    private static void executeQueryAndPopulateResponses(PreparedStatement stmt, KeysAndAttributes requestItemMap,
                                                   BatchGetItemResult finalResult,String tableName)
            throws SQLException{
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        ResultSet rs  = stmt.executeQuery();
        while (rs.next()) {
            Map<String, AttributeValue> item = BsonDocumentToDdbAttributes.getProjectedItem(
                    (RawBsonDocument) rs.getObject(1), getProjectionAttributes(requestItemMap));
            items.add(item);
        }
        finalResult.addResponsesEntry(tableName, items);
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(KeysAndAttributes requestItemMap) {
        List<String> attributesToGet = requestItemMap.getAttributesToGet();
        String projExpr = requestItemMap.getProjectionExpression();
        Map<String, String> exprAttrNames = requestItemMap.getExpressionAttributeNames();
        return DQLUtils.getProjectionAttributes(attributesToGet, projExpr, exprAttrNames);
    }

}
