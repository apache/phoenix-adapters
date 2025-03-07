package org.apache.phoenix.ddb.service;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
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

    public static BatchGetItemResponse batchGetItem(BatchGetItemRequest request, String connectionUrl) {

        List<PColumn> tablePKCols;
        Map<String, List<Map<String, AttributeValue>>> responses = new HashMap<>();
        BatchGetItemResponse.Builder finalResult = BatchGetItemResponse.builder();
        Map<String, KeysAndAttributes> unprocessed = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            //iterates over each table and executes SQL for all items to query per table
            for (String tableName : request.requestItems().keySet()) {
                tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);

                //map which contains all keys to get extract values of
                KeysAndAttributes keysAndAttributes = request.requestItems().get(tableName);
                int numKeysToQuery = keysAndAttributes.keys().size();
                //creating PreparedStatement and setting values on it
                PreparedStatement stmt =
                        getPreparedStatement(connection, tableName, tablePKCols,
                                Integer.min(numKeysToQuery,QUERY_LIMIT));
                setPreparedStatementValues(stmt, tablePKCols, keysAndAttributes,
                        Integer.min(numKeysToQuery,QUERY_LIMIT));
                //executing the sql query to get response
                List<Map<String, AttributeValue>> items = executeQueryAndGetResponses(stmt, keysAndAttributes);
                responses.put(tableName, items);

                //putting unexecuted keys for unprocessed key set
                if(numKeysToQuery > QUERY_LIMIT){
                    List<Map<String, AttributeValue>> unprocessedKeyList =
                            keysAndAttributes.keys().subList(QUERY_LIMIT, keysAndAttributes.keys().size());
                    KeysAndAttributes unprocessedKeysAndAttributes = keysAndAttributes.toBuilder().keys(unprocessedKeyList).build();
                    unprocessed.put(tableName, unprocessedKeysAndAttributes);
                }

            }
            finalResult.responses(responses).unprocessedKeys(unprocessed);
            return finalResult.build();
        } catch (SQLException e){
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection connection, String tableName,
                                                         List<PColumn> tablePKCols, int numKeysToQuery)
            throws SQLException{

        StringBuilder queryBuilder;
        String partitionKeyPKCol = tablePKCols.get(0).toString();

        if(tablePKCols.size() > 1){
            String sortKeyPKCol = tablePKCols.get(1).toString();
            queryBuilder = new StringBuilder(String.format(SELECT_QUERY_WITH_SORT_COL, tableName,
                    CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                    CommonServiceUtils.getEscapedArgument(sortKeyPKCol),
                    buildSQLQueryClause(numKeysToQuery, true)));
        }
        else{
            queryBuilder = new StringBuilder(String.format(SELECT_QUERY_WITH_ONLY_PARTITION_COL, tableName,
                    CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                    buildSQLQueryClause(numKeysToQuery, false)));

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
                                                   List<PColumn> tablePKCols,
                                                   KeysAndAttributes requestItemMap, int numKeysToQuery)
            throws SQLException {
        int index = 1;
        String partitionKeyPKCol = tablePKCols.get(0).toString();
        //iterates over the request map to get all the keys to query
        for(int j = 0; j < numKeysToQuery; j++){
            AttributeValue valueForPartitionCol = requestItemMap.keys().get(j).get(partitionKeyPKCol);
            DQLUtils.setKeyValueOnStatement(stmt, index++, valueForPartitionCol, false);
            if(tablePKCols.size() > 1){
                String sortKeyPKCol = tablePKCols.get(1).toString();
                AttributeValue valueForSortCol = requestItemMap.keys().get(j).get(sortKeyPKCol);
                DQLUtils.setKeyValueOnStatement(stmt, index++, valueForSortCol, false);
            }
        }

    }

    /**
     * Execute the given PreparedStatement, collect the returned item with projected attributes
     * and return GetItemResponse.
     */
    private static List<Map<String, AttributeValue>> executeQueryAndGetResponses(PreparedStatement stmt,
                                                                                 KeysAndAttributes requestItemMap)
            throws SQLException{
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        ResultSet rs  = stmt.executeQuery();
        while (rs.next()) {
            Map<String, AttributeValue> item = BsonDocumentToDdbAttributes.getProjectedItem(
                    (RawBsonDocument) rs.getObject(1), getProjectionAttributes(requestItemMap));
            items.add(item);
        }
        return items;
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(KeysAndAttributes requestItemMap) {
        List<String> attributesToGet = requestItemMap.attributesToGet();
        String projExpr = requestItemMap.projectionExpression();
        Map<String, String> exprAttrNames = requestItemMap.expressionAttributeNames();
        return DQLUtils.getProjectionAttributes(attributesToGet, projExpr, exprAttrNames);
    }

}
