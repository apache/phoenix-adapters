package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.service.utils.ApiMetadata;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.service.utils.DQLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;

public class BatchGetItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchGetItemService.class);

    private static final String SELECT_QUERY_WITH_SORT_COL =
            "SELECT COL FROM %s.\"%s\" WHERE (%s,%s) IN (%s)";
    private static final String SELECT_QUERY_WITH_ONLY_PARTITION_COL =
            "SELECT COL FROM %s.\"%s\" WHERE (%s) IN (%s)";
    private static final String PARAMETER_CLAUSE_IF_ONLY_PARTITION_COL = "(?)";
    private static final String PARAMETER_CLAUSE_IF_BOTH_COLS = "(?,?)";
    private static final String COMMA = ",";
    private static final int QUERY_LIMIT = 100;

    public static Map<String, Object> batchGetItem(Map<String, Object> request,
            String connectionUrl) {

        List<PColumn> tablePKCols;
        Map<String, Object> finalResult = new HashMap<>();
        Map<String, List<Map<String, Object>>> responses = new HashMap<>();
        Map<String, Map<String, Object>> unprocessed = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            //iterates over each table and executes SQL for all items to query per table
            for (Map.Entry<String, Object> tableEntry : ((Map<String, Object>) request.get(
                    ApiMetadata.REQUEST_ITEMS)).entrySet()) {
                tablePKCols = PhoenixUtils.getPKColumns(connection, tableEntry.getKey());

                //map which contains all keys to get extract values of
                Map<String, Object> keysAndAttributes = (Map<String, Object>) tableEntry.getValue();
                int numKeysToQuery = ((List<Object>) keysAndAttributes.get(ApiMetadata.KEYS)).size();
                //creating PreparedStatement and setting values on it
                PreparedStatement stmt =
                        getPreparedStatement(connection, tableEntry.getKey(), tablePKCols,
                                Integer.min(numKeysToQuery, QUERY_LIMIT));
                setPreparedStatementValues(stmt, tablePKCols, keysAndAttributes,
                        Integer.min(numKeysToQuery, QUERY_LIMIT));
                //executing the sql query to get response
                List<Map<String, Object>> items =
                        executeQueryAndGetResponses(stmt, keysAndAttributes);
                responses.put(tableEntry.getKey(), items);

                //putting unexecuted keys for unprocessed key set
                if (numKeysToQuery > QUERY_LIMIT) {
                    List<Map<String, Object>> keysAndAttributesList =
                            ((List<Map<String, Object>>) keysAndAttributes.get(ApiMetadata.KEYS));
                    List<Map<String, Object>> unprocessedKeyList =
                            keysAndAttributesList.subList(QUERY_LIMIT,
                                    keysAndAttributesList.size());
                    Map<String, Object> unprocessedKeyMap = new HashMap<>();
                    if (keysAndAttributes.containsKey(ApiMetadata.ATTRIBUTES_TO_GET)) {
                        unprocessedKeyMap.put(ApiMetadata.ATTRIBUTES_TO_GET,
                                keysAndAttributes.get(ApiMetadata.ATTRIBUTES_TO_GET));
                    }
                    if (keysAndAttributes.containsKey(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES)) {
                        unprocessedKeyMap.put(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES,
                                keysAndAttributes.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES));
                    }
                    unprocessedKeyMap.put(ApiMetadata.KEYS, unprocessedKeyList);
                    unprocessed.put(tableEntry.getKey(), unprocessedKeyMap);
                }
            }
            finalResult.put(ApiMetadata.RESPONSES, responses);
            finalResult.put(ApiMetadata.UNPROCESSED_KEYS, unprocessed);
            return finalResult;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection connection, String tableName,
            List<PColumn> tablePKCols, int numKeysToQuery) throws SQLException {

        StringBuilder queryBuilder;
        String partitionKeyPKCol = tablePKCols.get(0).toString();

        if (tablePKCols.size() > 1) {
            String sortKeyPKCol = tablePKCols.get(1).toString();
            queryBuilder = new StringBuilder(
                    String.format(SELECT_QUERY_WITH_SORT_COL, "DDB", tableName,
                            CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                            CommonServiceUtils.getEscapedArgument(sortKeyPKCol),
                            buildSQLQueryClause(numKeysToQuery, true)));
        } else {
            queryBuilder = new StringBuilder(
                    String.format(SELECT_QUERY_WITH_ONLY_PARTITION_COL, "DDB", tableName,
                            CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                            buildSQLQueryClause(numKeysToQuery, false)));

        }
        LOGGER.info("SELECT Query: " + queryBuilder);
        PreparedStatement stmt = connection.prepareStatement(queryBuilder.toString());
        return stmt;
    }

    /**
     * Place ? in the SQL at the right places
     *
     * @param numKeysToQuery
     * @param isSortKeyPresent
     * @return SQL Query
     */
    private static String buildSQLQueryClause(int numKeysToQuery, Boolean isSortKeyPresent) {
        StringBuilder temp = new StringBuilder();
        for (int k = 0; k < numKeysToQuery; k++) {
            if (isSortKeyPresent) {
                temp.append(PARAMETER_CLAUSE_IF_BOTH_COLS);
            } else {
                temp.append(PARAMETER_CLAUSE_IF_ONLY_PARTITION_COL);
            }
            if (k != numKeysToQuery - 1) {
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
            List<PColumn> tablePKCols, Map<String, Object> requestItemMap, int numKeysToQuery)
            throws SQLException {
        int index = 1;
        String partitionKeyPKCol = tablePKCols.get(0).toString();
        //iterates over the request map to get all the keys to query
        for (int j = 0; j < numKeysToQuery; j++) {
            Map<String, Object> valueForPartitionCol =
                    (Map<String, Object>) ((List<Map<String, Object>>) requestItemMap.get(
                            ApiMetadata.KEYS)).get(j).get(partitionKeyPKCol);
            DQLUtils.setKeyValueOnStatement(stmt, index++, valueForPartitionCol, false);
            if (tablePKCols.size() > 1) {
                String sortKeyPKCol = tablePKCols.get(1).toString();
                Map<String, Object> valueForSortCol =
                        (Map<String, Object>) ((List<Map<String, Object>>) requestItemMap.get(
                                ApiMetadata.KEYS)).get(j).get(sortKeyPKCol);
                DQLUtils.setKeyValueOnStatement(stmt, index++, valueForSortCol, false);
            }
        }
    }

    /**
     * Execute the given PreparedStatement, collect the returned item with projected attributes
     * and return GetItemResponse.
     */
    private static List<Map<String, Object>> executeQueryAndGetResponses(PreparedStatement stmt,
            Map<String, Object> requestItemMap) throws SQLException {
        List<Map<String, Object>> items = new ArrayList<>();
        ResultSet rs = stmt.executeQuery();
        while (rs.next()) {
            Map<String, Object> item =
                    BsonDocumentToMap.getProjectedItem((RawBsonDocument) rs.getObject(1),
                            getProjectionAttributes(requestItemMap));
            items.add(item);
        }
        return items;
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(Map<String, Object> requestItemMap) {
        List<String> attributesToGet = (List<String>) requestItemMap.get(ApiMetadata.ATTRIBUTES_TO_GET);
        String projExpr = (String) requestItemMap.get(ApiMetadata.PROJECTION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) requestItemMap.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        return DQLUtils.getProjectionAttributes(attributesToGet, projExpr, exprAttrNames);
    }

}
