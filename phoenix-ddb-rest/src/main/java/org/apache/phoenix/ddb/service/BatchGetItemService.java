package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.service.utils.DQLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.schema.PColumn;

public class BatchGetItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchGetItemService.class);

    private static final String SELECT_QUERY_WITH_SORT_COL =
            "SELECT COL FROM %s WHERE (%s,%s) IN (%s)";
    private static final String SELECT_QUERY_WITH_ONLY_PARTITION_COL =
            "SELECT COL FROM %s WHERE (%s) IN (%s)";
    private static final String PARAMETER_CLAUSE_IF_ONLY_PARTITION_COL = "(?)";
    private static final String PARAMETER_CLAUSE_IF_BOTH_COLS = "(?,?)";
    private static final String COMMA = ",";
    private static final int BATCH_GET_SIZE_LIMIT = 16 * 1024 * 1024; // 16MB
    
    private static class TableBatchResult {
        final List<Map<String, Object>> items;
        final List<Map<String, Object>> unprocessedKeys;
        final int sizeConsumed;
        
        TableBatchResult(List<Map<String, Object>> items,
                List<Map<String, Object>> unprocessedKeys, int sizeConsumed) {
            this.items = items;
            this.unprocessedKeys = unprocessedKeys;
            this.sizeConsumed = sizeConsumed;
        }
    }

    public static Map<String, Object> batchGetItem(Map<String, Object> request,
            String connectionUrl) {
        ValidationUtil.validateBatchGetItemRequest(request);
        List<PColumn> tablePKCols;
        Map<String, Object> finalResult = new HashMap<>();
        Map<String, List<Map<String, Object>>> responses = new HashMap<>();
        Map<String, Map<String, Object>> unprocessed = new HashMap<>();
        int totalResponseSize = 0;
        
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            Map<String, Object> requestItems = (Map<String, Object>) request.get(ApiMetadata.REQUEST_ITEMS);
            boolean sizeLimitReached = false;

            for (Map.Entry<String, Object> tableEntry : requestItems.entrySet()) {
                Map<String, Object> keysAndAttributes = (Map<String, Object>) tableEntry.getValue();
                if (sizeLimitReached) {
                    unprocessed.put(tableEntry.getKey(), keysAndAttributes);
                    continue;
                }
                
                tablePKCols = PhoenixUtils.getPKColumns(connection, tableEntry.getKey());
                int numKeysToQuery = ((List<Object>) keysAndAttributes.get(ApiMetadata.KEYS)).size();
                
                PreparedStatement stmt = getPreparedStatement(connection, tableEntry.getKey(), tablePKCols, numKeysToQuery);
                setPreparedStatementValues(stmt, tablePKCols, keysAndAttributes, numKeysToQuery);
                
                TableBatchResult
                        result = executeQueryAndGetResponses(stmt, keysAndAttributes, tablePKCols, BATCH_GET_SIZE_LIMIT - totalResponseSize);
                
                responses.put(tableEntry.getKey(), result.items);
                totalResponseSize += result.sizeConsumed;

                if (!result.unprocessedKeys.isEmpty()) {
                    Map<String, Object> unprocessedKeyMap = new HashMap<>();
                    unprocessedKeyMap.put(ApiMetadata.KEYS, result.unprocessedKeys);
                    if (keysAndAttributes.containsKey(ApiMetadata.ATTRIBUTES_TO_GET)) {
                        unprocessedKeyMap.put(ApiMetadata.ATTRIBUTES_TO_GET,
                                keysAndAttributes.get(ApiMetadata.ATTRIBUTES_TO_GET));
                    }
                    if (keysAndAttributes.containsKey(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES)) {
                        unprocessedKeyMap.put(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES,
                                keysAndAttributes.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES));
                    }
                    if (keysAndAttributes.containsKey(ApiMetadata.PROJECTION_EXPRESSION)) {
                        unprocessedKeyMap.put(ApiMetadata.PROJECTION_EXPRESSION,
                                keysAndAttributes.get(ApiMetadata.PROJECTION_EXPRESSION));
                    }
                    unprocessed.put(tableEntry.getKey(), unprocessedKeyMap);
                    sizeLimitReached = true;
                }
            }
            finalResult.put(ApiMetadata.RESPONSES, responses);
            finalResult.put(ApiMetadata.UNPROCESSED_KEYS, unprocessed);
            return finalResult;
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
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
                    String.format(SELECT_QUERY_WITH_SORT_COL, PhoenixUtils.getFullTableName(tableName, true),
                            CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                            CommonServiceUtils.getEscapedArgument(sortKeyPKCol),
                            buildSQLQueryClause(numKeysToQuery, true)));
        } else {
            queryBuilder = new StringBuilder(
                    String.format(SELECT_QUERY_WITH_ONLY_PARTITION_COL, PhoenixUtils.getFullTableName(tableName, true),
                            CommonServiceUtils.getEscapedArgument(partitionKeyPKCol),
                            buildSQLQueryClause(numKeysToQuery, false)));

        }
        LOGGER.debug("SELECT Query: " + queryBuilder);
        return connection.prepareStatement(queryBuilder.toString());
    }

    /**
     * Place ? in the SQL at the right places
     *
     * @param numKeysToQuery
     * @param isSortKeyPresent
     * @return SQL Query
     */
    private static String buildSQLQueryClause(int numKeysToQuery, boolean isSortKeyPresent) {
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
     * and return QueryResult with size tracking.
     */
    private static TableBatchResult executeQueryAndGetResponses(PreparedStatement stmt,
            Map<String, Object> requestItemMap, List<PColumn> tablePKCols, int remainingSizeLimit) throws SQLException {
        List<Map<String, Object>> items = new ArrayList<>();
        List<Map<String, Object>> unprocessedKeys = new ArrayList<>();
        int bytesSize = 0;
        boolean sizeLimitReached = false;
        List<String> projectionAttrs = getProjectionAttributes(requestItemMap);

        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                RawBsonDocument bsonDoc = (RawBsonDocument) rs.getObject(1);
                Map<String, Object> item =
                        BsonDocumentToMap.getProjectedItem(bsonDoc, projectionAttrs);
                int itemSize = (int) rs.unwrap(PhoenixResultSet.class).getCurrentRow().getSerializedSize();
                
                if (!sizeLimitReached && bytesSize + itemSize > remainingSizeLimit) {
                    sizeLimitReached = true;
                }
                
                if (sizeLimitReached) {
                    // Extract primary key from this item for unprocessed keys
                    Map<String, Object> keyMap = extractPrimaryKey(bsonDoc, tablePKCols);
                    unprocessedKeys.add(keyMap);
                } else {
                    items.add(item);
                    bytesSize += itemSize;
                }
            }
        }
        return new TableBatchResult(items, unprocessedKeys, bytesSize);
    }

    /**
     * Extract primary key from BsonDocument for unprocessed keys.
     */
    private static Map<String, Object> extractPrimaryKey(RawBsonDocument bsonDoc, List<PColumn> tablePKCols) {
        List<String> keyNames = new ArrayList<>();
        for (PColumn pkCol : tablePKCols) {
            keyNames.add(pkCol.getName().toString());
        }
        return BsonDocumentToMap.getProjectedItem(bsonDoc, keyNames);
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(Map<String, Object> requestItemMap) {
        List<String> attributesToGet = (List<String>) requestItemMap.get(ApiMetadata.ATTRIBUTES_TO_GET);
        Map<String, String> exprAttrNames =
                (Map<String, String>) requestItemMap.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        String projExpr = (String) requestItemMap.get(ApiMetadata.PROJECTION_EXPRESSION);
        if (attributesToGet != null) {
            if (exprAttrNames != null) {
                List<String> projectionAttrs = new ArrayList<>();
                for (String s : attributesToGet) {
                    projectionAttrs.add(exprAttrNames.getOrDefault(s, s));
                }
                return projectionAttrs;
            } else {
                return attributesToGet;
            }
        } else {
            return DQLUtils.getProjectionAttributes(projExpr, exprAttrNames);
        }
    }

}
