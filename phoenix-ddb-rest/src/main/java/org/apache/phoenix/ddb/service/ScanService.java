package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.service.utils.DQLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;

public class ScanService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanService.class);

    private static final String SELECT_QUERY = "SELECT COL FROM %s.\"%s\" ";
    private static final String SELECT_QUERY_WITH_INDEX_HINT =
            "SELECT /*+ INDEX(\"%s.%s\" \"%s\") */ COL FROM %s.\"%s\" ";

    private static final int MAX_SCAN_LIMIT = 100;

    /**
     * Enumeration of different scan types
     */
    private enum ScanType {
        NO_EXCLUSIVE_START_KEY,           // Simple scan without pagination
        SINGLE_KEY_CONTINUATION,          // pk > value (single key table)
        TWO_KEY_FIRST_QUERY,             // pk1 = value AND pk2 > value (two key table, first query)
        TWO_KEY_SECOND_QUERY             // pk1 > value (two key table, second query)
    }

    /**
     * Container for scan configuration
     */
    private static class ScanConfig {
        final ScanType type;
        final boolean useIndex;
        final List<PColumn> tablePKCols;
        final List<PColumn> indexPKCols;
        final PColumn partitionKeyCol;
        final PColumn sortKeyCol;
        final int limit;
        final String tableName;
        final String indexName;

        ScanConfig(ScanType type, boolean useIndex, List<PColumn> tablePKCols,
                       List<PColumn> indexPKCols, int limit, String tableName, String indexName) {
            this.type = type;
            this.useIndex = useIndex;
            this.tablePKCols = tablePKCols;
            this.indexPKCols = indexPKCols;
            this.limit = limit;
            this.tableName = tableName;
            this.indexName = indexName;
            
            List<PColumn> relevantPKCols = useIndex ? indexPKCols : tablePKCols;
            this.partitionKeyCol = relevantPKCols.get(0);
            this.sortKeyCol = (relevantPKCols.size() > 1) ? relevantPKCols.get(1) : null;
        }
    }

    public static Map<String, Object> scan(Map<String, Object> request, String connectionUrl) {
        ValidationUtil.validateScanRequest(request);
        
        // Phoenix does not support parallel scans from the client
        if (request.get(ApiMetadata.SEGMENT) != null && (Integer) request.get(ApiMetadata.SEGMENT) > 0) {
            return Collections.emptyMap();
        }
        
        try (Connection connection = DriverManager.getConnection(connectionUrl, DQLUtils.getConnectionProps())) {
            return executeScan(connection, request);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Public method to get PreparedStatement for testing purposes.
     * Returns the primary PreparedStatement that would be used for the scan request.
     * For two-query scenarios, returns the first query's PreparedStatement.
     */
    public static PreparedStatement getPreparedStatement(Connection connection, Map<String, Object> request) 
            throws SQLException {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        boolean useIndex = !StringUtils.isEmpty(indexName);
        
        List<PColumn> tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
        List<PColumn> indexPKCols = useIndex ? PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName) : null;
        
        Map<String, Object> exclusiveStartKey = (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
        int effectiveLimit = getEffectiveLimit(request);
        
        ScanConfig config = new ScanConfig(
            determineScanType(exclusiveStartKey, useIndex, tablePKCols, indexPKCols),
            useIndex, tablePKCols, indexPKCols, effectiveLimit, tableName, indexName
        );

        // For two-query scenarios, return the first query's PreparedStatement
        if (config.type == ScanType.TWO_KEY_FIRST_QUERY) {
            ScanConfig firstConfig = new ScanConfig(ScanType.TWO_KEY_FIRST_QUERY, config.useIndex,
                    config.tablePKCols, config.indexPKCols, config.limit, config.tableName, config.indexName);
            return buildQuery(connection, request, firstConfig);
        } else {
            return buildQuery(connection, request, config);
        }
    }

    /**
     * Main scan execution logic - determines approach and executes accordingly.
     */
    private static Map<String, Object> executeScan(Connection connection, Map<String, Object> request) 
            throws SQLException {
        
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        boolean useIndex = !StringUtils.isEmpty(indexName);
        
        List<PColumn> tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
        List<PColumn> indexPKCols = useIndex ? PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName) : null;
        
        Map<String, Object> exclusiveStartKey = (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
        int effectiveLimit = getEffectiveLimit(request);
        
        ScanConfig config = new ScanConfig(
            determineScanType(exclusiveStartKey, useIndex, tablePKCols, indexPKCols),
            useIndex, tablePKCols, indexPKCols, effectiveLimit, tableName, indexName
        );

        // Execute based on scan type
        switch (config.type) {
            case NO_EXCLUSIVE_START_KEY:
            case SINGLE_KEY_CONTINUATION:
                return executeSingleQuery(connection, request, config);
            case TWO_KEY_FIRST_QUERY:
                return executeTwoKeyTableScan(connection, request, config);
            default:
                throw new IllegalStateException("Unsupported scan config type: " + config.type);
        }
    }

    /**
     * Determine the appropriate scan type based on request parameters
     */
    private static ScanType determineScanType(Map<String, Object> exclusiveStartKey, boolean useIndex,
                                               List<PColumn> tablePKCols, List<PColumn> indexPKCols) {
        if (exclusiveStartKey == null || exclusiveStartKey.isEmpty()) {
            return ScanType.NO_EXCLUSIVE_START_KEY;
        }
        
        List<PColumn> relevantPKCols = useIndex ? indexPKCols : tablePKCols;
        if (relevantPKCols.size() == 1) {
            return ScanType.SINGLE_KEY_CONTINUATION;
        } else {
            return ScanType.TWO_KEY_FIRST_QUERY;
        }
    }

    /**
     * Get effective limit, applying default and maximum constraints
     */
    private static int getEffectiveLimit(Map<String, Object> request) {
        Integer requestLimit = (Integer) request.get(ApiMetadata.LIMIT);
        return (requestLimit == null) ? MAX_SCAN_LIMIT : Math.min(requestLimit, MAX_SCAN_LIMIT);
    }

    /**
     * Execute a single query scan (for no pagination, single key, or original logic)
     */
    private static Map<String, Object> executeSingleQuery(Connection connection, Map<String, Object> request,
                                                         ScanConfig config) throws SQLException {
        PreparedStatement stmt = buildQuery(connection, request, config);
        return DQLUtils.executeStatementReturnResult(stmt, getProjectionAttributes(request),
                config.useIndex, config.tablePKCols, config.indexPKCols, config.tableName, false, false);
    }

    /**
     * Execute two-key table scan using the two-query approach
     */
    private static Map<String, Object> executeTwoKeyTableScan(Connection connection, Map<String, Object> request,
                                                            ScanConfig config) throws SQLException {
        
        // Execute first query: (pk1 = k1 AND pk2 > k2)
        
        PreparedStatement firstStmt = buildQuery(connection, request, config);
        Map<String, Object> firstResult = DQLUtils.executeStatementReturnResult(firstStmt,
                getProjectionAttributes(request), config.useIndex, config.tablePKCols, config.indexPKCols,
                config.tableName, false, true);
        
        List<Map<String, Object>> allItems = new ArrayList<>((List<Map<String, Object>>) firstResult.get(ApiMetadata.ITEMS));
        int totalCount = (Integer) firstResult.get(ApiMetadata.COUNT);
        int totalScannedCount = (Integer) firstResult.get(ApiMetadata.SCANNED_COUNT);
        Map<String, Object> lastEvaluatedKey = (Map<String, Object>) firstResult.get(ApiMetadata.LAST_EVALUATED_KEY);
        
        // Execute second query if needed: (pk1 > k1)
        if (totalCount < config.limit && !(boolean)firstResult.get(DQLUtils.SIZE_LIMIT_REACHED)) {
            int remainingLimit = config.limit - totalCount;
            ScanConfig secondConfig = new ScanConfig(ScanType.TWO_KEY_SECOND_QUERY, config.useIndex,
                    config.tablePKCols, config.indexPKCols, remainingLimit, config.tableName, config.indexName);
            
            PreparedStatement secondStmt = buildQuery(connection, request, secondConfig);
            Map<String, Object> secondResult = DQLUtils.executeStatementReturnResult(secondStmt,
                    getProjectionAttributes(request), config.useIndex, config.tablePKCols, config.indexPKCols,
                    config.tableName, false, false);
            
            List<Map<String, Object>> secondItems = (List<Map<String, Object>>) secondResult.get(ApiMetadata.ITEMS);
            allItems.addAll(secondItems);
            totalCount += (Integer) secondResult.get(ApiMetadata.COUNT);
            totalScannedCount += (Integer) secondResult.get(ApiMetadata.SCANNED_COUNT);
            
            // Use LastEvaluatedKey from second query if it returned items, otherwise from first query
            Map<String, Object> secondLastKey = (Map<String, Object>) secondResult.get(ApiMetadata.LAST_EVALUATED_KEY);
            if (secondLastKey != null) {
                lastEvaluatedKey = secondLastKey;
            }
        }
        
        return buildScanResponse(allItems, totalCount, totalScannedCount, config.tableName, lastEvaluatedKey);
    }

    /**
     * Unified query builder that handles all scan types
     */
    private static PreparedStatement buildQuery(Connection connection, Map<String, Object> request,
                                              ScanConfig config) throws SQLException {
        
        StringBuilder queryBuilder = buildBaseSelectClause(config);
        queryBuilder.append(" WHERE ");
        
        boolean hasFilter = addFilterConditionIfPresent(queryBuilder, request);
        addKeyConditions(queryBuilder, config, hasFilter);
        addLimitClause(queryBuilder, config.limit);
        
        LOGGER.debug("Scan Query ({}): {}", config.type, queryBuilder);
        
        PreparedStatement stmt = connection.prepareStatement(queryBuilder.toString());
        setQueryParameters(stmt, request, config);
        return stmt;
    }

    /**
     * Build the base SELECT clause with optional index hint
     */
    private static StringBuilder buildBaseSelectClause(ScanConfig config) {
        if (StringUtils.isEmpty(config.indexName)) {
            return new StringBuilder(String.format(SELECT_QUERY, "DDB", config.tableName));
        } else {
            return new StringBuilder(String.format(SELECT_QUERY_WITH_INDEX_HINT, 
                    "DDB", config.tableName, config.indexName, "DDB", config.tableName));
        }
    }

    /**
     * Add filter condition if present in request
     * @return true if filter was added
     */
    private static boolean addFilterConditionIfPresent(StringBuilder queryBuilder, Map<String, Object> request) {
        String filterExpr = (String) request.get(ApiMetadata.FILTER_EXPRESSION);
        if (!StringUtils.isEmpty(filterExpr)) {
            Map<String, String> exprAttrNames = (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
            Map<String, Object> exprAttrValues = (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);
            DQLUtils.addFilterCondition(false, queryBuilder, filterExpr, exprAttrNames, exprAttrValues);
            return true;
        }
        return false;
    }

    /**
     * Add key-based WHERE conditions based on scan type
     */
    private static void addKeyConditions(StringBuilder queryBuilder, ScanConfig config, boolean hasFilter) {
        if (config.type == ScanType.NO_EXCLUSIVE_START_KEY) {
            // No key conditions needed for simple scan
            if (!hasFilter) {
                // Remove the WHERE clause since no conditions
                queryBuilder.setLength(queryBuilder.length() - " WHERE ".length());
            }
            return;
        }
        
        if (hasFilter) {
            queryBuilder.append(" AND ");
        }
        
        String partitionKeyName = getColumnNameForQuery(config.partitionKeyCol, config.useIndex);
        
        switch (config.type) {
            case SINGLE_KEY_CONTINUATION:
            case TWO_KEY_SECOND_QUERY:
                queryBuilder.append(partitionKeyName).append(" > ?");
                break;
            case TWO_KEY_FIRST_QUERY:
                String sortKeyName = getColumnNameForQuery(config.sortKeyCol, config.useIndex);
                queryBuilder.append("(").append(partitionKeyName).append(" = ? AND ")
                           .append(sortKeyName).append(" > ?)");
                break;
        }
    }

    /**
     * Add LIMIT clause to query
     */
    private static void addLimitClause(StringBuilder queryBuilder, int limit) {
        queryBuilder.append(" LIMIT ").append(limit);
    }

    /**
     * Set all parameters on the PreparedStatement based on scan type
     */
    private static void setQueryParameters(PreparedStatement stmt, Map<String, Object> request,
                                         ScanConfig config) throws SQLException {
        if (config.type == ScanType.NO_EXCLUSIVE_START_KEY) {
            return; // No parameters to set
        }
        
        Map<String, Object> exclusiveStartKey = (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
        String partitionKeyName = getKeyNameFromColumn(config.partitionKeyCol, config.useIndex);
        
        switch (config.type) {
            case SINGLE_KEY_CONTINUATION:
            case TWO_KEY_SECOND_QUERY:
                DQLUtils.setKeyValueOnStatement(stmt, 1, 
                        (Map<String, Object>) exclusiveStartKey.get(partitionKeyName), false);
                break;
                
            case TWO_KEY_FIRST_QUERY:
                String sortKeyName = getKeyNameFromColumn(config.sortKeyCol, config.useIndex);
                // Set pk1 = ?
                DQLUtils.setKeyValueOnStatement(stmt, 1, 
                        (Map<String, Object>) exclusiveStartKey.get(partitionKeyName), false);
                // Set pk2 > ?
                DQLUtils.setKeyValueOnStatement(stmt, 2, 
                        (Map<String, Object>) exclusiveStartKey.get(sortKeyName), false);
                break;
        }
    }

    /**
     * Get column name for SQL query
     */
    private static String getColumnNameForQuery(PColumn column, boolean useIndex) {
        String name = column.getName().toString();
        return useIndex ? name.substring(1) : CommonServiceUtils.getEscapedArgument(name);
    }

    /**
     * Get key name from column for parameter mapping
     */
    private static String getKeyNameFromColumn(PColumn column, boolean useIndex) {
        String keyName = column.getName().toString();
        return useIndex ? CommonServiceUtils.getKeyNameFromBsonValueFunc(keyName) : keyName;
    }

    /**
     * Build the final scan response
     */
    private static Map<String, Object> buildScanResponse(List<Map<String, Object>> items, int count, 
                                                        int scannedCount, String tableName, 
                                                        Map<String, Object> lastEvaluatedKey) {
        Map<String, Object> response = new HashMap<>();
        response.put(ApiMetadata.ITEMS, items);
        response.put(ApiMetadata.COUNT, count);
        response.put(ApiMetadata.SCANNED_COUNT, scannedCount);
        response.put(ApiMetadata.CONSUMED_CAPACITY, CommonServiceUtils.getConsumedCapacity(tableName));
        response.put(ApiMetadata.LAST_EVALUATED_KEY, lastEvaluatedKey);
        return response;
    }

    /**
     * Get projection attributes from request
     */
    private static List<String> getProjectionAttributes(Map<String, Object> request) {
        String projExpr = (String) request.get(ApiMetadata.PROJECTION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        return DQLUtils.getProjectionAttributes(projExpr, exprAttrNames);
    }
}
