/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.service.utils.SegmentScanUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.service.utils.DQLUtils;
import org.apache.phoenix.ddb.service.utils.ScanSegmentInfo;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PDouble;

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
        final boolean countOnly;
        // Segment scan fields
        boolean isSegmentScan = false;
        ScanSegmentInfo scanSegmentInfo = null;


        ScanConfig(ScanType type, boolean useIndex, List<PColumn> tablePKCols,
                List<PColumn> indexPKCols, int limit, String tableName, String indexName,
                boolean countOnly) {
            this.type = type;
            this.useIndex = useIndex;
            this.tablePKCols = tablePKCols;
            this.indexPKCols = indexPKCols;
            this.limit = limit;
            this.tableName = tableName;
            this.indexName = indexName;
            this.countOnly = countOnly;
            
            List<PColumn> relevantPKCols = useIndex ? indexPKCols : tablePKCols;
            this.partitionKeyCol = relevantPKCols.get(0);
            this.sortKeyCol = (relevantPKCols.size() > 1) ? relevantPKCols.get(1) : null;
        }

        void setScanSegmentInfo(ScanSegmentInfo segmentInfo) {
            this.isSegmentScan = true;
            this.scanSegmentInfo = segmentInfo;
        }

        boolean isSegmentScan() {
            return isSegmentScan && (scanSegmentInfo != null);
        }

        @Override
        public String toString() {
            if (isSegmentScan) {
                return this.type.toString() + "," + this.scanSegmentInfo.toShortString();
            } else {
                return this.type.toString();
            }
        }
    }

    public static Map<String, Object> scan(Map<String, Object> request, String connectionUrl) {
        ValidationUtil.validateScanRequest(request);
        handleLegacyParamsConversion(request);
        CommonServiceUtils.handleLegacyProjectionConversion(request);

        // Segment Scan on indexes is not yet supported - we will return all items for segment 0.
        if (isSegmentScanRequestOnIndex(request) && (Integer) request.get(ApiMetadata.SEGMENT) > 0) {
            return buildEmptyScanResponse(request);
        }

        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            return executeScan(connection, request);
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
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
        boolean countOnly = ApiMetadata.SELECT_COUNT.equals(request.get(ApiMetadata.SELECT));
        
        ScanConfig config = new ScanConfig(
            determineScanType(exclusiveStartKey, useIndex, tablePKCols, indexPKCols),
            useIndex, tablePKCols, indexPKCols, effectiveLimit, tableName, indexName, countOnly
        );

        // Set segment info if this is a segment scan
        if (isSegmentScanRequestOnTable(request)) {
            ScanSegmentInfo segmentInfo = getSegmentInfo(connection, request);
            // Return empty result if segment doesn't exist
            if (segmentInfo == null || segmentInfo.isEmptySegment()) {
                return buildEmptyScanResponse(request);
            }
            config.setScanSegmentInfo(segmentInfo);
        }

        // Execute based on scan type (same logic for both regular and segment scans)
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
                config.useIndex, config.tablePKCols, config.indexPKCols, config.tableName,
                false, false, config.countOnly);
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
                config.tableName, false, true, config.countOnly);
        
        List<Map<String, Object>> allItems = config.countOnly
                ? new ArrayList<>()
                : new ArrayList<>((List<Map<String, Object>>) firstResult.get(ApiMetadata.ITEMS));
        int totalCount = (Integer) firstResult.get(ApiMetadata.COUNT);
        int totalScannedCount = (Integer) firstResult.get(ApiMetadata.SCANNED_COUNT);
        Map<String, Object> lastEvaluatedKey = (Map<String, Object>) firstResult.get(ApiMetadata.LAST_EVALUATED_KEY);

        // Execute second query if needed: (pk1 > k1)
        if (totalCount < config.limit && !(boolean)firstResult.get(DQLUtils.SIZE_LIMIT_REACHED)) {
            int remainingLimit = config.limit - totalCount;
            ScanConfig secondConfig = new ScanConfig(ScanType.TWO_KEY_SECOND_QUERY, config.useIndex,
                    config.tablePKCols, config.indexPKCols, remainingLimit, config.tableName,
                    config.indexName, config.countOnly);
            // Copy segment scan configuration if present
            if (config.isSegmentScan()) {
                secondConfig.setScanSegmentInfo(config.scanSegmentInfo);
            }
            PreparedStatement secondStmt = buildQuery(connection, request, secondConfig);
            Map<String, Object> secondResult = DQLUtils.executeStatementReturnResult(secondStmt,
                    getProjectionAttributes(request), config.useIndex, config.tablePKCols, config.indexPKCols,
                    config.tableName, false, false, config.countOnly);

            if (!config.countOnly) {
                List<Map<String, Object>> secondItems = (List<Map<String, Object>>) secondResult.get(ApiMetadata.ITEMS);
                allItems.addAll(secondItems);
            }
            totalCount += (Integer) secondResult.get(ApiMetadata.COUNT);
            totalScannedCount += (Integer) secondResult.get(ApiMetadata.SCANNED_COUNT);
            
            // Use LastEvaluatedKey from second query if it returned items, otherwise from first query
            Map<String, Object> secondLastKey = (Map<String, Object>) secondResult.get(ApiMetadata.LAST_EVALUATED_KEY);
            if (secondLastKey != null) {
                lastEvaluatedKey = secondLastKey;
            }
        }
        
        return buildScanResponse(allItems, totalCount, totalScannedCount, config.tableName, lastEvaluatedKey, config.countOnly);
    }

    /**
     * Unified query builder that handles all scan types
     */
    private static PreparedStatement buildQuery(Connection connection, Map<String, Object> request,
                                              ScanConfig config) throws SQLException {
        
        StringBuilder queryBuilder = buildBaseSelectClause(config);

        // Add filter conditions
        boolean hasFilterCondition = addFilterConditionIfPresent(queryBuilder, request);

        // Add key conditions
        boolean hasKeyConditions = addKeyConditions(queryBuilder, config, hasFilterCondition);

        // Add segment boundary conditions
        addSegmentBoundaryConditions(queryBuilder, config, hasFilterCondition || hasKeyConditions);

        // Add order by clause
        addOrderByClause(queryBuilder, config, hasFilterCondition);

        // Add limit clause
        addLimitClause(queryBuilder, config.limit);

        LOGGER.debug("Scan Query ({}): {}", config, queryBuilder);

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
            queryBuilder.append(" WHERE ");
            Map<String, String> exprAttrNames = (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
            Map<String, Object> exprAttrValues = (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);
            DQLUtils.addFilterCondition(false, queryBuilder, filterExpr, exprAttrNames, exprAttrValues);
            return true;
        }
        return false;
    }

    /**
     * Add key-based WHERE conditions based on scan type
     *
     * @return true if key conditions were added
     */
    private static boolean addKeyConditions(StringBuilder queryBuilder, ScanConfig config,
            boolean hasPreviousConditions) {
        if (config.type == ScanType.NO_EXCLUSIVE_START_KEY) {
            // No key conditions needed for simple scan
            return false;
        }
        
        if (hasPreviousConditions) {
            queryBuilder.append(" AND ");
        } else {
            queryBuilder.append(" WHERE ");
        }
        
        String partitionKeyName = getColumnNameForQuery(config.partitionKeyCol, config.useIndex);
        
        switch (config.type) {
            case SINGLE_KEY_CONTINUATION:
            case TWO_KEY_SECOND_QUERY:
                queryBuilder.append(partitionKeyName).append(" > ? ");
                break;
            case TWO_KEY_FIRST_QUERY:
                String sortKeyName = getColumnNameForQuery(config.sortKeyCol, config.useIndex);
                queryBuilder.append("( ").append(partitionKeyName).append(" = ? AND ")
                           .append(sortKeyName).append(" > ? ) ");
                break;
        }
        return true;
    }

    /**
     * Add LIMIT clause to query
     */
    private static void addLimitClause(StringBuilder queryBuilder, int limit) {
        queryBuilder.append(" LIMIT ").append(limit);
    }

    /**
     * Add ORDER BY clause to query.
     * If leading key is double, rows can be returned in a different order than number ordering
     * since a query without ORDER BY uses ROUND ROBIN FULL SCAN.
     */
    private static void addOrderByClause(StringBuilder queryBuilder, ScanConfig config,
            boolean hasFilterCondition) {
        if (hasFilterCondition && config.partitionKeyCol.getDataType() == PDouble.INSTANCE) {
            String partitionKeyName = config.useIndex
                    ? config.partitionKeyCol.getName().getString().substring(1)
                    : CommonServiceUtils.getEscapedArgument(config.partitionKeyCol.getName().getString());
            queryBuilder.append(" ORDER BY ").append(partitionKeyName).append(" ");
        }
    }

    /**
     * Set all parameters on the PreparedStatement based on scan type
     */
    private static void setQueryParameters(PreparedStatement stmt, Map<String, Object> request,
                                         ScanConfig config) throws SQLException {

        int paramIndex = 1;
        if (config.type != ScanType.NO_EXCLUSIVE_START_KEY) {
            // Set key condition parameters first
            Map<String, Object> exclusiveStartKey =
                    (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
            String partitionKeyName = getKeyNameFromColumn(config.partitionKeyCol, config.useIndex);

            switch (config.type) {
            case SINGLE_KEY_CONTINUATION:
            case TWO_KEY_SECOND_QUERY:
                DQLUtils.setKeyValueOnStatement(stmt, paramIndex++,
                        (Map<String, Object>) exclusiveStartKey.get(partitionKeyName), false);
                break;

            case TWO_KEY_FIRST_QUERY:
                String sortKeyName = getKeyNameFromColumn(config.sortKeyCol, config.useIndex);
                // Set pk1 = ?
                DQLUtils.setKeyValueOnStatement(stmt, paramIndex++,
                        (Map<String, Object>) exclusiveStartKey.get(partitionKeyName), false);
                // Set pk2 > ?
                DQLUtils.setKeyValueOnStatement(stmt, paramIndex++,
                        (Map<String, Object>) exclusiveStartKey.get(sortKeyName), false);
                break;
            }
        }

        // Set segment boundary parameters if this is a segment scan
        if (config.isSegmentScan()) {
            byte[] startKey = config.scanSegmentInfo.getStartKey();
            byte[] endKey = config.scanSegmentInfo.getEndKey();
            stmt.setBytes(paramIndex++, startKey);
            stmt.setBytes(paramIndex++, endKey);
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
                                                        Map<String, Object> lastEvaluatedKey,
                                                        boolean countOnly) {
        Map<String, Object> response = new HashMap<>();
        if (!countOnly) {
            response.put(ApiMetadata.ITEMS, items);
        }
        response.put(ApiMetadata.COUNT, count);
        response.put(ApiMetadata.SCANNED_COUNT, scannedCount);
        response.put(ApiMetadata.CONSUMED_CAPACITY, CommonServiceUtils.getConsumedCapacity(tableName));
        response.put(ApiMetadata.LAST_EVALUATED_KEY, lastEvaluatedKey);
        return response;
    }

    /**
     * Build the final scan response
     */
    private static Map<String, Object> buildEmptyScanResponse(Map<String, Object> request) {
        return buildScanResponse(Collections.emptyList(), 0, 0,
                (String) request.get(ApiMetadata.TABLE_NAME), null,
                ApiMetadata.SELECT_COUNT.equals(request.get(ApiMetadata.SELECT)));
    }

    /**
     * Get projection attributes from request
     */
    private static List<String> getProjectionAttributes(Map<String, Object> request) {
        String projExpr = (String) request.get(ApiMetadata.PROJECTION_EXPRESSION);
        String select = (String) request.get(ApiMetadata.SELECT);
        if (ApiMetadata.SPECIFIC_ATTRIBUTES.equals(select) && StringUtils.isEmpty(projExpr)) {
            throw new ValidationException("ProjectionExpression must be provided when querying SPECIFIC_ATTRIBUTES.");
        }
        if (ApiMetadata.ALL_ATTRIBUTES.equals(select) && !StringUtils.isEmpty(projExpr)) {
            throw new ValidationException("Cannot specify the ProjectionExpression when choosing to get ALL_ATTRIBUTES.");
        }
        // select all attributes overrides projection expression
        if (ApiMetadata.ALL_ATTRIBUTES.equals(select)) {
            projExpr = StringUtils.EMPTY;
        }
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        return DQLUtils.getProjectionAttributes(projExpr, exprAttrNames);
    }

    /**
     * Check if the request is for a segment scan
     */
    public static boolean isSegmentScanRequestOnTable(Map<String, Object> request) {
        return request.get(ApiMetadata.SEGMENT) != null
                && request.get(ApiMetadata.TOTAL_SEGMENTS) != null
                && StringUtils.isEmpty((String)request.get(ApiMetadata.INDEX_NAME));
    }

    /**
     * Check if the request is for a segment scan
     */
    public static boolean isSegmentScanRequestOnIndex(Map<String, Object> request) {
        return request.get(ApiMetadata.SEGMENT) != null
                && request.get(ApiMetadata.TOTAL_SEGMENTS) != null
                && !StringUtils.isEmpty((String)request.get(ApiMetadata.INDEX_NAME));
    }

    /**
     * Get segment info for segment scan
     */
    private static ScanSegmentInfo getSegmentInfo(Connection connection,
            Map<String, Object> request) throws SQLException {
        Integer segment = (Integer) request.get(ApiMetadata.SEGMENT);
        Integer totalSegments = (Integer) request.get(ApiMetadata.TOTAL_SEGMENTS);
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        Map<String, Object> exclusiveStartKey =
                (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);

        // Get segment boundaries using SegmentScanUtil
        if (exclusiveStartKey == null || exclusiveStartKey.isEmpty()) {
            // First page - generate and get segment boundaries
            return SegmentScanUtil.updateAndGetSegmentScanRange(connection, tableName,
                    totalSegments, segment);
        } else {
            // Subsequent page - boundaries should already exist
            return SegmentScanUtil.getSegmentScanRange(connection, tableName, totalSegments,
                    segment);
        }
    }

    /**
     * Add segment boundary conditions to the query
     *
     * @return true if segment conditions were added
     */
    private static boolean addSegmentBoundaryConditions(StringBuilder queryBuilder,
            ScanConfig config, boolean hasPreviousConditions) {
        if (!config.isSegmentScan()) {
            return false;
        }
        if (hasPreviousConditions) {
            queryBuilder.append(" AND ");
        } else {
            queryBuilder.append(" WHERE ");
        }
        queryBuilder.append(" SCAN_START_KEY() = ? AND SCAN_END_KEY() = ? ");
        return true;
    }

    /*
     * Handles legacy parameter conversion to modern equivalents.
     */
    private static void handleLegacyParamsConversion(Map<String, Object> request) {
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        if (exprAttrNames == null) {
            exprAttrNames = new HashMap<>();
            request.put(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES, exprAttrNames);
        }
        Map<String, Object> exprAttrValues =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);
        if (exprAttrValues == null) {
            exprAttrValues = new HashMap<>();
            request.put(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES, exprAttrValues);
        }

        Map<String, Object> scanFilter = (Map<String, Object>) request.get(ApiMetadata.SCAN_FILTER);
        if (scanFilter != null) {
            String conditionalOperator = (String) request.get(ApiMetadata.CONDITIONAL_OPERATOR);
            if (conditionalOperator == null) {
                conditionalOperator = "AND";
            }

            String filterExpression =
                    CommonServiceUtils.convertExpectedToConditionExpression(scanFilter,
                            conditionalOperator, exprAttrNames, exprAttrValues);
            if (filterExpression != null) {
                request.put(ApiMetadata.FILTER_EXPRESSION, filterExpression);
            }
            request.remove(ApiMetadata.SCAN_FILTER);
            request.remove(ApiMetadata.CONDITIONAL_OPERATOR);
        }
    }

    /**
     * Public method to get PreparedStatement for testing purposes.
     * Returns the primary PreparedStatement that would be used for the scan request.
     * For two-query scenarios, returns the first query's PreparedStatement.
     */
    @VisibleForTesting
    public static PreparedStatement getPreparedStatement(Connection connection, Map<String, Object> request)
            throws SQLException {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        boolean useIndex = !StringUtils.isEmpty(indexName);

        List<PColumn> tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
        List<PColumn> indexPKCols = useIndex ? PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName) : null;

        Map<String, Object> exclusiveStartKey = (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
        int effectiveLimit = getEffectiveLimit(request);
        boolean countOnly = ApiMetadata.SELECT_COUNT.equals(request.get(ApiMetadata.SELECT));

        ScanConfig config = new ScanConfig(
                determineScanType(exclusiveStartKey, useIndex, tablePKCols, indexPKCols),
                useIndex, tablePKCols, indexPKCols, effectiveLimit, tableName, indexName, countOnly
        );

        // For two-query scenarios, return the first query's PreparedStatement
        if (config.type == ScanType.TWO_KEY_FIRST_QUERY) {
            ScanConfig firstConfig = new ScanConfig(ScanType.TWO_KEY_FIRST_QUERY, config.useIndex,
                    config.tablePKCols, config.indexPKCols, config.limit, config.tableName,
                    config.indexName, config.countOnly);
            return buildQuery(connection, request, firstConfig);
        } else {
            return buildQuery(connection, request, config);
        }
    }
}
