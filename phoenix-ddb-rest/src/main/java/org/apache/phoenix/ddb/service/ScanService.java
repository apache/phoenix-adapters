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
import java.util.HashMap;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.service.utils.SegmentScanUtil;
import org.apache.phoenix.ddb.service.utils.ScanConfig;
import org.apache.phoenix.ddb.service.utils.ScanConfig.ScanType;
import org.apache.phoenix.ddb.utils.ApiMetadata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.service.utils.DQLUtils;
import org.apache.phoenix.ddb.service.utils.ScanSegmentInfo;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;

public class ScanService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanService.class);

    private static final String SELECT_QUERY = "SELECT COL FROM %s ";
    private static final String SELECT_QUERY_WITH_INDEX_HINT =
            "SELECT /*+ INDEX(\"%s.%s\" \"%s\") */ COL FROM %s ";

    private static final int MAX_SCAN_LIMIT = 100;

    public static Map<String, Object> scan(Map<String, Object> request, String connectionUrl) {
        ValidationUtil.validateScanRequest(request);
        handleLegacyParamsConversion(request);
        CommonServiceUtils.handleLegacyProjectionConversion(request);

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
        
        ScanConfig config = new ScanConfig(determineScanType(exclusiveStartKey),
            useIndex, tablePKCols, indexPKCols, effectiveLimit, tableName, indexName, countOnly
        );

        // Set segment info if this is a segment scan
        if (isSegmentScanRequest(request)) {
            ScanSegmentInfo segmentInfo = getSegmentInfo(connection, request);
            // Return empty result if segment doesn't exist
            if (segmentInfo == null || segmentInfo.isEmptySegment()) {
                return buildEmptyScanResponse(request);
            }
            config.setScanSegmentInfo(segmentInfo);
        }

        PreparedStatement stmt = buildQuery(connection, request, config);
        return DQLUtils.executeStatementReturnResult(stmt, getProjectionAttributes(request),
                config.useIndex(), config.getTablePKCols(), config.getIndexPKCols(), config.getTableName(),
                false, config.isCountOnly());
    }

    /**
     * Determine the appropriate scan type based on request parameters
     */
    public static ScanType determineScanType(Map<String, Object> exclusiveStartKey) {
        if (exclusiveStartKey == null || exclusiveStartKey.isEmpty()) {
            return ScanType.NO_EXCLUSIVE_START_KEY;
        }
        return ScanType.WITH_EXCLUSIVE_START_KEY;
    }

    /**
     * Get effective limit, applying default and maximum constraints
     */
    private static int getEffectiveLimit(Map<String, Object> request) {
        Integer requestLimit = (Integer) request.get(ApiMetadata.LIMIT);
        return (requestLimit == null) ? MAX_SCAN_LIMIT : Math.min(requestLimit, MAX_SCAN_LIMIT);
    }

    /**
     * Unified query builder that handles all scan types
     */
    public static PreparedStatement buildQuery(Connection connection, Map<String, Object> request,
            ScanConfig config) throws SQLException {
        
        StringBuilder queryBuilder = buildBaseSelectClause(config);

        // Add filter conditions
        boolean hasFilterCondition = addFilterConditionIfPresent(queryBuilder, request);

        // Add key conditions (RVC)
        boolean hasKeyConditions = addRVC(queryBuilder, config, hasFilterCondition);

        // Add segment boundary conditions
        addSegmentBoundaryConditions(queryBuilder, config, hasFilterCondition || hasKeyConditions);

        // Add order by clause
        addOrderByClause(queryBuilder, config);

        // Add limit clause
        addLimitClause(queryBuilder, config.getLimit());

        LOGGER.debug("Scan Query ({}): {}", config, queryBuilder);

        PreparedStatement stmt = connection.prepareStatement(queryBuilder.toString());
        setQueryParameters(stmt, request, config);
        return stmt;
    }

    /**
     * Build the base SELECT clause with optional index hint
     */
    private static StringBuilder buildBaseSelectClause(ScanConfig config) {
        String fullTableName = PhoenixUtils.getFullTableName(config.getTableName(), true);
        if (StringUtils.isEmpty(config.getIndexName())) {
            return new StringBuilder(String.format(SELECT_QUERY, fullTableName));
        } else {
            String fullIndexName = PhoenixUtils.getInternalIndexName(config.getTableName(), config.getIndexName());
            return new StringBuilder(
                    String.format(SELECT_QUERY_WITH_INDEX_HINT, PhoenixUtils.SCHEMA_NAME,
                            config.getTableName(), fullIndexName, fullTableName));
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
     * Add RVC clause if request has lastEvaluatedKey.
     *
     * @return true if RVC clause was added
     */
    private static boolean addRVC(StringBuilder queryBuilder, ScanConfig config,
                                  boolean hasPreviousConditions) {
        if (config.getType() == ScanType.NO_EXCLUSIVE_START_KEY) {
            // No key conditions needed for simple scan
            return false;
        }
        
        if (hasPreviousConditions) {
            queryBuilder.append(" AND ");
        } else {
            queryBuilder.append(" WHERE ");
        }

        String rvcClause = DQLUtils.getRVCClauseForScan(" > ", config.getTablePKCols(),
                config.useIndex(), config.getIndexPKCols());
        queryBuilder.append(rvcClause);
        return true;
    }

    /**
     * Add LIMIT clause to query
     */
    private static void addLimitClause(StringBuilder queryBuilder, int limit) {
        queryBuilder.append(" LIMIT ").append(limit);
    }

    private static void addOrderByClause(StringBuilder queryBuilder, ScanConfig config) {
        queryBuilder.append(" ORDER BY ");

        if (config.useIndex()) {
            for (int i = 0; i < config.getIndexPKCols().size(); i++) {
                if (i > 0) queryBuilder.append(", ");
                queryBuilder.append(CommonServiceUtils.getColumnExprFromPCol(config.getIndexPKCols().get(i), true));
            }
            for (PColumn tablePkCol : config.getTablePKCols()) {
                queryBuilder.append(", ");
                queryBuilder.append(CommonServiceUtils.getEscapedArgument(tablePkCol.getName().toString()));
            }
        } else {
            for (int i = 0; i < config.getTablePKCols().size(); i++) {
                if (i > 0) queryBuilder.append(", ");
                queryBuilder.append(CommonServiceUtils.getEscapedArgument(config.getTablePKCols().get(i).getName().toString()));
            }
        }
    }

    /**
     * Set all parameters on the PreparedStatement based on scan type
     */
    private static void setQueryParameters(PreparedStatement stmt, Map<String, Object> request,
                                         ScanConfig config) throws SQLException {

        int paramIndex = 1;
        if (config.getType() != ScanType.NO_EXCLUSIVE_START_KEY) {
            Map<String, Object> exclusiveStartKey =
                    (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);

            if (config.useIndex()) {
                for (PColumn indexPkCol : config.getIndexPKCols()) {
                    String keyName = CommonServiceUtils.getColumnNameFromPCol(indexPkCol, true);
                    DQLUtils.setKeyValueOnStatement(stmt, paramIndex++,
                            (Map<String, Object>) exclusiveStartKey.get(keyName), false);
                }
            }
            for (PColumn tablePkCol : config.getTablePKCols()) {
                String keyName = tablePkCol.getName().getString();
                DQLUtils.setKeyValueOnStatement(stmt, paramIndex++,
                        (Map<String, Object>) exclusiveStartKey.get(keyName), false);
            }
        }

        // Set segment boundary parameters if this is a segment scan
        if (config.isSegmentScan()) {
            byte[] startKey = config.getScanSegmentInfo().getStartKey();
            byte[] endKey = config.getScanSegmentInfo().getEndKey();
            stmt.setBytes(paramIndex++, startKey);
            stmt.setBytes(paramIndex++, endKey);
        }
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
    public static boolean isSegmentScanRequest(Map<String, Object> request) {
        return request.get(ApiMetadata.SEGMENT) != null
                && request.get(ApiMetadata.TOTAL_SEGMENTS) != null;
    }

    /**
     * Get segment info for segment scan
     */
    private static ScanSegmentInfo getSegmentInfo(Connection connection,
            Map<String, Object> request) throws SQLException {
        Integer segment = (Integer) request.get(ApiMetadata.SEGMENT);
        Integer totalSegments = (Integer) request.get(ApiMetadata.TOTAL_SEGMENTS);
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        Map<String, Object> exclusiveStartKey =
                (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);

        // Get segment boundaries using SegmentScanUtil
        if (exclusiveStartKey == null || exclusiveStartKey.isEmpty()) {
            // First page - generate and get segment boundaries
            return SegmentScanUtil.updateAndGetSegmentScanRange(connection, tableName,
                    indexName, totalSegments, segment);
        } else {
            return SegmentScanUtil.getSegmentScanRange(connection, tableName, indexName,
                    totalSegments, segment);
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
}
