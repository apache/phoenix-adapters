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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.service.utils.DQLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.KeyConditionsHolder;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;

public class QueryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryService.class);

    public static final String SELECT_QUERY = "SELECT COL FROM %s WHERE ";
    public static final String SELECT_QUERY_WITH_INDEX_HINT =
            "SELECT /*+ INDEX(\"%s.%s\" \"%s\") */ COL FROM %s WHERE ";

    private static final int MAX_QUERY_LIMIT = 100;

    public static Map<String, Object> query(Map<String, Object> request, String connectionUrl) {
        ValidationUtil.validateQueryRequest(request);
        handleLegacyParamsConversion(request);
        CommonServiceUtils.handleLegacyProjectionConversion(request);
        
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        boolean useIndex = !StringUtils.isEmpty(indexName);
        List<PColumn> tablePKCols;
        List<PColumn> indexPKCols = null;
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            // get PKs from phoenix
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            if (useIndex) {
                indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            }

            // build PreparedStatement and execute
            Pair<PreparedStatement, Boolean> pairVal =
                    getPreparedStatement(connection, request, useIndex, tablePKCols, indexPKCols);
            PreparedStatement stmt = pairVal.getFirst();
            boolean isSingleRowExpected = pairVal.getSecond();
            boolean countOnly = ApiMetadata.SELECT_COUNT.equals(request.get(ApiMetadata.SELECT));
            return DQLUtils.executeStatementReturnResult(stmt,
                    getProjectionAttributes(request), useIndex, tablePKCols, indexPKCols, tableName,
                    isSingleRowExpected, false, countOnly);
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
    }

    /**
     * Builds and returns a PreparedStatement for executing the Query operation.
     *
     * @param conn The database connection to use
     * @param request The query request parameters
     * @param useIndex Whether to use a secondary index for the query
     * @param tablePKCols List of primary key columns for the table
     * @param indexPKCols List of primary key columns for the index (if using an index)
     * @return A Pair containing:
     *         - The prepared SQL statement with all conditions and values set
     *         - A boolean indicating if a single row is expected
     * @throws SQLException If there is an error preparing the statement
     */
    public static Pair<PreparedStatement, Boolean> getPreparedStatement(Connection conn,
            Map<String, Object> request, boolean useIndex, List<PColumn> tablePKCols,
            List<PColumn> indexPKCols) throws SQLException {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);

        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrValues =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);
        String keyCondExpr = (String) request.get(ApiMetadata.KEY_CONDITION_EXPRESSION);

        // build SQL query
        StringBuilder queryBuilder = StringUtils.isEmpty(indexName) ?
                new StringBuilder(String.format(SELECT_QUERY, PhoenixUtils.getFullTableName(tableName, true))) :
                new StringBuilder(String.format(SELECT_QUERY_WITH_INDEX_HINT, PhoenixUtils.SCHEMA_NAME,
                        tableName, indexName,  PhoenixUtils.getFullTableName(tableName, true)));

        // parse Key Conditions
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames,
                useIndex ? indexPKCols : tablePKCols, useIndex);
        PColumn sortKeyPKCol = keyConditions.getSortKeyPKCol();

        // append all conditions for WHERE clause
        // TODO: Validate exclusiveStartKey against sortKey range in key condition expression
        queryBuilder.append(keyConditions.getSQLWhereClause());
        boolean scanIndexForward = doScanIndexForward(request);
        DQLUtils.addExclusiveStartKeyConditionForQuery(queryBuilder,
                (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY), useIndex,
                sortKeyPKCol, scanIndexForward);
        DQLUtils.addFilterCondition(true, queryBuilder, (String) request.get(ApiMetadata.FILTER_EXPRESSION),
                exprAttrNames, exprAttrValues);
        addOrderByClause(queryBuilder, useIndex, tablePKCols, indexPKCols, scanIndexForward);
        DQLUtils.addLimit(queryBuilder, (Integer) request.get(ApiMetadata.LIMIT), MAX_QUERY_LIMIT);
        LOGGER.debug("SELECT Query: " + queryBuilder);

        // Set values on the PreparedStatement
        PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, keyConditions, useIndex, sortKeyPKCol);
        return Pair.newPair(stmt, sortKeyPKCol == null);
    }

    private static void addOrderByClause(StringBuilder queryBuilder, boolean useIndex,
            List<PColumn> tablePKCols, List<PColumn> indexPKCols, boolean scanIndexForward) {
        List<PColumn> relevantPKCols = useIndex ? indexPKCols : tablePKCols;
        PColumn partitionKeyCol = relevantPKCols.get(0);
        PColumn sortKeyCol = (relevantPKCols.size() > 1) ? relevantPKCols.get(1) : null;

        String partitionKeyName =
                CommonServiceUtils.getColumnExprFromPCol(partitionKeyCol, useIndex);
        queryBuilder.append(" ORDER BY ").append(partitionKeyName).append(" ASC");
        if (sortKeyCol != null) {
            String sortKeyName = CommonServiceUtils.getColumnExprFromPCol(sortKeyCol, useIndex);
            queryBuilder.append(", ").append(sortKeyName)
                    .append(scanIndexForward ? " ASC " : " DESC ");
        }
    }

    /**
     * Set all the values on the PreparedStatement:
     * - 1 value for partitionKey,
     * - 1 or 2 values for sortKey, if present
     * - 1 value for ExclusiveStartKey's sortKey, if present.
     */
    private static void setPreparedStatementValues(PreparedStatement stmt,
            Map<String, Object> request, KeyConditionsHolder keyConditions, boolean useIndex,
            PColumn sortKeyPKCol) throws SQLException {
        int index = 1;
        Map<String, Object> exclusiveStartKey =
                (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);
        Map<String, Object> partitionAttrVal =
                (Map<String, Object>) exprAttrVals.get(keyConditions.getPartitionValue());
        DQLUtils.setKeyValueOnStatement(stmt, index++, partitionAttrVal, false);
        if (keyConditions.hasSortKey()) {
            if (keyConditions.hasBeginsWith()) {
                Map<String, Object> sortAttrVal = (Map<String, Object>) exprAttrVals.get(
                        keyConditions.getBeginsWithSortKeyVal());
                DQLUtils.setKeyValueOnStatement(stmt, index++, sortAttrVal, true);
                index++; // we set 2 parameters for SUBSTR/SUBBINARY
            } else {
                Map<String, Object> sortAttrVal1 =
                        (Map<String, Object>) exprAttrVals.get(keyConditions.getSortKeyValue1());
                DQLUtils.setKeyValueOnStatement(stmt, index++, sortAttrVal1, false);
                if (keyConditions.hasBetween()) {
                    Map<String, Object> sortAttrVal2 = (Map<String, Object>) exprAttrVals.get(
                            keyConditions.getSortKeyValue2());
                    DQLUtils.setKeyValueOnStatement(stmt, index++, sortAttrVal2, false);
                }
            }
        }
        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty() && sortKeyPKCol != null) {
            String sortKeyName = CommonServiceUtils.getColumnNameFromPCol(sortKeyPKCol, useIndex);
            DQLUtils.setKeyValueOnStatement(stmt, index,
                    (Map<String, Object>) exclusiveStartKey.get(sortKeyName), false);
        }
    }

    /**
     * Return a list of attribute names to project.
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

    private static boolean doScanIndexForward(Map<String, Object> request) {
        Boolean scanIndexForward = (Boolean) request.get(ApiMetadata.SCAN_INDEX_FORWARD);
        return scanIndexForward == null || scanIndexForward;
    }

    /**
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

        // Track counters to avoid conflicts between KeyConditions and QueryFilter conversions
        int nameCounter = 1;
        int valueCounter = 1;

        Map<String, Object> keyConditions =
                (Map<String, Object>) request.get(ApiMetadata.KEY_CONDITIONS);
        if (keyConditions != null) {
            Map<String, Object> orderedKeyConditions = reorderKeyConditions(keyConditions);
            String keyConditionExpression =
                    CommonServiceUtils.convertExpectedToConditionExpression(orderedKeyConditions,
                            "AND", exprAttrNames, exprAttrValues, nameCounter, valueCounter);
            if (keyConditionExpression != null) {
                request.put(ApiMetadata.KEY_CONDITION_EXPRESSION, keyConditionExpression);
            }
            request.remove(ApiMetadata.KEY_CONDITIONS);

            nameCounter += CommonServiceUtils.getExpectedNameCount(orderedKeyConditions) + 10;
            valueCounter += nameCounter * 5;
        }

        Map<String, Object> queryFilter =
                (Map<String, Object>) request.get(ApiMetadata.QUERY_FILTER);
        if (queryFilter != null) {
            String conditionalOperator = (String) request.get(ApiMetadata.CONDITIONAL_OPERATOR);
            if (conditionalOperator == null) {
                conditionalOperator = "AND";
            }
            String filterExpression =
                    CommonServiceUtils.convertExpectedToConditionExpression(queryFilter,
                            conditionalOperator, exprAttrNames, exprAttrValues, nameCounter,
                            valueCounter);
            if (filterExpression != null) {
                request.put(ApiMetadata.FILTER_EXPRESSION, filterExpression);
            }
            request.remove(ApiMetadata.QUERY_FILTER);
            request.remove(ApiMetadata.CONDITIONAL_OPERATOR);
        }
    }

    /**
     * Reorders KeyConditions to ensure hash key (partition key) comes first, then sort key.
     *
     * @param keyConditions The original KeyConditions map
     * @return Reordered map with hash key first, sort key second
     */
    private static Map<String, Object> reorderKeyConditions(Map<String, Object> keyConditions) {
        if (keyConditions.size() <= 1) {
            return keyConditions;
        }
        Map<String, Object> orderedConditions = new LinkedHashMap<>();
        String hashKeyName = null;
        Object hashKeyCondition = null;
        String sortKeyName = null;
        Object sortKeyCondition = null;

        for (Map.Entry<String, Object> entry : keyConditions.entrySet()) {
            String keyName = entry.getKey();
            Map<String, Object> condition = (Map<String, Object>) entry.getValue();
            String operator = (String) condition.get("ComparisonOperator");
            if ("EQ".equals(operator)) {
                hashKeyName = keyName;
                hashKeyCondition = condition;
            } else {
                sortKeyName = keyName;
                sortKeyCondition = condition;
            }
        }
        if (hashKeyName != null) {
            orderedConditions.put(hashKeyName, hashKeyCondition);
        }
        if (sortKeyName != null) {
            orderedConditions.put(sortKeyName, sortKeyCondition);
        }
        return orderedConditions;
    }
}
