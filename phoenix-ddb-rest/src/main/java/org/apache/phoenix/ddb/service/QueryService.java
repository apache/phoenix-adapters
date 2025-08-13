package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.util.Pair;
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

    public static final String SELECT_QUERY = "SELECT COL FROM %s.\"%s\" WHERE ";
    public static final String SELECT_QUERY_WITH_INDEX_HINT =
            "SELECT /*+ INDEX(\"%s.%s\" \"%s\") */ COL FROM %s.\"%s\" WHERE ";

    private static final int MAX_QUERY_LIMIT = 100;

    public static Map<String, Object> query(Map<String, Object> request, String connectionUrl) {
        ValidationUtil.validateQueryRequest(request);
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        boolean useIndex = !StringUtils.isEmpty(indexName);
        List<PColumn> tablePKCols, indexPKCols = null;
        try (Connection connection = DriverManager.getConnection(connectionUrl,
                DQLUtils.getConnectionProps())) {
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
            return DQLUtils.executeStatementReturnResult(stmt,
                    getProjectionAttributes(request), useIndex, tablePKCols, indexPKCols, tableName,
                    isSingleRowExpected, false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
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
                new StringBuilder(String.format(SELECT_QUERY, "DDB", tableName)) :
                new StringBuilder(String.format(SELECT_QUERY_WITH_INDEX_HINT, "DDB",
                        tableName, indexName, "DDB", tableName));

        // parse Key Conditions
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames,
                useIndex ? indexPKCols : tablePKCols, useIndex);
        PColumn sortKeyPKCol = keyConditions.getSortKeyPKCol();
        PColumn partitionKeyPKCol = keyConditions.getPartitionKeyPKCol();

        // append all conditions for WHERE clause
        // TODO: Validate exclusiveStartKey against sortKey range in key condition expression
        queryBuilder.append(keyConditions.getSQLWhereClause());
        boolean scanIndexForward = doScanIndexForward(request);
        DQLUtils.addExclusiveStartKeyConditionForQuery(queryBuilder,
                (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY), useIndex,
                sortKeyPKCol, scanIndexForward);
        DQLUtils.addFilterCondition(true, queryBuilder, (String) request.get(ApiMetadata.FILTER_EXPRESSION),
                exprAttrNames, exprAttrValues);
        if (!scanIndexForward && sortKeyPKCol != null) {
            addScanIndexForwardCondition(queryBuilder, useIndex, sortKeyPKCol);
        }
        DQLUtils.addLimit(queryBuilder, (Integer) request.get(ApiMetadata.LIMIT), MAX_QUERY_LIMIT);
        LOGGER.debug("SELECT Query: " + queryBuilder);

        // Set values on the PreparedStatement
        PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, keyConditions, useIndex, sortKeyPKCol);
        return Pair.newPair(stmt, sortKeyPKCol == null);
    }

    /**
     * If the QueryRequest has ScanIndexForward set to False and there is a sortKey,
     * add an ORDER BY sortKey DESC clause to the query.
     * When using an index, use the BSON_VALUE expression.
     */
    private static void addScanIndexForwardCondition(StringBuilder queryBuilder, boolean useIndex, PColumn sortKeyPKCol) {
        String name = sortKeyPKCol.getName().getString();
        name = (useIndex) ? name.substring(1) : CommonServiceUtils.getEscapedArgument(name);
        queryBuilder.append(" ORDER BY ").append(name).append(" DESC ");
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
            String name = sortKeyPKCol.getName().toString();
            name = (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
            DQLUtils.setKeyValueOnStatement(stmt, index,
                    (Map<String, Object>) exclusiveStartKey.get(name), false);
        }
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(Map<String, Object> request) {
        String projExpr = (String) request.get(ApiMetadata.PROJECTION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        return DQLUtils.getProjectionAttributes(projExpr, exprAttrNames);
    }

    private static boolean doScanIndexForward(Map<String, Object> request) {
        Boolean scanIndexForward = (Boolean) request.get(ApiMetadata.SCAN_INDEX_FORWARD);
        return scanIndexForward == null || scanIndexForward;
    }
}
