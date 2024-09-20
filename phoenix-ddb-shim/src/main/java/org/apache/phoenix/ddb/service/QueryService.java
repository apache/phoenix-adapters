package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.DQLUtils;
import org.apache.phoenix.ddb.utils.KeyConditionsHolder;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class QueryService {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryService.class);
    private static final String SELECT_QUERY = "SELECT COL FROM %s WHERE ";
    private static final String SELECT_QUERY_WITH_INDEX_HINT
            = "SELECT /*+ INDEX(%s %s) */ COL FROM %s WHERE ";

    private static final int MAX_QUERY_LIMIT = 500;

    public static QueryResult query(QueryRequest request, String connectionUrl)  {
        String tableName = request.getTableName();
        String indexName = request.getIndexName();
        boolean useIndex = !StringUtils.isEmpty(indexName);
        List<PColumn> tablePKCols, indexPKCols = null;
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            // get PKs from phoenix
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            if (useIndex) {
                indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            }

            // build PreparedStatement and execute
            PreparedStatement stmt
                    = getPreparedStatement(connection, request, useIndex, tablePKCols, indexPKCols);
            return (QueryResult) DQLUtils.executeStatementReturnResult(true, stmt,
                    getProjectionAttributes(request), useIndex, tablePKCols, indexPKCols);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection conn, QueryRequest request,
                          boolean useIndex, List<PColumn> tablePKCols, List<PColumn> indexPKCols)
            throws SQLException {
        String tableName = request.getTableName();
        String indexName = request.getIndexName();

        Map<String, String> exprAttrNames = request.getExpressionAttributeNames();
        Map<String, AttributeValue> exprAttrValues = request.getExpressionAttributeValues();
        String keyCondExpr = request.getKeyConditionExpression();

        // build SQL query
        StringBuilder queryBuilder = StringUtils.isEmpty(indexName)
                ? new StringBuilder(String.format(SELECT_QUERY, tableName))
                : new StringBuilder(String.format(SELECT_QUERY_WITH_INDEX_HINT,
                                                    tableName, indexName, tableName));

        // parse Key Conditions
        KeyConditionsHolder keyConditions
                = new KeyConditionsHolder(keyCondExpr, exprAttrNames,
                                            useIndex ? indexPKCols : tablePKCols, useIndex);
        PColumn sortKeyPKCol = keyConditions.getSortKeyPKCol();
        PColumn partitionKeyPKCol = keyConditions.getPartitionKeyPKCol();

        // append all conditions for WHERE clause
        queryBuilder.append(keyConditions.getSQLWhereClause());
        DQLUtils.addExclusiveStartKeyCondition(true, queryBuilder,
                request.getExclusiveStartKey(), useIndex, partitionKeyPKCol, sortKeyPKCol);
        DQLUtils.addFilterCondition(true, queryBuilder,
                request.getFilterExpression(), exprAttrNames, exprAttrValues);
        addScanIndexForwardCondition(queryBuilder, request, useIndex, sortKeyPKCol);
        DQLUtils.addLimit(queryBuilder, request.getLimit(), MAX_QUERY_LIMIT);
        LOGGER.info("SELECT Query: " + queryBuilder);

        // Set values on the PreparedStatement
        PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, keyConditions, useIndex, sortKeyPKCol);
        return stmt;
    }

    /**
     * If the QueryRequest has ScanIndexForward set to False and there is a sortKey,
     * add an ORDER BY sortKey DESC clause to the query.
     * When using an index, use the BSON_VALUE expression.
     */
    private static void addScanIndexForwardCondition(StringBuilder queryBuilder,
                                 QueryRequest request, boolean useIndex, PColumn sortKeyPKCol) {
        Boolean scanIndexForward = request.getScanIndexForward();
        if (scanIndexForward != null && !scanIndexForward && sortKeyPKCol != null) {
            String name = sortKeyPKCol.getName().getString();
            name =  (useIndex)
                    ? name.substring(1)
                    : CommonServiceUtils.getEscapedArgument(name);
            queryBuilder.append(" ORDER BY " + name + " DESC ");
        }
    }

    /**
     * Set all the values on the PreparedStatement:
     * - 1 value for partitionKey,
     * - 1 or 2 values for sortKey, if present
     * - 1 value for ExclusiveStartKey's sortKey, if present.
     */
    private static void setPreparedStatementValues(PreparedStatement stmt, QueryRequest request,
                                                  KeyConditionsHolder keyConditions,
                                                   boolean useIndex, PColumn sortKeyPKCol)
            throws SQLException {
        int index = 1;
        Map<String, AttributeValue> exclusiveStartKey =  request.getExclusiveStartKey();
        Map<String, AttributeValue> exprAttrVals =  request.getExpressionAttributeValues();
        AttributeValue partitionAttrVal = exprAttrVals.get(keyConditions.getPartitionValue());
        DQLUtils.setKeyValueOnStatement(stmt, index++, partitionAttrVal, false);
        if (keyConditions.hasSortKey()) {
            if (keyConditions.hasBeginsWith()) {
                AttributeValue sortAttrVal = exprAttrVals.get(keyConditions.getBeginsWithSortKeyVal());
                DQLUtils.setKeyValueOnStatement(stmt, index++, sortAttrVal, true);
            } else {
                AttributeValue sortAttrVal1 = exprAttrVals.get(keyConditions.getSortKeyValue1());
                DQLUtils.setKeyValueOnStatement(stmt, index++, sortAttrVal1, false);
                if (keyConditions.hasBetween()) {
                    AttributeValue sortAttrVal2 = exprAttrVals.get(keyConditions.getSortKeyValue2());
                    DQLUtils.setKeyValueOnStatement(stmt, index++, sortAttrVal2, false);
                }
            }
        }
        if (exclusiveStartKey != null && sortKeyPKCol != null) {
            String name = sortKeyPKCol.getName().toString();
            name =  (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
            DQLUtils.setKeyValueOnStatement(stmt, index, exclusiveStartKey.get(name), false);
        }
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(QueryRequest request) {
        List<String> attributesToGet = request.getAttributesToGet();
        String projExpr = request.getProjectionExpression();
        Map<String, String> exprAttrNames = request.getExpressionAttributeNames();
        return DQLUtils.getProjectionAttributes(attributesToGet, projExpr, exprAttrNames);
    }
}
