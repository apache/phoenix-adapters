package org.apache.phoenix.ddb.service;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.DQLUtils;
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

public class ScanService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanService.class);
    private static final String SELECT_QUERY = "SELECT COL FROM %s ";

    private static final String SELECT_QUERY_WITH_INDEX_HINT
            = "SELECT /*+ INDEX(%s %s) */ COL FROM %s ";

    private static final int MAX_SCAN_LIMIT = 500;

    public static ScanResponse scan(ScanRequest request, String connectionUrl) {
        String tableName = request.tableName();
        String indexName = request.indexName();
        boolean useIndex = !StringUtils.isEmpty(indexName);
        List<PColumn> tablePKCols, indexPKCols = null;
        try (Connection connection = DriverManager.getConnection(connectionUrl,
                DQLUtils.getConnectionProps())) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            if (useIndex) {
                indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            }
            PreparedStatement stmt
                    = getPreparedStatement(connection, request, useIndex, tablePKCols, indexPKCols);
            return (ScanResponse) DQLUtils.executeStatementReturnResult(false, stmt,
                    getProjectionAttributes(request), useIndex, tablePKCols, indexPKCols);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the SELECT query based on the scan request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection connection,
                                                          ScanRequest request,
                                                          boolean useIndex,
                                                          List<PColumn> tablePKCols,
                                                          List<PColumn> indexPKCols)
            throws SQLException {
        PColumn partitionKeyPKCol = (useIndex) ? indexPKCols.get(0) : tablePKCols.get(0);
        PColumn sortKeyPKCol = (tablePKCols.size()==2) ? tablePKCols.get(1) : null;
        if (useIndex) {
            sortKeyPKCol = (indexPKCols.size()==2) ? indexPKCols.get(1) : null;
        }
        String tableName = request.tableName();
        String indexName = request.indexName();
        Map<String, String> exprAttrNames =  request.expressionAttributeNames();
        Map<String, AttributeValue> exprAttrValues =  request.expressionAttributeValues();

        StringBuilder queryBuilder = StringUtils.isEmpty(indexName)
                ? new StringBuilder(String.format(SELECT_QUERY, tableName))
                : new StringBuilder(String.format(SELECT_QUERY_WITH_INDEX_HINT,
                tableName, indexName, tableName));
        String filterExpr = request.filterExpression();
        Map<String, AttributeValue> exclusiveStartKey =  request.exclusiveStartKey();
        if (!StringUtils.isEmpty(filterExpr) ||
                (exclusiveStartKey != null && !exclusiveStartKey.isEmpty())) {
            queryBuilder.append(" WHERE ");
        }
        DQLUtils.addFilterCondition(false, queryBuilder,
                request.filterExpression(), exprAttrNames, exprAttrValues);
        DQLUtils.addExclusiveStartKeyCondition(false, !StringUtils.isEmpty(filterExpr),
                queryBuilder, exclusiveStartKey, useIndex, partitionKeyPKCol, sortKeyPKCol);
        DQLUtils.addLimit(queryBuilder, request.limit(), MAX_SCAN_LIMIT);
        //TODO : extract PKs from filterExpression and append to WHERE clause
        LOGGER.info("Query for Scan: " + queryBuilder);

        PreparedStatement stmt = connection.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, useIndex, partitionKeyPKCol, sortKeyPKCol);
        return stmt;
    }

    /**
     * Set all the required values on the PreparedStatement.
     */
    private static void setPreparedStatementValues(PreparedStatement stmt, ScanRequest request,
                                                   boolean useIndex,  PColumn partitionKeyPKCol,
                                                   PColumn sortKeyPKCol) throws SQLException {
        Map<String, AttributeValue> exclusiveStartKey =  request.exclusiveStartKey();
        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
            String name = partitionKeyPKCol.getName().toString();
            name =  (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
            DQLUtils.setKeyValueOnStatement(stmt, 1, exclusiveStartKey.get(name), false);
            if (sortKeyPKCol != null) {
                DQLUtils.setKeyValueOnStatement(stmt, 3, exclusiveStartKey.get(name), false);
                name = sortKeyPKCol.getName().toString();
                name =  (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
                DQLUtils.setKeyValueOnStatement(stmt, 2, exclusiveStartKey.get(name), false);
            }
        }
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(ScanRequest request) {
        List<String> attributesToGet = request.attributesToGet();
        String projExpr = request.projectionExpression();
        Map<String, String> exprAttrNames = request.expressionAttributeNames();
        return DQLUtils.getProjectionAttributes(attributesToGet, projExpr, exprAttrNames);
    }
}
