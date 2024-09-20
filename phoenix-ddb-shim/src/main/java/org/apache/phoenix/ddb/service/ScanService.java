package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
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

    public static ScanResult scan(ScanRequest request, String connectionUrl) {
        String tableName = request.getTableName();
        String indexName = request.getIndexName();
        boolean useIndex = !StringUtils.isEmpty(indexName);
        List<PColumn> tablePKCols, indexPKCols = null;
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            if (useIndex) {
                indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            }
            PreparedStatement stmt = getPreparedStatement(connection, request, useIndex, tablePKCols, indexPKCols);
            return (ScanResult) DQLUtils.executeStatementReturnResult(false, stmt,
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
        String tableName = request.getTableName();
        String indexName = request.getIndexName();
        Map<String, String> exprAttrNames =  request.getExpressionAttributeNames();
        Map<String, AttributeValue> exprAttrValues =  request.getExpressionAttributeValues();

        StringBuilder queryBuilder = StringUtils.isEmpty(indexName)
                ? new StringBuilder(String.format(SELECT_QUERY, tableName))
                : new StringBuilder(String.format(SELECT_QUERY_WITH_INDEX_HINT,
                tableName, indexName, tableName));
        String filterExpr = request.getFilterExpression();
        if (!StringUtils.isEmpty(filterExpr)) {
            queryBuilder.append(" WHERE ");
            DQLUtils.addFilterCondition(false, queryBuilder,
                    request.getFilterExpression(), exprAttrNames, exprAttrValues);
        }
        DQLUtils.addExclusiveStartKeyCondition(false, queryBuilder,
                request.getExclusiveStartKey(), useIndex, partitionKeyPKCol, sortKeyPKCol);
        DQLUtils.addLimit(queryBuilder, request.getLimit(), MAX_SCAN_LIMIT);
        //TODO : extract PKs from filterExpression and append to WHERE clause
        LOGGER.info("SELECT Query: " + queryBuilder);

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
        int index = 1;
        Map<String, AttributeValue> exclusiveStartKey =  request.getExclusiveStartKey();
        if (exclusiveStartKey != null) {
            String name;
            if (sortKeyPKCol != null) {
                name = sortKeyPKCol.getName().toString();
                name =  (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
                DQLUtils.setKeyValueOnStatement(stmt, index++, exclusiveStartKey.get(name), false);
            }
            name = partitionKeyPKCol.getName().toString();
            name =  (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
            DQLUtils.setKeyValueOnStatement(stmt, index, exclusiveStartKey.get(name), false);
        }
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(ScanRequest request) {
        List<String> attributesToGet = request.getAttributesToGet();
        String projExpr = request.getProjectionExpression();
        Map<String, String> exprAttrNames = request.getExpressionAttributeNames();
        return DQLUtils.getProjectionAttributes(attributesToGet, projExpr, exprAttrNames);
    }
}
