package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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

    private static final int MAX_SCAN_LIMIT = 500;

    public static Map<String, Object> scan(Map<String, Object> request, String connectionUrl) {
        ValidationUtil.validateScanRequest(request);
        // phoenix does not support parallel scans from the client so
        // we will return all items in the first segment and no items in all other segments
        if (request.get(ApiMetadata.SEGMENT) != null && (Integer) request.get(ApiMetadata.SEGMENT) > 0) {
            return Collections.emptyMap();
        }
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        boolean useIndex = !StringUtils.isEmpty(indexName);
        List<PColumn> tablePKCols, indexPKCols = null;
        try (Connection connection = DriverManager.getConnection(connectionUrl,
                DQLUtils.getConnectionProps())) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            if (useIndex) {
                indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            }
            PreparedStatement stmt =
                    getPreparedStatement(connection, request, useIndex, tablePKCols, indexPKCols);
            return DQLUtils.executeStatementReturnResult(false, stmt,
                    getProjectionAttributes(request), useIndex, tablePKCols, indexPKCols,
                    tableName, false);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Build the SELECT query based on the scan request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection connection,
            Map<String, Object> request, boolean useIndex, List<PColumn> tablePKCols,
            List<PColumn> indexPKCols) throws SQLException {
        PColumn partitionKeyPKCol = (useIndex) ? indexPKCols.get(0) : tablePKCols.get(0);
        PColumn sortKeyPKCol = (tablePKCols.size() == 2) ? tablePKCols.get(1) : null;
        if (useIndex) {
            sortKeyPKCol = (indexPKCols.size() == 2) ? indexPKCols.get(1) : null;
        }
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrValues =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);

        StringBuilder queryBuilder = StringUtils.isEmpty(indexName) ?
                new StringBuilder(String.format(SELECT_QUERY, "DDB", tableName)) :
                new StringBuilder(
                        String.format(SELECT_QUERY_WITH_INDEX_HINT, "DDB", tableName, indexName,
                                "DDB", tableName));
        String filterExpr = (String) request.get(ApiMetadata.FILTER_EXPRESSION);
        Map<String, Object> exclusiveStartKey =
                (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
        if (!StringUtils.isEmpty(filterExpr) || (exclusiveStartKey != null
                && !exclusiveStartKey.isEmpty())) {
            queryBuilder.append(" WHERE ");
        }
        DQLUtils.addFilterCondition(false, queryBuilder, (String) request.get(ApiMetadata.FILTER_EXPRESSION),
                exprAttrNames, exprAttrValues);
        DQLUtils.addExclusiveStartKeyCondition(false, !StringUtils.isEmpty(filterExpr),
                queryBuilder, exclusiveStartKey, useIndex, partitionKeyPKCol, sortKeyPKCol, false);
        DQLUtils.addLimit(queryBuilder, (Integer) request.get(ApiMetadata.LIMIT), MAX_SCAN_LIMIT);
        //TODO : extract PKs from filterExpression and append to WHERE clause
        LOGGER.info("Query for Scan: " + queryBuilder);

        PreparedStatement stmt = connection.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, useIndex, partitionKeyPKCol, sortKeyPKCol);
        return stmt;
    }

    /**
     * Set all the required values on the PreparedStatement.
     */
    private static void setPreparedStatementValues(PreparedStatement stmt,
            Map<String, Object> request, boolean useIndex, PColumn partitionKeyPKCol,
            PColumn sortKeyPKCol) throws SQLException {
        Map<String, Object> exclusiveStartKey =
                (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
            String name = partitionKeyPKCol.getName().toString();
            name = (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
            DQLUtils.setKeyValueOnStatement(stmt, 1,
                    (Map<String, Object>) exclusiveStartKey.get(name), false);
            if (sortKeyPKCol != null) {
                DQLUtils.setKeyValueOnStatement(stmt, 3,
                        (Map<String, Object>) exclusiveStartKey.get(name), false);
                name = sortKeyPKCol.getName().toString();
                name = (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
                DQLUtils.setKeyValueOnStatement(stmt, 2,
                        (Map<String, Object>) exclusiveStartKey.get(name), false);
            }
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
}
