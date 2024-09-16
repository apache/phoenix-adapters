package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.utils.KeyConditionsHolder;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.ddb.service.CommonServiceUtils.DOUBLE_QUOTE;

public class QueryUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryUtils.class);
    private static final String SELECT_QUERY = "SELECT COL FROM %s WHERE ";
    private static final String SELECT_QUERY_WITH_INDEX_HINT
            = "SELECT /*+ INDEX(%s %s) */ COL FROM %s WHERE ";

    private static final int MAX_LIMIT = 500;

    public static QueryResult query(QueryRequest request, String connectionUrl)  {
        int count = 0;
        List<Map<String, AttributeValue>> items = new ArrayList<>();

        String tableName = request.getTableName();
        String indexName = request.getIndexName();
        boolean useIndex = !StringUtils.isEmpty(indexName);
        RawBsonDocument lastBsonDoc = null;
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
            LOGGER.info("SELECT Query: " + stmt);
            ResultSet rs  = stmt.executeQuery();
            while (rs.next()) {
                lastBsonDoc = (RawBsonDocument) rs.getObject(1);
                Map<String, AttributeValue> item = BsonDocumentToDdbAttributes.getProjectedItem(
                        lastBsonDoc, getProjectionAttributes(request));
                items.add(item);
                count++;
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return new QueryResult().withItems(items).withCount(count)
                .withLastEvaluatedKey(count == 0
                        ? null
                        : getLastEvaluatedKey(lastBsonDoc, useIndex, tablePKCols, indexPKCols));
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

        Map<String, String> exprAttrNames =  request.getExpressionAttributeNames();
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

        // append all conditions for WHERE clause
        queryBuilder.append(keyConditions.getSQLWhereClause());
        addExclusiveStartKeyCondition(queryBuilder, request, useIndex, sortKeyPKCol);
        addFilterCondition(queryBuilder, request);
        addScanIndexForwardCondition(queryBuilder, request, useIndex, sortKeyPKCol);
        addLimit(queryBuilder, request);

        // Set values on the PreparedStatement
        PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, keyConditions, useIndex, sortKeyPKCol);
        return stmt;
    }

    /**
     * If table has a sortKey and the QueryRequest provides an ExclusiveStartKey,
     * add the condition for the sortKey to the query. If the request provides an index,
     * replace sortKey name with a BSON_VALUE expression.
     * Return the sortKeyName here in case the QueryRequest's KeyConditionExpression
     * did not have a condition on the sortKey.
     */
    private static PColumn addExclusiveStartKeyCondition(StringBuilder queryBuilder,
                                                        QueryRequest request,
                                                        boolean useIndex,
                                                        PColumn sortKeyPKCol) {
        Map<String, AttributeValue> exclusiveStartKey = request.getExclusiveStartKey();
        if (exclusiveStartKey != null && sortKeyPKCol != null) {
            String name = sortKeyPKCol.getName().toString();
            name =  (useIndex)
                    ? name.substring(1)
                    : DOUBLE_QUOTE + name + DOUBLE_QUOTE;
            queryBuilder.append(" AND " + name + " > ? ");
        }
        return sortKeyPKCol;
    }

    /**
     * If the QueryRequest has a FilterExpression for non-pk columns,
     * add BSON_CONDITION_EXPRESSION to the query.
     */
    private static void addFilterCondition(StringBuilder queryBuilder, QueryRequest request) {
        String filterExpr = request.getFilterExpression();
        if (!StringUtils.isEmpty(filterExpr)) {
            Map<String, String> exprAttrNames =  request.getExpressionAttributeNames();
            Map<String, AttributeValue> exprAttrVals =  request.getExpressionAttributeValues();
            String bsonCondExpr = CommonServiceUtils
                    .getBsonConditionExpression(filterExpr, exprAttrNames, exprAttrVals);
            queryBuilder.append(" AND BSON_CONDITION_EXPRESSION(COL, '");
            queryBuilder.append(bsonCondExpr);
            queryBuilder.append("')" );
        }
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
                    : DOUBLE_QUOTE + name + DOUBLE_QUOTE;
            queryBuilder.append(" ORDER BY " + name + " DESC ");
        }
    }

    /**
     * Add a LIMIT clause to the query if QueryRequest has a Limit.
     */
    private static void addLimit(StringBuilder queryBuilder, QueryRequest request) {
        Integer limit = request.getLimit();
        limit = (limit == null) ? MAX_LIMIT : Math.min(limit, MAX_LIMIT);
        queryBuilder.append(" LIMIT " + limit);
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
        setKeyValueOnStatement(stmt, index++, partitionAttrVal, false);
        if (keyConditions.hasSortKey()) {
            if (keyConditions.hasBeginsWith()) {
                AttributeValue sortAttrVal = exprAttrVals.get(keyConditions.getBeginsWithSortKeyVal());
                setKeyValueOnStatement(stmt, index++, sortAttrVal, true);
            } else {
                AttributeValue sortAttrVal1 = exprAttrVals.get(keyConditions.getSortKeyValue1());
                setKeyValueOnStatement(stmt, index++, sortAttrVal1, false);
                if (keyConditions.hasBetween()) {
                    AttributeValue sortAttrVal2 = exprAttrVals.get(keyConditions.getSortKeyValue2());
                    setKeyValueOnStatement(stmt, index++, sortAttrVal2, false);
                }
            }
        }
        if (exclusiveStartKey != null && sortKeyPKCol != null) {
            String name = sortKeyPKCol.getName().toString();
            name =  (useIndex) ? CommonServiceUtils.getKeyNameFromBsonValueFunc(name) : name;
            setKeyValueOnStatement(stmt, index, exclusiveStartKey.get(name), false);
        }
    }

    /**
     * Set the given AttributeValue on the PreparedStatement at the given index based on type.
     */
    private static void setKeyValueOnStatement(PreparedStatement stmt, int index,
                                               AttributeValue attrVal, boolean isLike)
            throws SQLException {
        // TODO: does LIKE work with varbinary_encoded
        if (attrVal.getN() != null) {
            stmt.setDouble(index, Double.parseDouble(attrVal.getN()));
        } else if (attrVal.getS() != null) {
            String stringVal = isLike ? attrVal.getS()+"%" : attrVal.getS();
            stmt.setString(index, stringVal);
        } else if (attrVal.getB() != null) {
            stmt.setBytes(index, attrVal.getB().array());
        }
    }

    /**
     * Return a list of attribute names from the request's projection expression.
     * Use ExpressionAttributeNames to replace back any reserved keywords.
     * Return empty list if no projection expression is provided in the request.
     */
    private static List<String> getProjectionAttributes(QueryRequest request) {
        String projExpr = request.getProjectionExpression();
        if (StringUtils.isEmpty(projExpr)) {
            return null;
        }
        List<String> projectionList = new ArrayList<>();
        Map<String, String> exprAttrNames = request.getExpressionAttributeNames();
        String[] projectionArray = projExpr.split("\\s*,\\s*");
        for (String s : projectionArray) {
            projectionList.add(exprAttrNames.getOrDefault(s, s));
        }
        return projectionList;
    }

    /**
     * Return the attribute value map with only the primary keys from the given bson document.
     * Return both data and index table keys when querying index table.
     */
    private static Map<String, AttributeValue> getLastEvaluatedKey(BsonDocument lastBsonDoc,
                                                                   boolean useIndex,
                                                                   List<PColumn> tablePKCols,
                                                                   List<PColumn> indexPKCols) {
        List<String> keys = new ArrayList<>();
        for (PColumn pkCol : tablePKCols) {
            keys.add(pkCol.getName().toString());
        }
        if (useIndex && indexPKCols != null) {
            for (PColumn pkCol : indexPKCols) {
                keys.add(CommonServiceUtils.getKeyNameFromBsonValueFunc(pkCol.getName().toString()));
            }
        }
        return BsonDocumentToDdbAttributes.getProjectedItem(lastBsonDoc, keys);
    }
}
