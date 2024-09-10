package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.utils.BsonUtils;
import org.apache.phoenix.ddb.utils.KeyConditionsHolder;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.bson.BsonDocument;
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

public class QueryUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryUtils.class);
    private static final String SELECT_QUERY = "SELECT * FROM %s WHERE ";
    private static final String SELECT_QUERY_WITH_INDEX_HINT
            = "select /*+ INDEX(%s %s) */ * FROM %s WHERE ";

    private static final int MAX_LIMIT = 500;

    public static QueryResult query(QueryRequest request, String connectionUrl)  {
        int count = 0;
        List<Map<String, AttributeValue>> items = new ArrayList<>();

        PTable table;
        BsonDocument lastBsonDoc = null;
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            // get PKs from phoenix
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            table = phoenixConnection.getTable(
                    new PTableKey(phoenixConnection.getTenantId(), request.getTableName()));
            boolean isSortKeyPresent = table.getPKColumns().size() == 2;;

            // build PreparedStatement and execute
            PreparedStatement stmt = getPreparedStatement(connection, request, table.getPKColumns());
            LOGGER.info("SELECT Query: " + stmt);
            ResultSet rs  = stmt.executeQuery();
            while (rs.next()) {
                lastBsonDoc = (BsonDocument) rs.getObject(isSortKeyPresent ? 3 : 2);
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
                        : getLastEvaluatedKey(lastBsonDoc, table.getPKColumns()));
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    private static PreparedStatement getPreparedStatement(Connection conn, QueryRequest request,
                                                          List<PColumn> pkCols)
            throws SQLException {
        String tableName = request.getTableName();
        String indexName = request.getIndexName();

        // TODO: use BSON_VALUE for attributes when index is given
        boolean hasIndex = StringUtils.isEmpty(indexName);

        Map<String, String> exprAttrNames =  request.getExpressionAttributeNames();
        String keyCondExpr = request.getKeyConditionExpression();

        // build SQL query
        StringBuilder queryBuilder = StringUtils.isEmpty(indexName)
                ? new StringBuilder(String.format(SELECT_QUERY, tableName))
                : new StringBuilder(String.format(SELECT_QUERY_WITH_INDEX_HINT,
                                                    tableName, indexName, tableName));

        // parse Key Conditions
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames);
        String partitionKeyName = keyConditions.getPartitionKeyName();
        String sortKeyName = keyConditions.getSortKeyName();

        // append all conditions for WHERE clause
        queryBuilder.append(keyConditions.getSQLWhereClause());
        sortKeyName = addExclusiveStartKeyCondition(queryBuilder,
                request, pkCols, partitionKeyName, sortKeyName);
        addFilterCondition(queryBuilder, request);
        addScanIndexForwardCondition(queryBuilder, request, sortKeyName);
        addLimit(queryBuilder, request);

        // Set values on the PreparedStatement
        PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, keyConditions, sortKeyName);
        return stmt;
    }

    /**
     * If table has a sortKey and the QueryRequest provides an ExclusiveStartKey,
     * add the condition for the sortKey to the query.
     * Return the sortKeyName here in case the QueryRequest's KeyConditionExpression
     * did not have a condition on the sortKey.
     */
    private static String addExclusiveStartKeyCondition(StringBuilder queryBuilder,
                                                        QueryRequest request,
                                                        List<PColumn> pkCols,
                                                        String partitionKeyName,
                                                        String sortKeyName) {
        Map<String, AttributeValue> exclusiveStartKey = request.getExclusiveStartKey();
        if (exclusiveStartKey != null) {
            for (PColumn pkCol : pkCols) {
                String pkName = pkCol.getName().getString().toLowerCase();
                if (!pkName.equals(partitionKeyName)) {
                    sortKeyName = pkName;
                    queryBuilder.append(" AND " + pkName + " > ? ");
                }
            }
        }
        return sortKeyName;
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
            String bsonCondExpr
                    = BsonUtils.getBsonConditionExpression(filterExpr, exprAttrNames, exprAttrVals);
            queryBuilder.append(" AND BSON_CONDITION_EXPRESSION(COL, '");
            queryBuilder.append(bsonCondExpr);
            queryBuilder.append("')" );
        }
    }

    /**
     * If the QueryRequest has ScanIndexForward set to False and there is a sortKey,
     * add an ORDER BY sortKey DESC clause to the query.
     */
    private static void addScanIndexForwardCondition(StringBuilder queryBuilder,
                                                     QueryRequest request, String sortKeyName) {
        Boolean scanIndexForward = request.getScanIndexForward();
        if (scanIndexForward != null && !scanIndexForward && sortKeyName != null) {
            queryBuilder.append(" ORDER BY " + sortKeyName + " DESC ");
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
                                                   String sortKeyName)
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
        if (exclusiveStartKey != null && sortKeyName != null) {
            setKeyValueOnStatement(stmt, index, exclusiveStartKey.get(sortKeyName), false);
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
        List<String> projectionList = new ArrayList<>();
        if (StringUtils.isEmpty(projExpr)) {
            return projectionList;
        }
        Map<String, String> exprAttrNames =  request.getExpressionAttributeNames();
        String[] projectionArray = projExpr.split("\\s*,\\s*");
        for (int i=0; i<projectionArray.length; i++) {
            projectionList.add(exprAttrNames.getOrDefault(projectionArray[i], projectionArray[i]));
        }
        return projectionList;
    }

    /**
     * Return the attribute value map with only the primary keys from the given bson document.
     */
    private static Map<String, AttributeValue> getLastEvaluatedKey(BsonDocument lastBsonDoc,
                                                                   List<PColumn> pkCols) {
        List<String> keys = new ArrayList<>();
        for (PColumn pkCol : pkCols) {
            keys.add(pkCol.getName().toString().toLowerCase());
        }
        return BsonDocumentToDdbAttributes.getProjectedItem(lastBsonDoc, keys);
    }
}
