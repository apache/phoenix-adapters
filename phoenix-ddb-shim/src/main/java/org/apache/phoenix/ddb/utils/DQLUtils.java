package org.apache.phoenix.ddb.utils;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.schema.PColumn;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utility methods used for both Query and Scan API requests.
 */
public class DQLUtils {

    /**
     * Execute the given PreparedStatement, collect all returned items with projected attributes
     * and return QueryReuslt or ScanResponse.
     */
    public static DynamoDbResponse executeStatementReturnResult(boolean isQuery,
                                                                PreparedStatement stmt,
                                                                List<String> projectionAttributes,
                                                                boolean useIndex,
                                                                List<PColumn> tablePKCols,
                                                                List<PColumn> indexPKCols)
            throws SQLException {
        int count = 0;
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        RawBsonDocument lastBsonDoc = null;
        try (ResultSet rs  = stmt.executeQuery()) {
            while (rs.next()) {
                lastBsonDoc = (RawBsonDocument) rs.getObject(1);
                Map<String, AttributeValue> item = BsonDocumentToDdbAttributes.getProjectedItem(
                        lastBsonDoc, projectionAttributes);
                items.add(item);
                count++;
            }
            Map<String, AttributeValue> lastKey
                    = DQLUtils.getKeyFromDoc(lastBsonDoc, useIndex, tablePKCols, indexPKCols);
            int countRowsScanned = (int) PhoenixUtils.getRowsScanned(rs);
            if (isQuery) {
                return QueryResponse.builder().items(items).count(count)
                        .lastEvaluatedKey(lastKey).scannedCount(countRowsScanned).build();
            } else {
                return ScanResponse.builder().items(items).count(count)
                        .lastEvaluatedKey(lastKey).scannedCount(countRowsScanned).build();
            }
        }
    }

    /**
     * Return the attribute value map with only the primary keys from the given bson document.
     * Return both data and index table keys when querying index table.
     */
    public static Map<String, AttributeValue> getKeyFromDoc(BsonDocument lastBsonDoc,
                                                            boolean useIndex,
                                                            List<PColumn> tablePKCols,
                                                            List<PColumn> indexPKCols) {
        if (lastBsonDoc == null) return null;
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

    /**
     * Add a LIMIT clause to the query if Query or Scan Request has a limit.
     * Set it to a maxLimit if request provides a higher limit.
     */
    public static void addLimit(StringBuilder queryBuilder, Integer limit, int maxLimit) {
        limit = (limit == null) ? maxLimit : Math.min(limit, maxLimit);
        queryBuilder.append(" LIMIT " + limit);
    }

    /**
     * Return a list of attribute names from the request's projection expression.
     * Use ExpressionAttributeNames to replace back any reserved keywords.
     * Return empty list if no projection expression is provided in the request.
     */
    public static List<String> getProjectionAttributes(List<String> attributesToGet,
                                                             String projExpr,
                                                             Map<String, String> exprAttrNames) {
        List<String> projectionList = new ArrayList<>();
        if (attributesToGet != null && !attributesToGet.isEmpty()) {
            for (String s : attributesToGet) {
                projectionList.add(CommonServiceUtils
                        .replaceExpressionAttributeNames(s, exprAttrNames));
            }
            return projectionList;
        }
        if (StringUtils.isEmpty(projExpr)) {
            return null;
        }
        projExpr = CommonServiceUtils.replaceExpressionAttributeNames(projExpr, exprAttrNames);
        String[] projectionArray = projExpr.split("\\s*,\\s*");
        projectionList.addAll(Arrays.asList(projectionArray));
        return projectionList;
    }

    /**
     * If table has a sortKey and the QueryRequest provides an ExclusiveStartKey,
     * add the condition for the sortKey to the query. If the request provides an index,
     * replace sortKey name with a BSON_VALUE expression.
     * Return the sortKeyName here in case the QueryRequest's KeyConditionExpression
     * did not have a condition on the sortKey.
     */
    public static void addExclusiveStartKeyCondition(boolean isQuery,
                                                     boolean isFilterAddedForScan,
                                                     StringBuilder queryBuilder,
                                                     Map<String, AttributeValue> exclusiveStartKey,
                                                     boolean useIndex,
                                                     PColumn partitionKeyPKCol,
                                                     PColumn sortKeyPKCol) {
        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
            // query, only sort key
            if (isQuery) {
                if (sortKeyPKCol != null) {
                    //append sortKey condition if there is a sortKey
                    String name = sortKeyPKCol.getName().toString();
                    name =  (useIndex)
                            ? name.substring(1)
                            : CommonServiceUtils.getEscapedArgument(name);
                    queryBuilder.append(" AND " + name + " > ? ");
                }
            }
            // scan
            else {
                if (isFilterAddedForScan) {
                    queryBuilder.append(" AND ");
                }
                queryBuilder.append(getExclusiveStartKeyConditionForScan(
                        partitionKeyPKCol, sortKeyPKCol, useIndex));
            }
        }
    }

    /**
     * If the QueryRequest has a FilterExpression for non-pk columns,
     * add BSON_CONDITION_EXPRESSION to the query.
     */
    public static void addFilterCondition(boolean isQuery,
                                          StringBuilder queryBuilder,
                                          String filterExpr,
                                          Map<String, String> exprAttrNames,
                                          Map<String, AttributeValue> exprAttrVals) {
        if (!StringUtils.isEmpty(filterExpr)) {
            if (isQuery) {
                // we would have added KeyCondition already
                queryBuilder.append(" AND ");
            }
            String bsonCondExpr = CommonServiceUtils
                    .getBsonConditionExpression(filterExpr, exprAttrNames, exprAttrVals);
            queryBuilder.append(" BSON_CONDITION_EXPRESSION(COL, '");
            queryBuilder.append(bsonCondExpr);
            queryBuilder.append("') ");
        }
    }

    /**
     * Set the given AttributeValue on the PreparedStatement at the given index based on type.
     */
    public static void setKeyValueOnStatement(PreparedStatement stmt, int index,
                                               AttributeValue attrVal, boolean isBeginsWith)
            throws SQLException {
        if (attrVal.n() != null) {
            stmt.setDouble(index, Double.parseDouble(attrVal.n()));
        } else if (attrVal.s() != null) {
            if (isBeginsWith) { // SUBSTR(column, 0, val_length) = val
                stmt.setInt(index, attrVal.s().length());
                stmt.setString(index+1, attrVal.s());
            } else {
                stmt.setString(index, attrVal.s());
            }
        } else if (attrVal.b() != null) {
            byte[] byteArr = attrVal.b().asByteArray();
            if (isBeginsWith) { // SUBBINARY(column, 0, val_length) = val
                stmt.setInt(index, byteArr.length);
                stmt.setBytes(index+1, byteArr);
            } else {
                stmt.setBytes(index, byteArr);
            }
        }
    }

    /**
     * Return DQL specific connection configuration properties.
     */
    public static Properties getConnectionProps() {
        Properties props = PhoenixUtils.getConnectionProps();
        return props;
    }

    /**
     * last evaluated key as k1 --> pk1 > k1
     * last evaluated key as k1,k2 --> (pk1 = k1 AND pk2 > k2) OR pk1 > k1)
     */
    private static String getExclusiveStartKeyConditionForScan(PColumn partitionKeyPKCol,
                                                               PColumn sortKeyPKCol,
                                                               boolean useIndex) {
        String pkName = partitionKeyPKCol.getName().toString();
        pkName =  (useIndex)
                ? pkName.substring(1)
                : CommonServiceUtils.getEscapedArgument(pkName);
        if (sortKeyPKCol == null) {
            return pkName + " > ?";
        }
        String skName = sortKeyPKCol.getName().toString();
        skName =  (useIndex)
                ? skName.substring(1)
                : CommonServiceUtils.getEscapedArgument(skName);
        return String.format(" ((%s = ? AND %s > ?) OR %s > ?) ", pkName, skName, pkName);
    }
}
