package org.apache.phoenix.ddb.service.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;

import org.bson.BsonDocument;
import org.bson.RawBsonDocument;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
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
    public static Map<String, Object> executeStatementReturnResult(boolean isQuery,
            PreparedStatement stmt, List<String> projectionAttributes, boolean useIndex,
            List<PColumn> tablePKCols, List<PColumn> indexPKCols, String tableName) throws SQLException {
        int count = 0;
        List<Map<String, Object>> items = new ArrayList<>();
        RawBsonDocument lastBsonDoc = null;
        try (ResultSet rs = stmt.executeQuery()) {
            while (rs.next()) {
                lastBsonDoc = (RawBsonDocument) rs.getObject(1);
                Map<String, Object> item =
                        BsonDocumentToMap.getProjectedItem(lastBsonDoc, projectionAttributes);
                items.add(item);
                count++;
            }
            Map<String, Object> lastKey =
                    DQLUtils.getKeyFromDoc(lastBsonDoc, useIndex, tablePKCols, indexPKCols);
            int countRowsScanned = (int) PhoenixUtils.getRowsScanned(rs);
            Map<String, Object> response = new HashMap<>();
            response.put(ApiMetadata.ITEMS, items);
            response.put(ApiMetadata.COUNT, count);
            response.put(ApiMetadata.LAST_EVALUATED_KEY, lastKey);
            response.put(ApiMetadata.SCANNED_COUNT, countRowsScanned);
            response.put(ApiMetadata.CONSUMED_CAPACITY,
                    CommonServiceUtils.getConsumedCapacity(tableName));
            return response;
        }
    }

    /**
     * Return the attribute value map with only the primary keys from the given bson document.
     * Return both data and index table keys when querying index table.
     */
    public static Map<String, Object> getKeyFromDoc(BsonDocument lastBsonDoc, boolean useIndex,
            List<PColumn> tablePKCols, List<PColumn> indexPKCols) {
        if (lastBsonDoc == null)
            return null;
        List<String> keys = new ArrayList<>();
        for (PColumn pkCol : tablePKCols) {
            keys.add(pkCol.getName().toString());
        }
        if (useIndex && indexPKCols != null) {
            for (PColumn pkCol : indexPKCols) {
                keys.add(
                        CommonServiceUtils.getKeyNameFromBsonValueFunc(pkCol.getName().toString()));
            }
        }
        return BsonDocumentToMap.getProjectedItem(lastBsonDoc, keys);
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
            String projExpr, Map<String, String> exprAttrNames) {
        List<String> projectionList = new ArrayList<>();
        if (attributesToGet != null && !attributesToGet.isEmpty()) {
            for (String s : attributesToGet) {
                projectionList.add(
                        CommonServiceUtils.replaceExpressionAttributeNames(s, exprAttrNames));
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
    public static void addExclusiveStartKeyCondition(boolean isQuery, boolean isFilterAddedForScan,
            StringBuilder queryBuilder, Map<String, Object> exclusiveStartKey, boolean useIndex,
            PColumn partitionKeyPKCol, PColumn sortKeyPKCol, boolean scanIndexForward) {
        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
            // query, only sort key
            if (isQuery) {
                String op = " > ";
                // when using index and scanning backwards, flip the operator
                if (useIndex && !scanIndexForward) {
                    op = " < ";
                }
                if (sortKeyPKCol != null) {
                    //append sortKey condition if there is a sortKey
                    String name = sortKeyPKCol.getName().toString();
                    name = (useIndex) ?
                            name.substring(1) :
                            CommonServiceUtils.getEscapedArgument(name);
                    queryBuilder.append(" AND " + name + op + " ? ");
                }
            }
            // scan
            else {
                if (isFilterAddedForScan) {
                    queryBuilder.append(" AND ");
                }
                queryBuilder.append(
                        getExclusiveStartKeyConditionForScan(partitionKeyPKCol, sortKeyPKCol,
                                useIndex));
            }
        }
    }

    /**
     * If the QueryRequest has a FilterExpression for non-pk columns,
     * add BSON_CONDITION_EXPRESSION to the query.
     */
    public static void addFilterCondition(boolean isQuery, StringBuilder queryBuilder,
            String filterExpr, Map<String, String> exprAttrNames,
            Map<String, Object> exprAttrVals) {
        if (!StringUtils.isEmpty(filterExpr)) {
            if (isQuery) {
                // we would have added KeyCondition already
                queryBuilder.append(" AND ");
            }
            String bsonCondExpr =
                    CommonServiceUtils.getBsonConditionExpressionFromMap(filterExpr, exprAttrNames,
                            exprAttrVals);
            queryBuilder.append(" BSON_CONDITION_EXPRESSION(COL, '");
            queryBuilder.append(bsonCondExpr);
            queryBuilder.append("') ");
        }
    }

    /**
     * Set the given AttributeValue on the PreparedStatement at the given index based on type.
     */
    public static void setKeyValueOnStatement(PreparedStatement stmt, int index,
            Map<String, Object> attrVal, boolean isBeginsWith) throws SQLException {
        if (attrVal.containsKey("N")) {
            stmt.setDouble(index, Double.parseDouble((String) attrVal.get("N")));
        } else if (attrVal.containsKey("S")) {
            String val = (String) attrVal.get("S");
            if (isBeginsWith) { // SUBSTR(column, 0, val_length) = val
                stmt.setInt(index, val.length());
                stmt.setString(index+1, val);
            } else {
                stmt.setString(index, val);
            }
        } else if (attrVal.containsKey("B")) {
            byte[] val = Base64.getDecoder().decode((String) attrVal.get("B"));
            if (isBeginsWith) { // SUBBINARY(column, 0, val_length) = val
                stmt.setInt(index, val.length);
                stmt.setBytes(index+1, val);
            } else {
                stmt.setBytes(index, val);
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
            PColumn sortKeyPKCol, boolean useIndex) {
        String pkName = partitionKeyPKCol.getName().toString();
        pkName = (useIndex) ? pkName.substring(1) : CommonServiceUtils.getEscapedArgument(pkName);
        if (sortKeyPKCol == null) {
            return pkName + " > ?";
        }
        String skName = sortKeyPKCol.getName().toString();
        skName = (useIndex) ? skName.substring(1) : CommonServiceUtils.getEscapedArgument(skName);
        return String.format(" ((%s = ? AND %s > ?) OR %s > ?) ", pkName, skName, pkName);
    }
}
