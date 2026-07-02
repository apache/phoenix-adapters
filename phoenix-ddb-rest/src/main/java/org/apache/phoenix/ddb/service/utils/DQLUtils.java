package org.apache.phoenix.ddb.service.utils;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.bson.BsonNumberConversionUtil;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PDataType;

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

/**
 * Utility methods used for both Query and Scan API requests.
 */
public class DQLUtils {

    public static final String SIZE_LIMIT_REACHED = "SizeLimitReached";
    private static final String RVC_1 = "(%s) %s (?)";
    private static final String RVC_2 = "(%s, %s) %s (?, ?)";
    private static final String RVC_3 = "(%s, %s, %s) %s (?, ?, ?)";
    private static final String RVC_4 = "(%s, %s, %s, %s) %s (?, ?, ?, ?)";

    /**
     * Execute the given PreparedStatement and return a QueryResult / ScanResponse.
     *
     * <p>When {@code countOnly} is true the SQL must project PK columns only
     * (see {@link #buildCountProjection}); the per-row {@code MAX_BYTES_SIZE} cap does
     * not apply on that path ; row count is bounded by the SQL {@code LIMIT}.
     */
    public static Map<String, Object> executeStatementReturnResult(PreparedStatement stmt,
            List<String> projectionAttributes, boolean useIndex,
            List<PColumn> tablePKCols, List<PColumn> indexPKCols, String tableName,
            boolean isSingleRowExpected, boolean countOnly) throws SQLException {
        try (ResultSet rs = stmt.executeQuery()) {
            if (countOnly) {
                return executeCountOnlyResult(rs, useIndex, tablePKCols, indexPKCols, tableName,
                        isSingleRowExpected);
            }
            return executeItemsResult(rs, projectionAttributes, useIndex, tablePKCols, indexPKCols,
                    tableName, isSingleRowExpected);
        } catch (SQLException e) {
            if (e.getMessage() != null && e.getMessage()
                    .contains("BsonConditionInvalidArgumentException")) {
                throw new ValidationException("Invalid arguments in Condition Expression");
            }
            throw e;
        }
    }

    private static Map<String, Object> executeItemsResult(ResultSet rs,
            List<String> projectionAttributes, boolean useIndex,
            List<PColumn> tablePKCols, List<PColumn> indexPKCols, String tableName,
            boolean isSingleRowExpected) throws SQLException {
        int count = 0;
        int bytesSize = 0;
        List<Map<String, Object>> items = new ArrayList<>();
        RawBsonDocument lastBsonDoc = null;
        while (rs.next()) {
            lastBsonDoc = (RawBsonDocument) rs.getObject(1);
            Map<String, Object> item =
                    BsonDocumentToMap.getProjectedItem(lastBsonDoc, projectionAttributes);
            items.add(item);
            count++;
            bytesSize +=
                    (int) rs.unwrap(PhoenixResultSet.class).getCurrentRow().getSerializedSize();
            if (bytesSize >= ApiMetadata.MAX_BYTES_SIZE) {
                break;
            }
        }
        Map<String, Object> lastKey = isSingleRowExpected ? null
                : DQLUtils.getKeyFromDoc(lastBsonDoc, useIndex, tablePKCols, indexPKCols);
        Map<String, Object> response = buildResponseEnvelope(count,
                (int) PhoenixUtils.getRowsScanned(rs), lastKey, tableName);
        response.put(ApiMetadata.ITEMS, items);
        return response;
    }

    private static Map<String, Object> executeCountOnlyResult(ResultSet rs, boolean useIndex,
            List<PColumn> tablePKCols, List<PColumn> indexPKCols, String tableName,
            boolean isSingleRowExpected) throws SQLException {
        int count = 0;
        Map<String, Object> lastKey = isSingleRowExpected ? null : new HashMap<>();
        boolean haveLastRow = false;
        while (rs.next()) {
            count++;
            if (lastKey != null) {
                readKeyFromResultSet(rs, lastKey, useIndex, tablePKCols, indexPKCols);
                haveLastRow = true;
            }
        }
        return buildResponseEnvelope(count, (int) PhoenixUtils.getRowsScanned(rs),
                haveLastRow ? lastKey : null, tableName);
    }

    private static Map<String, Object> buildResponseEnvelope(int count, int scannedCount,
            Map<String, Object> lastKey, String tableName) {
        Map<String, Object> response = new HashMap<>();
        response.put(ApiMetadata.COUNT, count);
        response.put(ApiMetadata.LAST_EVALUATED_KEY, lastKey);
        response.put(ApiMetadata.SCANNED_COUNT, scannedCount);
        response.put(ApiMetadata.CONSUMED_CAPACITY,
                CommonServiceUtils.getConsumedCapacity(tableName));
        return response;
    }

    /**
     * Comma-separated list of PK columns (index PKs first when {@code useIndex}, then table
     * PKs). Used as the SELECT projection on the count-only path: on an UNCOVERED INDEX this
     * projection is fully covered by the index row key, so the server never back-joins to
     * the data table.
     */
    public static String buildCountProjection(boolean useIndex, List<PColumn> tablePKCols,
            List<PColumn> indexPKCols) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        if (useIndex && indexPKCols != null) {
            for (PColumn pkCol : indexPKCols) {
                if (!first) sb.append(", ");
                sb.append(CommonServiceUtils.getColumnExprFromPCol(pkCol, true));
                first = false;
            }
        }
        for (PColumn pkCol : tablePKCols) {
            if (!first) sb.append(", ");
            sb.append(CommonServiceUtils.getEscapedArgument(pkCol.getName().toString()));
            first = false;
        }
        return sb.toString();
    }

    /**
     * Overwrite {@code dest} with the current row's PK values, index PKs first when
     * {@code useIndex}, then table PKs.
     */
    private static void readKeyFromResultSet(ResultSet rs, Map<String, Object> dest,
            boolean useIndex, List<PColumn> tablePKCols, List<PColumn> indexPKCols)
            throws SQLException {
        int col = 1;
        if (useIndex && indexPKCols != null) {
            for (PColumn pkCol : indexPKCols) {
                String name =
                        CommonServiceUtils.getKeyNameFromBsonValueFunc(pkCol.getName().toString());
                dest.put(name, attributeValueFromResultSetColumn(rs, col++, pkCol.getDataType()));
            }
        }
        for (PColumn pkCol : tablePKCols) {
            dest.put(pkCol.getName().toString(),
                    attributeValueFromResultSetColumn(rs, col++, pkCol.getDataType()));
        }
    }

    /** Convert a JDBC ResultSet column (typed by {@code PDataType}) into a DynamoDB-shaped
     *  attribute map (one of {@code {S: <string>}}, {@code {N: <string>}}, {@code {B: <base64 string>}}). */
    private static Map<String, Object> attributeValueFromResultSetColumn(ResultSet rs, int col,
                                                                         PDataType<?> dataType) throws SQLException {
        Map<String, Object> attrVal = new HashMap<>();
        switch (CommonServiceUtils.getScalarAttributeFromPDataType(dataType)) {
            case S:
                attrVal.put("S", rs.getString(col));
                break;
            case N:
                // N PKs are always stored in a DOUBLE column (see CreateTableService) so we
                // can't recover the original BSON sub-type. Pick the Number sub-type that gives
                // items-path-compatible formatting: long form when integral, double form
                // otherwise.
                double d = rs.getDouble(col);
                Number n;
                if (!Double.isInfinite(d) && !Double.isNaN(d) && d == Math.floor(d)
                        && d >= Long.MIN_VALUE && d <= Long.MAX_VALUE) {
                    n = (long) d;
                } else {
                    n = d;
                }
                attrVal.put("N", BsonNumberConversionUtil.numberToString(n));
                break;
            case B:
                attrVal.put("B", Base64.getEncoder().encodeToString(rs.getBytes(col)));
                break;
            default:
                throw new IllegalStateException(
                        "Unsupported PK data type for count-only LastEvaluatedKey: " + dataType);
        }
        return attrVal;
    }

    /**
     * Return the attribute value map with only the primary keys from the given bson document.
     * Return both data and index table keys when querying index table.
     */
    public static Map<String, Object> getKeyFromDoc(BsonDocument lastBsonDoc, boolean useIndex,
            List<PColumn> tablePKCols, List<PColumn> indexPKCols) {
        if (lastBsonDoc == null) {
            return null;
        }
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
    public static List<String> getProjectionAttributes(String projExpr,
                                                       Map<String, String> exprAttrNames) {
        if (StringUtils.isEmpty(projExpr)) {
            return null;
        }
        List<String> projectionList = new ArrayList<>();
        try {
            projExpr = CommonServiceUtils.replaceExpressionAttributeNames(projExpr, exprAttrNames);
        } catch (IllegalArgumentException e) {
            throw new ValidationException(e.getMessage());
        }
        String[] projectionArray = projExpr.split("\\s*,\\s*");
        projectionList.addAll(Arrays.asList(projectionArray));
        return projectionList;
    }

    /**
     * If table has a sortKey in its KeyConditionExpression and the QueryRequest provides an ExclusiveStartKey,
     * add the condition for the sortKey to the query.
     * If the request provides an index, replace sortKey name with a BSON_VALUE expression.
     * For index, we also need to provide RVC condition for data table PKs.
     */
    public static void addExclusiveStartKeyConditionForQuery(StringBuilder queryBuilder,
            Map<String, Object> exclusiveStartKey, boolean useIndex,
            boolean scanIndexForward, List<PColumn> tablePKCols, List<PColumn> indexPKCols) {
        if (exclusiveStartKey != null && !exclusiveStartKey.isEmpty()) {
                String op = " > ";
                // when scanning backwards, flip the operator
                if (!scanIndexForward) {
                    op = " < ";
                }
                if (useIndex) {
                    String rvcClause = getRVCClauseForIndexQuery(op, tablePKCols, indexPKCols);
                    queryBuilder.append(" AND ").append(rvcClause);
                } else {
                    if (tablePKCols.size() == 2) {
                        String name = tablePKCols.get(1).getName().toString();
                        name = CommonServiceUtils.getEscapedArgument(name);
                        queryBuilder.append(" AND " + name + op + " ? ");
                    }
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
                    CommonServiceUtils.getBsonConditionExpressionString(filterExpr, exprAttrNames,
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
        if (attrVal == null) {
            throw new ValidationException(
                    "An expression attribute value used is not defined.");
        }
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

    private static String getRVCClauseForIndexQuery(String op, List<PColumn> tablePKCols,
                                                    List<PColumn> indexPKCols) {
        String hk = CommonServiceUtils.getEscapedArgument(tablePKCols.get(0).getName().toString());
        if (indexPKCols.size() == 1) {
            // Index has only ihk
            if (tablePKCols.size() == 1) {
                // (hk) > (?)
                return String.format(RVC_1, hk, op);
            } else {
                // (hk, sk) > (?, ?)
                String sk = CommonServiceUtils.getEscapedArgument(tablePKCols.get(1).getName().toString());
                return String.format(RVC_2, hk, sk, op);
            }
        } else {
            // Index has ihk, isk
            String isk = CommonServiceUtils.getColumnExprFromPCol(indexPKCols.get(1), true);
            if (tablePKCols.size() == 1) {
                // (isk, hk) > (?, ?)
                return String.format(RVC_2, isk, hk, op);
            } else {
                // (isk, hk, sk) > (?, ?, ?)
                String sk = CommonServiceUtils.getEscapedArgument(tablePKCols.get(1).getName().toString());
                return String.format(RVC_3, isk, hk, sk, op);
            }
        }
    }

    public static String getRVCClauseForScan(String op, List<PColumn> tablePKCols,
                                              boolean useIndex, List<PColumn> indexPKCols) {
        String hk = CommonServiceUtils.getEscapedArgument(tablePKCols.get(0).getName().toString());
        if (!useIndex) {
            if (tablePKCols.size() == 1) {
                return String.format(RVC_1, hk, op);
            } else {
                String sk = CommonServiceUtils.getEscapedArgument(tablePKCols.get(1).getName().toString());
                return String.format(RVC_2, hk, sk, op);
            }
        }
        String ihk = CommonServiceUtils.getColumnExprFromPCol(indexPKCols.get(0), true);
        if (indexPKCols.size() == 1) {
            if (tablePKCols.size() == 1) {
                // (ihk, hk) > (?, ?)
                return String.format(RVC_2, ihk, hk, op);
            } else {
                // (ihk, hk, sk) > (?, ?, ?)
                String sk = CommonServiceUtils.getEscapedArgument(tablePKCols.get(1).getName().toString());
                return String.format(RVC_3, ihk, hk, sk, op);
            }
        } else {
            String isk = CommonServiceUtils.getColumnExprFromPCol(indexPKCols.get(1), true);
            if (tablePKCols.size() == 1) {
                // (ihk, isk, hk) > (?, ?, ?)
                return String.format(RVC_3, ihk, isk, hk, op);
            } else {
                // (ihk, isk, hk, sk) > (?, ?, ?, ?)
                String sk = CommonServiceUtils.getEscapedArgument(tablePKCols.get(1).getName().toString());
                return String.format(RVC_4, ihk, isk, hk, sk, op);
            }
        }
    }
}
