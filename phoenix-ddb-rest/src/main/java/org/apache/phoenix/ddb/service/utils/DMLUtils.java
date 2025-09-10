package org.apache.phoenix.ddb.service.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.bson.RawBsonDocument;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.service.exceptions.ConditionCheckFailedException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.rest.metrics.ApiOperation;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

public class DMLUtils {


    /**
     * Extract values for keys from the item and set them on the PreparedStatement.
     */
    public static void setKeysOnStatement(PreparedStatement stmt, List<PColumn> pkCols,
        Map<String, Object> item) throws SQLException {
        for (int i = 0; i < pkCols.size(); i++) {
            PColumn pkCol = pkCols.get(i);
            String colName = pkCol.getName().toString();
            PDataType type = pkCol.getDataType();
            if (type.equals(PDouble.INSTANCE)) {
                String strValue = (String) ((Map<String, Object>) item.get(colName)).get("N");
                double value = Double.parseDouble(strValue);
                stmt.setDouble(i + 1, value);
            } else if (type.equals(PVarchar.INSTANCE)) {
                String value = (String) ((Map<String, Object>) item.get(colName)).get("S");
                stmt.setString(i + 1, value);
            } else if (type.equals(PVarbinaryEncoded.INSTANCE)) {
                String value = (String) ((Map<String, Object>) item.get(colName)).get("B");
                byte[] b = Base64.getDecoder().decode(value);
                stmt.setBytes(i + 1, b);
            } else {
                throw new IllegalArgumentException(
                    "Primary Key column type " + type + " is not " + "correct type");
            }
        }
    }

    /**
     * Executes the given PreparedStatement of an UPSERT query for PutItem/UpdateItem API.
     *
     * If conditionExpression is given and it fails, throw ConditionalCheckFailedException.
     *  - if returnValuesOnConditionCheckFailure is ALL_OLD, set the item on the Exception
     * If conditionExpression succeeds return the item with type of returnValue.
     *
     * TODO: UPDATED_OLD | UPDATED_NEW
     */
    public static Map<String, Object> executeUpdate(PreparedStatement stmt, String returnValue,
        String returnValuesOnConditionCheckFailure, boolean hasCondExp, List<PColumn> pkCols,
        ApiOperation apiOperation) throws SQLException, ConditionCheckFailedException {
        try {
            Map<String, Object> returnAttrs = Collections.emptyMap();
            if (!needReturnRow(returnValue, returnValuesOnConditionCheckFailure)) {
                int returnStatus = stmt.executeUpdate();
                if (returnStatus == 0 && hasCondExp) {
                    throw new ConditionCheckFailedException();
                }
                return null;
            }
            Pair<Integer, ResultSet> resultPair;
            if (ApiMetadata.ALL_OLD.equals(returnValue)
                && apiOperation != ApiOperation.DELETE_ITEM) {
                resultPair =
                    stmt.unwrap(PhoenixPreparedStatement.class).executeAtomicUpdateReturnOldRow();
            } else {
                resultPair =
                    stmt.unwrap(PhoenixPreparedStatement.class).executeAtomicUpdateReturnRow();
            }
            int returnStatus = resultPair.getFirst();
            ResultSet rs = resultPair.getSecond();
            RawBsonDocument rawBsonDocument =
                rs == null ? null : (RawBsonDocument) rs.getObject(pkCols.size() + 1);
            if ((returnStatus == 0 && apiOperation != ApiOperation.DELETE_ITEM) ||
                (apiOperation == ApiOperation.DELETE_ITEM && rawBsonDocument == null)) {
                if (hasCondExp) {
                    ConditionCheckFailedException conditionalCheckFailedException =
                        new ConditionCheckFailedException();
                    if (ApiMetadata.ALL_OLD.equals(returnValuesOnConditionCheckFailure) &&
                        apiOperation != ApiOperation.DELETE_ITEM) {
                        conditionalCheckFailedException.setItem(
                            BsonDocumentToMap.getFullItem(rawBsonDocument));
                    }
                    throw conditionalCheckFailedException;
                }
            } else {
                boolean returnValuesInResponse = false;
                if (apiOperation != ApiOperation.DELETE_ITEM) {
                    // TODO : reject UPDATED_OLD, UPDATED_NEW cases which are not supported
                    if (ApiMetadata.ALL_NEW.equals(returnValue) || ApiMetadata.ALL_OLD.equals(
                        returnValue)) {
                        returnValuesInResponse = true;
                    }
                } else if (ApiMetadata.ALL_OLD.equals(returnValue)) {
                    returnValuesInResponse = true;
                }
                if (returnValuesInResponse) {
                    returnAttrs = BsonDocumentToMap.getFullItem(rawBsonDocument);
                    Map<String, Object> tmpReturnAttrs = returnAttrs;
                    returnAttrs = new HashMap<>();
                    returnAttrs.put(ApiMetadata.ATTRIBUTES, tmpReturnAttrs);
                }
            }
            return returnAttrs;
        } catch (SQLException e) {
            if (e.getMessage() != null && e.getMessage()
                .contains("BsonUpdateInvalidArgumentException")) {
                throw new ValidationException("Invalid document path used for update");
            }
            throw e;
        }
    }

    /**
     * Use return row api only if
     * returnValue is not empty/null and not NONE
     * OR
     * returnValuesOnConditionCheckFailure is not empty/null and not NONE
     */
    private static boolean needReturnRow(String returnValue,
        String returnValuesOnConditionCheckFailure) {
        return (returnValue != null && !returnValue.equals(ApiMetadata.NONE)) || (
            returnValuesOnConditionCheckFailure != null
                && !returnValuesOnConditionCheckFailure.equals(ApiMetadata.NONE));
    }
}