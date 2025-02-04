package org.apache.phoenix.ddb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.ReturnValue;
import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;
import org.bson.RawBsonDocument;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DMLUtils {

    /**
     * Extract values for keys from the item and set them on the PreparedStatement.
     */
    public static void setKeysOnStatement(PreparedStatement stmt, List<PColumn> pkCols, Map<String,
            AttributeValue> item) throws SQLException {
        for (int i=0; i<pkCols.size(); i++) {
            PColumn pkCol = pkCols.get(i);
            String colName = pkCol.getName().toString();
            PDataType type = pkCol.getDataType();
            if (type.equals(PDouble.INSTANCE)) {
                Double value = Double.parseDouble(item.get(colName).getN());
                stmt.setDouble(i+1, value);
            } else if (type.equals(PVarchar.INSTANCE)) {
                String value = item.get(colName).getS();
                stmt.setString(i+1, value);
            } else if (type.equals(PVarbinaryEncoded.INSTANCE)) {
                byte[] b = item.get(colName).getB().array();
                stmt.setBytes(i+1, b);
            } else {
                throw new IllegalArgumentException("Primary Key column type "
                        + type + " is not " + "correct type");
            }
        }
    }

    /**
     * Executes the given PreparedStatement of an UPSERT query for PutItem/UpdateItem API.
     *
     * If conditionExpression is given and it fails, throw ConditionalCheckFailedException.
     *  - if returnValuesOnConditionCheckFailure is ALL_OLD, set the item on the Exception
     * If conditionExpression succeeds and returnValue is ALL_NEW, return the item.
     *
     * TODO: UPDATED_OLD | UPDATED_NEW
     * TODO: ALL_OLD when condition succeeds
     */
    public static Map<String, AttributeValue> executeUpdate(PreparedStatement stmt,
                                                            String returnValue,
                                                            String returnValuesOnConditionCheckFailure,
                                                            String condExpr, List<PColumn> pkCols, boolean isDelete)
            throws SQLException, ConditionalCheckFailedException {
        Map<String, AttributeValue> returnAttrs = null;
        if (!needReturnRow(returnValue, returnValuesOnConditionCheckFailure)) {
            int returnStatus = stmt.executeUpdate();
            if (returnStatus == 0 && !StringUtils.isEmpty(condExpr)) {
                throw new ConditionalCheckFailedException("Conditional request failed: " + condExpr);
            }
            return null;
        }
        // atomic update
        Pair<Integer, ResultSet> resultPair =
                stmt.unwrap(PhoenixPreparedStatement.class).executeAtomicUpdateReturnRow();
        int returnStatus = resultPair.getFirst();
        ResultSet rs = resultPair.getSecond();
        RawBsonDocument rawBsonDocument = rs == null ? null :
                (RawBsonDocument) rs.getObject(pkCols.size()+1);
        if ((returnStatus == 0  && !isDelete) || (isDelete && rawBsonDocument == null) ) {
            if (!StringUtils.isEmpty(condExpr)) {
                ConditionalCheckFailedException conditionalCheckFailedException =
                        new ConditionalCheckFailedException(
                                "Conditional request failed: " + condExpr);
                if (ReturnValuesOnConditionCheckFailure.ALL_OLD.toString()
                        .equals(returnValuesOnConditionCheckFailure) && !isDelete) {
                    conditionalCheckFailedException.setItem(
                            BsonDocumentToDdbAttributes.getFullItem(rawBsonDocument));
                }
                throw conditionalCheckFailedException;
            }
        } else {
            if (ReturnValue.ALL_NEW.toString().equals(returnValue) || ReturnValue.ALL_OLD.toString().equals(returnValue)) {
                returnAttrs = BsonDocumentToDdbAttributes.getFullItem(rawBsonDocument);
            }
        }
        return returnAttrs;
    }

    /**
     * Use return row api only if
     * returnValue is not empty/null and not NONE
     * OR
     * returnValuesOnConditionCheckFailure is not empty/null and not NONE
     */
    private static boolean needReturnRow(String returnValue,
                                      String returnValuesOnConditionCheckFailure) {
        return (!StringUtils.isEmpty(returnValue)
                    && !ReturnValue.NONE.toString().equals(returnValue))
                || (!StringUtils.isEmpty(returnValuesOnConditionCheckFailure)
                        && !ReturnValuesOnConditionCheckFailure.NONE.toString().equals(returnValuesOnConditionCheckFailure));
    }
}
