package org.apache.phoenix.ddb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;
import org.bson.RawBsonDocument;

import java.sql.PreparedStatement;
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
     * If returnValuesOnConditionCheckFailure is not set or set to NONE, throws
     * ConditionalCheckFailedException.
     * If it is set to ALL_OLD, populates the item into the exception.
     *
     * TODO: implement ReturnValues when Phoenix supports them
     * TODO: ReturnValues is applicable only when update is successful
     * TODO: NONE | ALL_OLD | UPDATED_OLD | ALL_NEW | UPDATED_NEW
     */
    public static Map<String, AttributeValue> executeUpdate(PreparedStatement stmt,
                                              String returnValues,
                                              String returnValuesOnConditionCheckFailure,
                                              String condExpr, PTable table, List<PColumn> pkCols)
            throws SQLException, ConditionalCheckFailedException {
        Map<String, AttributeValue> returnAttrs = null;
        if (StringUtils.isEmpty(returnValuesOnConditionCheckFailure)
                || ReturnValuesOnConditionCheckFailure.NONE.toString()
                .equals(returnValuesOnConditionCheckFailure)) {
            int returnStatus = stmt.executeUpdate();
            if (returnStatus == 0) {
                throw new ConditionalCheckFailedException(
                        "Conditional request failed: " + condExpr);
            }
        } else if (ReturnValuesOnConditionCheckFailure.ALL_OLD.toString()
                .equals(returnValuesOnConditionCheckFailure)) {
            Pair<Integer, Tuple> tuplePair =
                    stmt.unwrap(PhoenixPreparedStatement.class).executeUpdateReturnRow();
            if (tuplePair.getFirst() == 0) {
                Tuple tuple = tuplePair.getSecond();
                Cell cell = tuple.getValue(QueryConstants.DEFAULT_COLUMN_FAMILY_BYTES,
                        table.getColumns().get(pkCols.size()).getColumnQualifierBytes());
                RawBsonDocument rawBsonDocument =
                        (RawBsonDocument) PBson.INSTANCE.toObject(cell.getValueArray(),
                                cell.getValueOffset(), cell.getValueLength());
                ConditionalCheckFailedException conditionalCheckFailedException =
                        new ConditionalCheckFailedException(
                                "Conditional request failed: " + condExpr);
                conditionalCheckFailedException.setItem(
                        BsonDocumentToDdbAttributes.getFullItem(rawBsonDocument));
                throw conditionalCheckFailedException;
            }
        }
        return returnAttrs;
    }
}
