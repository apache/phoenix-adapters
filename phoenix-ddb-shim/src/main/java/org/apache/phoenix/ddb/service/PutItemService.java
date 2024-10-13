package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.QueryConstants;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.tuple.Tuple;
import org.apache.phoenix.schema.types.PBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class PutItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(PutItemService.class);

    private static final String PUT_WITH_HASH_KEY = "UPSERT INTO %s VALUES (?,?)";
    private static final String PUT_WITH_HASH_SORT_KEY = "UPSERT INTO %s VALUES (?,?,?)";

    private static final String CONDITIONAL_PUT_WITH_HASH_KEY = "UPSERT INTO %s VALUES (?) " +
            " ON DUPLICATE KEY UPDATE\n" +
            " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') THEN ? \n" +
            " ELSE COL END";

    private static final String CONDITIONAL_PUT_WITH_HASH_SORT_KEY
            = "UPSERT INTO %s VALUES (?, ?) " + " ON DUPLICATE KEY UPDATE\n" +
            " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') THEN ? \n" +
            " ELSE COL END";

    public static PutItemResult putItem(PutItemRequest request, String connectionUrl) {
        // get tableName, item, conditional expr and convert item to bson
        Map<String, AttributeValue> item = request.getItem();
        BsonDocument bsonDoc = DdbAttributesToBsonDocument.getBsonDocument(item);

        String returnValuesOnConditionCheckFailure =
                request.getReturnValuesOnConditionCheckFailure();
        // TODO: implement ReturnValues for Phoenix. Currently, PHOENIX-7398 supports returning
        // old row only for condition check failure. For successful update, it returns updated full
        // new row. We need to introduce new API that returns old row even with successful update.
        String returnValues = request.getReturnValues();

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            // get PTable and PK PColumns
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTable(
                    new PTableKey(phoenixConnection.getTenantId(), request.getTableName()));
            List<PColumn> pkCols = table.getPKColumns();

            //create statement based on PKs and conditional expression
            PreparedStatement stmt
                    = getPreparedStatement(connection, request, pkCols.size());
            // extract PKs from item
            setKeysOnStatement(stmt, pkCols, item);

            // set bson document of entire item
            stmt.setObject(pkCols.size()+1, bsonDoc);

            //execute, auto commit is on
            LOGGER.info("Upsert Query for PutItem: {}", stmt);
            executeUpdate(request, returnValuesOnConditionCheckFailure, stmt, table,
                pkCols);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new PutItemResult();
    }

    private static void executeUpdate(PutItemRequest request,
        String returnValuesOnConditionCheckFailure, PreparedStatement stmt, PTable table,
        List<PColumn> pkCols) throws SQLException, ConditionalCheckFailedException {
        if (StringUtils.isEmpty(returnValuesOnConditionCheckFailure)
            || ReturnValuesOnConditionCheckFailure.NONE.toString()
            .equals(returnValuesOnConditionCheckFailure)) {
            int returnStatus = stmt.executeUpdate();
            if (returnStatus == 0) {
                throw new ConditionalCheckFailedException(
                    "Conditional request failed: " + request.getConditionExpression());
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
                        "Conditional request failed: " + request.getConditionExpression());
                conditionalCheckFailedException.setItem(
                    BsonDocumentToDdbAttributes.getFullItem(rawBsonDocument));
                throw conditionalCheckFailedException;
            }
        }
    }

    /**
     * Extract values for keys from the item and set them on the PreparedStatement.
     */
    private static void setKeysOnStatement(PreparedStatement stmt, List<PColumn> pkCols, Map<String,
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
     * Return the corresponding PreparedStatement based on number of
     * Primary Key columns and conditional expression.
     */
    private static PreparedStatement getPreparedStatement(Connection conn, PutItemRequest request,
                                                      int numPKs) throws SQLException {
        PreparedStatement stmt;
        String tableName = request.getTableName();
        String condExpr = request.getConditionExpression();
        Map<String, String> exprAttrNames =  request.getExpressionAttributeNames();
        Map<String, AttributeValue> exprAttrVals =  request.getExpressionAttributeValues();
        if (!StringUtils.isEmpty(condExpr)) {
            String bsonCondExpr = CommonServiceUtils
                    .getBsonConditionExpression(condExpr, exprAttrNames, exprAttrVals);
            String QUERY_FORMAT = (numPKs == 1)
                    ? CONDITIONAL_PUT_WITH_HASH_KEY : CONDITIONAL_PUT_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(
                    String.format(QUERY_FORMAT, tableName, bsonCondExpr));
        } else {
            String QUERY_FORMAT = (numPKs == 1)
                    ? PUT_WITH_HASH_KEY : PUT_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(String.format(QUERY_FORMAT, tableName));
        }
        return stmt;
    }
}
