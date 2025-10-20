package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.MapToBsonDocument;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.utils.DMLUtils;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.ddb.rest.metrics.ApiOperation;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.expression.util.bson.SQLComparisonExpressionUtils;
import org.apache.phoenix.schema.PColumn;

public class UpdateItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemService.class);

    private static final String UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?,?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_ONLY_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String UPDATE_ONLY_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?,?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,?)";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?,?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_ONLY_WITH_HASH_KEY =
            "UPSERT INTO %s VALUES (?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_ONLY_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s VALUES (?,?) " + " ON DUPLICATE KEY UPDATE_ONLY\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,?) "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,?) \n" + " ELSE COL END";

    private static final BsonDocument EMPTY_BSON_DOC = new BsonDocument();
    private static final RawBsonDocument EMPTY_RAW_BSON_DOC = RawBsonDocument.parse("{}");

    public static Map<String, Object> updateItem(Map<String, Object> request,
            String connectionUrl) {
        ValidationUtil.validateUpdateItemRequest(request);
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            // get PTable and PK PColumns
            List<PColumn> pkCols = PhoenixUtils.getPKColumns(connection, (String)request.get(ApiMetadata.TABLE_NAME));
            //create statement based on PKs and conditional expression
            StatementInfo statementInfo = getPreparedStatement(connection, request, pkCols);
            setValuesOnPreparedStatement(statementInfo, pkCols, request);

            //execute, auto commit is on
            LOGGER.debug("Upsert Query for UpdateItem: {}", statementInfo.stmt);
            // Determine if any condition expression is present
            boolean hasCondExp = (request.get(ApiMetadata.CONDITION_EXPRESSION) != null) || (
                    request.get(ApiMetadata.EXPECTED) != null);

            return DMLUtils.executeUpdate(statementInfo.stmt,
                    (String) request.get(ApiMetadata.RETURN_VALUES),
                    (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                    hasCondExp, pkCols, ApiOperation.UPDATE_ITEM);
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
    }

    private static StatementInfo getPreparedStatement(Connection conn, Map<String, Object> request,
            List<PColumn> pkCols) throws SQLException {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String condExpr = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);

        // Handle legacy Expected parameter conversion to ConditionExpression
        Map<String, Object> expected = (Map<String, Object>) request.get(ApiMetadata.EXPECTED);
        String conditionalOperator = (String) request.get(ApiMetadata.CONDITIONAL_OPERATOR);
        if (condExpr == null && expected != null) {
            // Initialize maps if they don't exist (for Expected conversion)
            if (exprAttrNames == null) {
                exprAttrNames = new HashMap<>();
            }
            if (exprAttrVals == null) {
                exprAttrVals = new HashMap<>();
            }
            // Convert Expected to ConditionExpression
            condExpr = CommonServiceUtils.convertExpectedToConditionExpression(expected,
                    conditionalOperator, exprAttrNames, exprAttrVals);
        }

        String updateExpression = (String) request.get(ApiMetadata.UPDATE_EXPRESSION);
        Map<String, Object> attributeUpdates =
                (Map<String, Object>) request.get(ApiMetadata.ATTRIBUTE_UPDATES);
        BsonDocument updateDoc;
        if (updateExpression != null) {
            updateDoc = CommonServiceUtils.getBsonUpdateExpressionFromMap(updateExpression,
                    exprAttrNames, exprAttrVals);
        } else {
            updateDoc = CommonServiceUtils.getBsonUpdateExpressionFromAttributeUpdates(
                    attributeUpdates);
        }

        // Extract $SET and $ADD portion from updateDoc for VALUES() clause
        BsonDocument newItemDoc = extractSetAndAddDocument(updateDoc, request, pkCols);

        // Determine query format to use
        QueryFormatInfo formatInfo =
                determineQueryFormat(condExpr, exprAttrNames, pkCols.size());

        BsonDocument conditionDoc = null;
        if (!StringUtils.isEmpty(condExpr)) {
            conditionDoc = CommonServiceUtils.getBsonConditionExpressionDoc(condExpr, exprAttrNames,
                    exprAttrVals);
        }

        PreparedStatement stmt =
                conn.prepareStatement(String.format(formatInfo.queryFormat, PhoenixUtils.getFullTableName(tableName, true)));
        return new StatementInfo(stmt, conditionDoc, updateDoc, newItemDoc, formatInfo.needsValuesDoc);
    }

    /**
     * Extract values from the update document to use in VALUES() clause for new row creation.
     * This includes SET operations, ADD operations (for new item creation), and primary keys.
     * For DynamoDB compatibility, ADD operations should contribute to initial values when creating new items.
     */
    private static BsonDocument extractSetAndAddDocument(BsonDocument updateDoc,
            Map<String, Object> request, List<PColumn> pkCols) {
        BsonDocument newItemDoc = new BsonDocument();

        // Add primary key values to the set document
        addKeysToNewItemDoc(newItemDoc, request, pkCols);

        if (updateDoc != null) {
            // Add SET operations - these always contribute to new item creation
            if (updateDoc.containsKey("$SET")) {
                BsonDocument setBsonDoc = updateDoc.getDocument("$SET");
                newItemDoc.putAll(setBsonDoc);
            }

            // Add ADD operations - for new item creation, these become initial values
            // DynamoDB semantics: ADD on non-existing item creates item with ADD value
            if (updateDoc.containsKey("$ADD")) {
                BsonDocument addBsonDoc = updateDoc.getDocument("$ADD");
                newItemDoc.putAll(addBsonDoc);
            }

            // Note: REMOVE and DELETE operations don't contribute to new item creation
            // They are no-ops on non-existing items, handled by the update expression
        }

        return newItemDoc;
    }

    /**
     * Helper method to add primary key values to the set document.
     */
    private static void addKeysToNewItemDoc(BsonDocument newItemDoc, Map<String, Object> request,
            List<PColumn> pkCols) {
        Map<String, Object> keyMap = (Map<String, Object>) request.get(ApiMetadata.KEY);
        if (keyMap != null) {
            for (PColumn pkCol : pkCols) {
                String keyName = pkCol.getName().getString();
                Object keyValue = keyMap.get(keyName);
                if (keyValue != null) {
                    BsonValue value =
                            MapToBsonDocument.getValueFromMapVal((Map<String, Object>) keyValue);
                    newItemDoc.put(keyName, value);
                }
            }
        }
    }

    /**
     * Set values on the prepared statement: keys, $SET document (if present),
     * condition and update documents.
     */
    private static void setValuesOnPreparedStatement(StatementInfo statementInfo,
            List<PColumn> pkCols, Map<String, Object> request) throws SQLException {
        
        DMLUtils.setKeysOnStatement(statementInfo.stmt, pkCols,
                (Map<String, Object>) request.get(ApiMetadata.KEY));
        int paramIndex = pkCols.size() + 1;

        // Set the document for VALUES() clause (only for UPDATE flavors, not UPDATE_ONLY)
        // This includes SET operations, ADD operations (for new items), and keys
        if (statementInfo.needsValuesDoc) {
            statementInfo.stmt.setObject(paramIndex++, statementInfo.newItemDoc);
        }

        if (statementInfo.conditionDoc != null) {
            statementInfo.stmt.setObject(paramIndex++, statementInfo.conditionDoc);
        }
        statementInfo.stmt.setObject(paramIndex, statementInfo.updateDoc);
    }

    /**
     * Determine the appropriate query format based on conditions and operations.
     */
    private static QueryFormatInfo determineQueryFormat(String condExpr,
            Map<String, String> exprAttrNames, int pkColsSize) {

        boolean hasCondition = !StringUtils.isEmpty(condExpr);
        boolean canCreateNewItemWithCondition = false;

        if (hasCondition) {
            // Evaluate if condition can be satisfied on non-existing item
            canCreateNewItemWithCondition = evaluateConditionOnNonExistingItem(condExpr, exprAttrNames);
        }

        if (canCreateNewItemWithCondition) {
            // Can create new item and have values to insert (set/add or even just keys)
            String format = pkColsSize == 1 ?
                            CONDITIONAL_UPDATE_WITH_HASH_KEY :
                            CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY;
            return new QueryFormatInfo(format, true);
        } else {
            if (hasCondition) {
                // Cannot create new item (condition prevents it) - only update existing
                String format = (pkColsSize == 1) ?
                        CONDITIONAL_UPDATE_ONLY_WITH_HASH_KEY :
                        CONDITIONAL_UPDATE_ONLY_WITH_HASH_SORT_KEY;
                return new QueryFormatInfo(format, false); // UPDATE_ONLY doesn't use VALUES document
            } else {
                // there was no condition to begin with, still allow creation
                String format = (pkColsSize == 1) ?
                        UPDATE_WITH_HASH_KEY :
                        UPDATE_WITH_HASH_SORT_KEY;
                return new QueryFormatInfo(format, true);
            }
        }
    }

    /**
     * Evaluate if a condition expression can be satisfied on a non-existing item.
     * This determines whether UPDATE (allows creation) or UPDATE_ONLY (existing only) should be used.
     * <p>
     * DynamoDB semantics:
     * - Non-existing items are treated as empty documents
     * - Function calls on non-existing attributes typically return false/null
     * - Existence checks (attribute_exists) return false
     * - Value comparisons with non-existing attributes return false
     * <p>
     */
    private static boolean evaluateConditionOnNonExistingItem(String condExpr,
            Map<String, String> exprAttrNames) {
        try {
            BsonDocument exprAttrNamesDoc =
                    CommonServiceUtils.getExpressionAttributeNamesDoc(exprAttrNames);
            boolean result = SQLComparisonExpressionUtils.evaluateConditionExpression(condExpr,
                    EMPTY_RAW_BSON_DOC, EMPTY_BSON_DOC, exprAttrNamesDoc);

            LOGGER.debug("Condition '{}' evaluation on empty document: {}", condExpr, result);
            return result;
        } catch (Exception e) {
            // If condition evaluation fails, be conservative and assume it cannot be satisfied
            LOGGER.warn("Failed to evaluate condition '{}' on empty document, assuming false: {}",
                    condExpr, e.getMessage());
            return false;
        }
    }

    /**
     * Helper class to return query format and whether it needs a VALUES document parameter.
     */
    private static class QueryFormatInfo {
        final String queryFormat;
        final boolean needsValuesDoc;

        QueryFormatInfo(String queryFormat, boolean needsValuesDoc) {
            this.queryFormat = queryFormat;
            this.needsValuesDoc = needsValuesDoc;
        }
    }

    /**
     * Helper class to return statement information including condition, update, and set documents.
     */
    private static class StatementInfo {

        final PreparedStatement stmt;
        final BsonDocument conditionDoc;
        final BsonDocument updateDoc;
        final BsonDocument newItemDoc;
        final boolean needsValuesDoc;

        public StatementInfo(PreparedStatement stmt, BsonDocument conditionDoc,
                BsonDocument updateDoc, BsonDocument newItemDoc, boolean needsValuesDoc) {
            this.stmt = stmt;
            this.conditionDoc = conditionDoc;
            this.updateDoc = updateDoc;
            this.newItemDoc = newItemDoc;
            this.needsValuesDoc = needsValuesDoc;
        }
    }
}
