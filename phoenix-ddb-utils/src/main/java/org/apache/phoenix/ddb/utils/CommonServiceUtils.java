/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.expression.util.bson.SQLComparisonExpressionUtils;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.phoenix.ddb.bson.MapToBsonDocument;
import org.apache.phoenix.ddb.update.UpdateExpressionToBson;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;

import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Common utilities to be used by phoenixDBClientV2 APIs.
 */
public class CommonServiceUtils {

    public static final String DOUBLE_QUOTE = "\"";
    public static final String HASH = "#";
    private static final Pattern EXPR_ATTR_NAME_PATTERN = Pattern.compile("#([a-zA-Z0-9_]+)");
    private static final BsonDocument EMPTY_BSON_DOC = new BsonDocument();
    private static final RawBsonDocument EMPTY_RAW_BSON_DOC = RawBsonDocument.parse("{}");
    private static final Logger LOGGER = LoggerFactory.getLogger(CommonServiceUtils.class);

    public static boolean isCauseMessageAvailable(Exception e) {
        return e.getCause() != null && e.getCause().getMessage() != null;
    }

    public static ScalarAttributeType getScalarAttributeFromPDataType(PDataType<?> pDataType) {
        if (pDataType == PVarchar.INSTANCE) {
            return ScalarAttributeType.S;
        } else if (pDataType == PDouble.INSTANCE || pDataType == PDecimal.INSTANCE) {
            return ScalarAttributeType.N;
        } else if (pDataType == PVarbinaryEncoded.INSTANCE) {
            return ScalarAttributeType.B;
        } else {
            throw new IllegalStateException("Invalid data type: " + pDataType.toString());
        }
    }

    /**
     * Return the column name from the given PColumn.
     * For tables, return name with quotes.
     * For indexes, return BSON_VALUE expression after trimming the : prefix.
     */
    public static String getColumnExprFromPCol(PColumn column, boolean useIndex) {
        String name = column.getName().toString();
        return useIndex ? name.substring(1) : CommonServiceUtils.getEscapedArgument(name);
    }

    /**
     * Return the column name from the given PColumn.
     * For tables, return name as is.
     * For indexes, extract column name from BSON_VALUE expression.
     */
    public static String getColumnNameFromPCol(PColumn column, boolean useIndex) {
        String keyName = column.getName().toString();
        return useIndex ? CommonServiceUtils.getKeyNameFromBsonValueFunc(keyName) : keyName;
    }

    public static String getKeyNameFromBsonValueFunc(String keyName) {
        if (keyName.contains("BSON_VALUE")) {
            keyName = keyName.split("BSON_VALUE")[1].split(",")[1];
            if (keyName.charAt(0) == '\'' && keyName.charAt(keyName.length() - 1) == '\'') {
                StringBuilder sb = new StringBuilder(keyName);
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(0);
                keyName = sb.toString();
            }
        } else if (keyName.startsWith(":")) {
            keyName = keyName.split(":")[1];
        }
        return keyName;
    }

    /**
     * Return BsonDocument representation of BSON Condition Expression based on dynamo condition
     * expression, expression attribute names and expression attribute values.
     */
    public static BsonDocument getBsonConditionExpressionDoc(String condExpr,
            Map<String, String> exprAttrNames, Map<String, Object> exprAttrVals) {
        BsonDocument exprAttrNamesDoc = getExpressionAttributeNamesDoc(exprAttrNames);
        BsonDocument conditionDoc = new BsonDocument();
        conditionDoc.put("$EXPR", new BsonString(condExpr));
        conditionDoc.put("$VAL", MapToBsonDocument.getBsonDocument(exprAttrVals));
        if (exprAttrNamesDoc != null) {
            conditionDoc.put("$KEYS", exprAttrNamesDoc);
        }
        return conditionDoc;
    }

    /**
     * Return string representation of BSON Condition Expression based on dynamo condition
     * expression, expression attribute names and expression attribute values.
     */
    public static String getBsonConditionExpressionString(String condExpr,
            Map<String, String> exprAttrNames, Map<String, Object> exprAttrVals) {
        return getBsonConditionExpressionDoc(condExpr, exprAttrNames, exprAttrVals).toJson();
    }

    public static BsonDocument getExpressionAttributeNamesDoc(Map<String, String> exprAttrNames) {
        if (exprAttrNames != null && !exprAttrNames.isEmpty()) {
            BsonDocument exprAttrNamesDoc = new BsonDocument();
            for (Map.Entry<String, String> entry : exprAttrNames.entrySet()) {
                String attrName = entry.getKey();
                String attrValue = entry.getValue();
                exprAttrNamesDoc.put(attrName, new BsonString(attrValue));
            }
            return exprAttrNamesDoc;
        }
        return null;
    }

    /**
     * Return BsonDocument representation of BSON Update Expression based on dynamo update
     * expression, expression attribute names and expression attribute values.
     */
    public static BsonDocument getBsonUpdateExpressionFromMap(String updateExpr,
            Map<String, String> exprAttrNames, Map<String, Object> exprAttrVals) {
        return UpdateExpressionToBson.toBsonUpdateDocument(updateExpr,
                MapToBsonDocument.getBsonDocument(exprAttrVals), exprAttrNames);
    }

    /**
     * Enclose given argument within double quotes. This is used to escape column/table names in queries.
     */
    public static String getEscapedArgument(String argument) {
        return DOUBLE_QUOTE + argument + DOUBLE_QUOTE;
    }

    /**
     * Replace all occurrences of aliases in the given String using expression attribute map.
     * @throws IllegalArgumentException if a placeholder is found that is not in the map,
     *         or if the expression contains placeholders but the map is null/empty
     */
    public static String replaceExpressionAttributeNames(String s,
                                                         Map<String, String> exprAttrNames) {
        Matcher m = EXPR_ATTR_NAME_PATTERN.matcher(s);
        StringBuffer result = new StringBuffer();
        while (m.find()) {
            String key = m.group(0);
            if (exprAttrNames == null || exprAttrNames.isEmpty()) {
                throw new IllegalArgumentException(
                    "Expression contains placeholder " + key + " but ExpressionAttributeNames is not provided");
            }
            String replacement = exprAttrNames.get(key);
            if (replacement == null) {
                throw new IllegalArgumentException(
                    "Expression attribute name " + key + " not found in ExpressionAttributeNames");
            }
            m.appendReplacement(result, Matcher.quoteReplacement(replacement));
        }
        m.appendTail(result);
        return result.toString();
    }

    public static Map<String, Object> getConsumedCapacity(final String tableName) {
        Map<String, Object> consumedCapacity = new HashMap<>();
        consumedCapacity.put(ApiMetadata.TABLE_NAME, tableName);

        Map<String, Double> tableCapacity = new HashMap<>();
        tableCapacity.put(ApiMetadata.READ_CAPACITY_UNITS, 1.0);
        tableCapacity.put(ApiMetadata.WRITE_CAPACITY_UNITS, 1.0);
        tableCapacity.put(ApiMetadata.CAPACITY_UNITS, 2.0);

        consumedCapacity.put(ApiMetadata.TABLE, tableCapacity);
        consumedCapacity.put(ApiMetadata.READ_CAPACITY_UNITS, 1.0);
        consumedCapacity.put(ApiMetadata.WRITE_CAPACITY_UNITS, 1.0);
        consumedCapacity.put(ApiMetadata.CAPACITY_UNITS, 2.0);

        return consumedCapacity;
    }

    /**
     * Convert AttributeUpdates (legacy parameter) to BsonDocument format
     * that BSON_UPDATE_EXPRESSION() understands. This method directly converts
     * AttributeUpdates to the internal BSON format without the intermediate
     * UpdateExpression string conversion.
     *
     * @param attributeUpdates The AttributeUpdates map from the request
     * @return BsonDocument equivalent to the AttributeUpdates
     */
    public static BsonDocument getBsonUpdateExpressionFromAttributeUpdates(
            Map<String, Object> attributeUpdates) {

        if (attributeUpdates == null || attributeUpdates.isEmpty()) {
            return new BsonDocument();
        }

        BsonDocument bsonDocument = new BsonDocument();
        BsonDocument setDoc = new BsonDocument();
        BsonDocument addDoc = new BsonDocument();
        BsonDocument deleteDoc = new BsonDocument();
        BsonDocument removeDoc = new BsonDocument();

        for (Map.Entry<String, Object> entry : attributeUpdates.entrySet()) {
            String attributeName = entry.getKey();
            Map<String, Object> attributeUpdate = (Map<String, Object>) entry.getValue();

            String action = (String) attributeUpdate.get("Action");
            Map<String, Object> value = (Map<String, Object>) attributeUpdate.get("Value");
            // Default action is PUT if not specified
            if (action == null) {
                action = "PUT";
            }
            switch (action.toUpperCase()) {
                case "PUT":
                    if (value != null) {
                        setDoc.put(attributeName, MapToBsonDocument.getValueFromMapVal(value));
                    }
                    break;

                case "ADD":
                    if (value != null) {
                        addDoc.put(attributeName, MapToBsonDocument.getValueFromMapVal(value));
                    }
                    break;

                case "DELETE":
                    if (value != null) {
                        deleteDoc.put(attributeName, MapToBsonDocument.getValueFromMapVal(value));
                    } else {
                        removeDoc.put(attributeName, new BsonNull());
                    }
                    break;

                default:
                    throw new RuntimeException(
                            "Invalid action: " + action + ". Valid actions are PUT, ADD, DELETE.");
            }
        }

        if (!setDoc.isEmpty()) {
            bsonDocument.put("$SET", setDoc);
        }
        if (!addDoc.isEmpty()) {
            bsonDocument.put("$ADD", addDoc);
        }
        if (!deleteDoc.isEmpty()) {
            bsonDocument.put("$DELETE_FROM_SET", deleteDoc);
        }
        if (!removeDoc.isEmpty()) {
            bsonDocument.put("$UNSET", removeDoc);
        }
        return bsonDocument;
    }

    /**
     * Convert legacy Expected and ConditionalOperator parameters to modern ConditionExpression.
     * This method supports the legacy DynamoDB conditional parameters and converts them to
     * the equivalent ConditionExpression format for internal processing.
     *
     * @param expected            The Expected map from the request (attribute conditions)
     * @param conditionalOperator The ConditionalOperator ("AND" or "OR", defaults to "AND")
     * @param exprAttrNames       Output map for expression attribute names (will be populated)
     * @param exprAttrValues      Output map for expression attribute values (will be populated)
     * @return ConditionExpression string equivalent to the Expected conditions
     */
    public static String convertExpectedToConditionExpression(Map<String, Object> expected,
            String conditionalOperator, Map<String, String> exprAttrNames,
            Map<String, Object> exprAttrValues) {
        return convertExpectedToConditionExpression(expected, conditionalOperator, exprAttrNames,
                exprAttrValues, 1, 1);
    }

    /**
     * Convert legacy Expected and ConditionalOperator parameters to modern ConditionExpression.
     * This overloaded version allows specifying starting counter values to avoid conflicts
     * when called multiple times with the same expression attribute maps.
     *
     * @param expected            The Expected map from the request (attribute conditions)
     * @param conditionalOperator The ConditionalOperator ("AND" or "OR", defaults to "AND")
     * @param exprAttrNames       Output map for expression attribute names (will be populated)
     * @param exprAttrValues      Output map for expression attribute values (will be populated)
     * @param startNameCounter    Starting value for name counter
     * @param startValueCounter   Starting value for value counter
     * @return ConditionExpression string equivalent to the Expected conditions
     */
    public static String convertExpectedToConditionExpression(Map<String, Object> expected,
            String conditionalOperator, Map<String, String> exprAttrNames,
            Map<String, Object> exprAttrValues, int startNameCounter, int startValueCounter) {

        if (expected == null || expected.isEmpty()) {
            return null;
        }

        List<String> conditions = new ArrayList<>();
        int nameCounter = startNameCounter;
        int valueCounter = startValueCounter;

        for (Map.Entry<String, Object> entry : expected.entrySet()) {
            String attributeName = entry.getKey();
            Map<String, Object> expectedValue = (Map<String, Object>) entry.getValue();

            // Generate expression attribute name
            String nameAlias = "#field_n___" + nameCounter++;
            exprAttrNames.put(nameAlias, attributeName);

            // Parse the expected value structure
            List<Object> attributeValueList =
                    (List<Object>) expectedValue.get("AttributeValueList");
            String comparisonOperator = (String) expectedValue.get("ComparisonOperator");
            Map<String, Object> value = (Map<String, Object>) expectedValue.get("Value");
            Boolean exists = (Boolean) expectedValue.get("Exists");

            // Handle the case where ComparisonOperator is NOT_NULL or NULL (for exists checks)
            if ("NOT_NULL".equals(comparisonOperator) || "NULL".equals(comparisonOperator)) {
                exists = "NOT_NULL".equals(comparisonOperator);
                value = null; // Clear value for exists checks
            }

            String condition =
                    buildConditionForAttribute(nameAlias, attributeValueList, comparisonOperator,
                            value, exists, exprAttrValues, valueCounter);

            if (condition != null) {
                conditions.add(condition);
                // Update valueCounter based on how many values were used
                valueCounter +=
                        getValueCountUsed(comparisonOperator, attributeValueList, value, exists);
            }
        }

        if (conditions.isEmpty()) {
            return null;
        }

        String operator = "OR".equalsIgnoreCase(conditionalOperator) ? " OR " : " AND ";

        return "(" + String.join(operator, conditions) + ")";
    }

    public static int getExpectedNameCount(Map<String, Object> expected) {
        if (expected == null || expected.isEmpty()) {
            return 0;
        }
        return expected.size();
    }

    private static String buildConditionForAttribute(String nameAlias,
            List<Object> attributeValueList, String comparisonOperator, Map<String, Object> value,
            Boolean exists, Map<String, Object> exprAttrValues, int valueCounter) {

        // Handle EXISTS/NOT_EXISTS conditions
        if (exists != null) {
            if (exists) {
                // exists(true) with value means: attribute exists AND equals the value
                if (value != null) {
                    String valueAlias = ":field_v___" + valueCounter;
                    exprAttrValues.put(valueAlias, value);
                    return "(" + nameAlias + " = " + valueAlias + ")";
                } else {
                    // exists(true) without value means: attribute just needs to exist
                    return "attribute_exists(" + nameAlias + ")";
                }
            } else {
                // exists(false) always means attribute should not exist (value is ignored)
                return "attribute_not_exists(" + nameAlias + ")";
            }
        }

        // Handle Value-based conditions (legacy single value format)
        if (value != null) {
            String valueAlias = ":field_v___" + valueCounter;
            exprAttrValues.put(valueAlias, value);

            // Default comparison operator is EQ if not specified
            if (comparisonOperator == null) {
                comparisonOperator = "EQ";
            }

            return buildComparisonCondition(nameAlias, comparisonOperator, valueAlias, null);
        }

        // Handle AttributeValueList-based conditions (can have multiple values)
        if (attributeValueList != null && comparisonOperator != null) {
            return buildAttributeValueListCondition(nameAlias, comparisonOperator,
                    attributeValueList, exprAttrValues, valueCounter);
        }

        return null;
    }

    private static String buildComparisonCondition(String nameAlias, String comparisonOperator,
            String valueAlias1, String valueAlias2) {

        switch (comparisonOperator.toUpperCase()) {
            case "EQ":
                return nameAlias + " = " + valueAlias1;
            case "NE":
                return nameAlias + " <> " + valueAlias1;
            case "LT":
                return nameAlias + " < " + valueAlias1;
            case "LE":
                return nameAlias + " <= " + valueAlias1;
            case "GT":
                return nameAlias + " > " + valueAlias1;
            case "GE":
                return nameAlias + " >= " + valueAlias1;
            case "BETWEEN":
                return nameAlias + " BETWEEN " + valueAlias1 + " AND " + valueAlias2;
            case "IN":
                return nameAlias + " IN (" + valueAlias1 + ")";
            case "BEGINS_WITH":
                return "begins_with(" + nameAlias + ", " + valueAlias1 + ")";
            case "CONTAINS":
                return "contains(" + nameAlias + ", " + valueAlias1 + ")";
            case "NOT_CONTAINS":
                return "NOT contains(" + nameAlias + ", " + valueAlias1 + ")";
            case "NULL":
                return "attribute_not_exists(" + nameAlias + ")";
            case "NOT_NULL":
                return "attribute_exists(" + nameAlias + ")";
            default:
                throw new RuntimeException(
                        "Unsupported comparison operator: " + comparisonOperator);
        }
    }

    private static String buildAttributeValueListCondition(String nameAlias,
            String comparisonOperator, List<Object> attributeValueList,
            Map<String, Object> exprAttrValues, int valueCounter) {

        switch (comparisonOperator.toUpperCase()) {
            case "IN": {
                // IN operator with multiple values
                List<String> valueAliases = new ArrayList<>();
                int counter = valueCounter;
                for (Object value : attributeValueList) {
                    String valueAlias = ":field_v___" + counter++;
                    exprAttrValues.put(valueAlias, value);
                    valueAliases.add(valueAlias);
                }
                return nameAlias + " IN (" + String.join(", ", valueAliases) + ")";
            }

            case "BETWEEN": {
                // BETWEEN requires exactly 2 values
                if (attributeValueList.size() != 2) {
                    throw new RuntimeException("BETWEEN operator requires exactly 2 values");
                }
                List<String> betweenValues = new ArrayList<>();
                int betweenCounter = valueCounter;
                for (Object value : attributeValueList) {
                    String valueAlias = ":field_v___" + betweenCounter++;
                    exprAttrValues.put(valueAlias, value);
                    betweenValues.add(valueAlias);
                }
                return nameAlias + " BETWEEN " + betweenValues.get(0) + " AND " + betweenValues.get(
                        1);
            }

            default: {
                // For other operators, use the first value
                String valueAlias = ":field_v___" + valueCounter;
                Object firstValue = attributeValueList.get(0);
                exprAttrValues.put(valueAlias, firstValue);
                return buildComparisonCondition(nameAlias, comparisonOperator, valueAlias, null);
            }
        }
    }

    private static int getValueCountUsed(String comparisonOperator, List<Object> attributeValueList,
            Map<String, Object> value, Boolean exists) {
        // exists(true) with value consumes one value counter
        if (exists != null && exists && value != null) {
            return 1;
        }
        // exists(false) or exists(true) without value doesn't consume value counter
        if (exists != null) {
            return 0;
        }
        if (value != null) {
            return 1;
        }
        if (attributeValueList != null) {
            switch (comparisonOperator != null ? comparisonOperator.toUpperCase() : "EQ") {
                case "IN":
                    return attributeValueList.size();
                case "BETWEEN":
                    return 2;
                default:
                    return 1;
            }
        }
        return 0;
    }

    /**
     * Handles legacy projection parameter conversion to modern equivalent.
     * Converts AttributesToGet list to ProjectionExpression string.
     *
     * @param request The request map to modify
     */
    public static void handleLegacyProjectionConversion(Map<String, Object> request) {
        List<String> attributesToGet = (List<String>) request.get(ApiMetadata.ATTRIBUTES_TO_GET);
        if (attributesToGet != null) {
            String projectionExpression =
                    convertAttributesToGetToProjectionExpression(attributesToGet);
            if (projectionExpression != null) {
                request.put(ApiMetadata.PROJECTION_EXPRESSION, projectionExpression);
            }
            request.remove(ApiMetadata.ATTRIBUTES_TO_GET);
        }
    }

    /**
     * Convert AttributesToGet list to a ProjectionExpression string.
     * AttributesToGet is a legacy parameter that contains a list of attribute names.
     *
     * @param attributesToGet List of attribute names to project
     * @return A ProjectionExpression string with comma-separated attribute names
     */
    private static String convertAttributesToGetToProjectionExpression(
            List<String> attributesToGet) {
        if (attributesToGet == null || attributesToGet.isEmpty()) {
            return null;
        }
        return String.join(", ", attributesToGet);
    }

    /**
     * Evaluate if a condition expression can be satisfied on a non-existing item.
     * Returns true if the condition PASSES when evaluated against an empty document.
     * <p>
     * <b>Usage by UpdateItemService:</b>
     * Determines whether UPDATE (allows creation) or UPDATE_ONLY (existing only) query should be used.
     * If condition passes on empty doc, the item can be created if it doesn't exist.
     * <p>
     * <b>Usage by DeleteItemService:</b>
     * Determines behavior when DELETE returns no row. If condition passes on empty doc,
     * a missing row is valid (no-op). If condition fails on empty doc, throw
     * ConditionalCheckFailedException.
     * TODO: When condition passes on empty doc but no row was
     * TODO: returned, DeleteItemService must distinguish between
     * TODO: "row didn't exist" (success) vs "row existed but condition failed" (throw exception).
     *
     * @param condExpr the condition expression string
     * @param exprAttrNames expression attribute names map
     * @return true if condition passes on empty document, false otherwise
     */
    public static boolean evaluateConditionOnNonExistingItem(String condExpr,
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
}
