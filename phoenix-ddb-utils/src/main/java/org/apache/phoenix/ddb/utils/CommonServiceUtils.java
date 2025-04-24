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

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.ddb.bson.MapToBsonDocument;
import org.apache.phoenix.ddb.bson.UpdateExpressionDdbToBson;
import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.Map;

/**
 * Common utilities to be used by phoenixDBClientV2 APIs.
 */
public class CommonServiceUtils {

    public static final String DOUBLE_QUOTE = "\"";
    public static final String HASH = "#";

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
     * Return string representation of BSON Condition Expression based on dynamo condition
     * expression, expression attribute names and expression attribute values.
     */
    public static String getBsonConditionExpression(String condExpr,
                                                       Map<String, String> exprAttrNames,
                                                       Map<String, AttributeValue> exprAttrVals) {

        // plug expression attribute names in $EXPR
        condExpr = replaceExpressionAttributeNames(condExpr, exprAttrNames);

        // BSON_CONDITION_EXPRESSION
        BsonDocument conditionDoc = new BsonDocument();
        conditionDoc.put("$EXPR", new BsonString(condExpr));
        conditionDoc.put("$VAL", DdbAttributesToBsonDocument.getBsonDocument(exprAttrVals));

        return conditionDoc.toJson();
    }

    /**
     * Return string representation of BSON Condition Expression based on dynamo condition
     * expression, expression attribute names and expression attribute values.
     */
    public static String getBsonConditionExpressionFromMap(String condExpr,
            Map<String, String> exprAttrNames, Map<String, Object> exprAttrVals) {

        // plug expression attribute names in $EXPR
        condExpr = replaceExpressionAttributeNames(condExpr, exprAttrNames);

        // BSON_CONDITION_EXPRESSION
        BsonDocument conditionDoc = new BsonDocument();
        conditionDoc.put("$EXPR", new BsonString(condExpr));
        conditionDoc.put("$VAL", MapToBsonDocument.getBsonDocument(exprAttrVals));

        return conditionDoc.toJson();
    }

    /**
     * Return BsonDocument representation of BSON Update Expression based on dynamo update
     * expression, expression attribute names and expression attribute values.
     */
    public static BsonDocument getBsonUpdateExpression(String updateExpr,
                                                       Map<String, String> exprAttrNames,
                                                       Map<String, AttributeValue> exprAttrVals) {

        if (StringUtils.isEmpty(updateExpr)) return new BsonDocument();
        updateExpr = replaceExpressionAttributeNames(updateExpr, exprAttrNames);
        return UpdateExpressionDdbToBson
                .getBsonDocumentForUpdateExpression(updateExpr,
                        DdbAttributesToBsonDocument.getBsonDocument(exprAttrVals));
    }

    /**
     * Enclose given argument within double quotes. This is used to escape column names in queries.
     */
    public static String getEscapedArgument(String argument) {
        return DOUBLE_QUOTE + argument + DOUBLE_QUOTE;
    }

    /**
     * Replace all occurrences of aliases in the given String using expression attribute map.
     */
    public static String replaceExpressionAttributeNames(String s,
                                                       Map<String, String> exprAttrNames) {
        if (exprAttrNames == null || !s.contains(HASH)) return s;
        for (String k : exprAttrNames.keySet()) {
            s = StringUtils.replace(s, k, exprAttrNames.get(k));
        }
        return s;
    }
}
