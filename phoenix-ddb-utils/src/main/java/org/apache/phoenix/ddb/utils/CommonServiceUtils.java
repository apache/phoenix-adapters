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

import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.commons.lang3.StringUtils;
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
import java.util.HashMap;

/**
 * Common utilities to be used by phoenixDBClientV2 APIs.
 */
public class CommonServiceUtils {

    public static final String DOUBLE_QUOTE = "\"";
    public static final String HASH = "#";

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
    public static String getBsonConditionExpressionFromMap(String condExpr,
            Map<String, String> exprAttrNames, Map<String, Object> exprAttrVals) {
        BsonDocument exprAttrNamesDoc = getExpressionAttributeNamesDoc(exprAttrNames);
        return getBsonConditionExpressionUtil(condExpr, exprAttrNamesDoc, exprAttrVals);
    }

    public static String getBsonConditionExpressionUtil(String condExpr,
            BsonDocument exprAttrNamesDoc, Map<String, Object> exprAttrVals) {
        BsonDocument conditionDoc = new BsonDocument();
        conditionDoc.put("$EXPR", new BsonString(condExpr));
        conditionDoc.put("$VAL", MapToBsonDocument.getBsonDocument(exprAttrVals));
        if (exprAttrNamesDoc != null) {
            conditionDoc.put("$KEYS", exprAttrNamesDoc);
        }
        return conditionDoc.toJson();
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
        if (StringUtils.isEmpty(updateExpr))
            return new BsonDocument();
        updateExpr = replaceExpressionAttributeNames(updateExpr, exprAttrNames);
        return UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(updateExpr,
                MapToBsonDocument.getBsonDocument(exprAttrVals));
    }

    /**
     * Enclose given argument within double quotes. This is used to escape column/table names in queries.
     */
    public static String getEscapedArgument(String argument) {
        return DOUBLE_QUOTE + argument + DOUBLE_QUOTE;
    }

    /**
     * Replace all occurrences of aliases in the given String using expression attribute map.
     */
    public static String replaceExpressionAttributeNames(String s,
            Map<String, String> exprAttrNames) {
        if (exprAttrNames == null || !s.contains(HASH)) {
            return s;
        }
        for (String k : exprAttrNames.keySet()) {
            s = StringUtils.replace(s, k, exprAttrNames.get(k));
        }
        return s;
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
}
