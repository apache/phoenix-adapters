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

package org.apache.phoenix.ddb.bson;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BsonDocumentConversionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(BsonDocumentConversionUtil.class);

    private static void updateNestedAttributeVal(BsonValue value, int idx,
            final String documentFieldKey, BsonValue newValue) {
        int curIdx = idx;
        if (documentFieldKey.charAt(curIdx) == '.') {
            BsonDocument nestedDocument =
                    value != null && value.isDocument() ? (BsonDocument) value : null;
            BsonDocument newNestedDocument =
                    newValue != null && newValue.isDocument() ? (BsonDocument) newValue : null;
            if (nestedDocument == null || newNestedDocument == null) {
                LOGGER.warn("Incorrect access. Should have found nested map for value: {}", value);
                return;
            }
            curIdx++;
            StringBuilder sb = new StringBuilder();
            for (; curIdx < documentFieldKey.length(); curIdx++) {
                if (documentFieldKey.charAt(curIdx) == '.') {
                    BsonValue nestedValue = nestedDocument.get(sb.toString());
                    if (nestedValue == null) {
                        return;
                    }
                    newNestedDocument.putIfAbsent(sb.toString(), new BsonDocument());
                    updateNestedAttributeVal(nestedValue, curIdx, documentFieldKey,
                            newNestedDocument.get(sb.toString()));
                    return;
                } else if (documentFieldKey.charAt(curIdx) == '[') {
                    BsonValue nestedValue = nestedDocument.get(sb.toString());
                    if (nestedValue == null) {
                        return;
                    }
                    newNestedDocument.putIfAbsent(sb.toString(), new BsonArray());
                    updateNestedAttributeVal(nestedValue, curIdx, documentFieldKey,
                            newNestedDocument.get(sb.toString()));
                    return;
                } else {
                    sb.append(documentFieldKey.charAt(curIdx));
                }
            }
            newNestedDocument.put(sb.toString(), nestedDocument.get(sb.toString()));
            return;
        } else if (documentFieldKey.charAt(curIdx) == '[') {
            curIdx++;
            StringBuilder arrayIdxStr = new StringBuilder();
            while (documentFieldKey.charAt(curIdx) != ']') {
                arrayIdxStr.append(documentFieldKey.charAt(curIdx));
                curIdx++;
            }
            curIdx++;
            int arrayIdx = Integer.parseInt(arrayIdxStr.toString());
            BsonArray nestedArray = value != null && value.isArray() ? (BsonArray) value : null;
            BsonArray newNestedArray =
                    newValue != null && newValue.isArray() ? (BsonArray) newValue : null;
            if (nestedArray == null || newNestedArray == null) {
                LOGGER.warn("Incorrect access. Should have found nested list for value: {}", value);
                return;
            }
            if (arrayIdx >= nestedArray.size()) {
                LOGGER.warn("Incorrect access. Nested list size {} is less than attempted index "
                        + "access at {}", nestedArray.size(), arrayIdx);
                return;
            }
            BsonValue valueAtIdx = nestedArray.get(arrayIdx);
            if (newNestedArray.size() <= arrayIdx) {
                for (int i = newNestedArray.size(); i <= arrayIdx; i++) {
                    newNestedArray.add(null);
                }
            }
            if (curIdx == documentFieldKey.length()) {
                newNestedArray.set(arrayIdx, valueAtIdx);
                return;
            }
            if (newNestedArray.get(arrayIdx) == null) {
                if (documentFieldKey.charAt(curIdx) == '.') {
                    newNestedArray.set(arrayIdx, new BsonDocument());
                } else if (documentFieldKey.charAt(curIdx) == '[') {
                    newNestedArray.set(arrayIdx, new BsonArray());
                }
            }
            updateNestedAttributeVal(valueAtIdx, curIdx, documentFieldKey,
                    newNestedArray.get(arrayIdx));
            return;
        }
        LOGGER.warn("This is erroneous case. getNestedFieldVal should not be used for "
                + "top level document fields");
    }

    /**
     * Traverse the given bsonDocument and extract the values for given attributeName into result.
     */
    public static void updateNewBsonDocumentByFieldKeyValue(final String documentFieldKey,
            final BsonDocument bsonDocument, final BsonDocument newDocument) {
        if ((documentFieldKey.contains(".") && bsonDocument.get(documentFieldKey) == null)
                || documentFieldKey.contains("[")) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < documentFieldKey.length(); i++) {
                if (documentFieldKey.charAt(i) == '.') {
                    BsonValue value = bsonDocument.get(sb.toString());
                    if (value == null) {
                        return;
                    }
                    newDocument.putIfAbsent(sb.toString(), new BsonDocument());
                    updateNestedAttributeVal(value, i, documentFieldKey,
                            newDocument.get(sb.toString()));
                    return;
                } else if (documentFieldKey.charAt(i) == '[') {
                    BsonValue value = bsonDocument.get(sb.toString());
                    if (value == null) {
                        return;
                    }
                    newDocument.putIfAbsent(sb.toString(), new BsonArray());
                    updateNestedAttributeVal(value, i, documentFieldKey,
                            newDocument.get(sb.toString()));
                    return;
                } else {
                    sb.append(documentFieldKey.charAt(i));
                }
            }
        } else {
            newDocument.put(documentFieldKey, bsonDocument.get(documentFieldKey));
        }
    }

    public static void removeNullListElements(BsonArray listValue) {
        listValue.removeIf(Objects::isNull);
        Iterator<BsonValue> iterator = listValue.iterator();
        while (iterator.hasNext()) {
            BsonValue value = iterator.next();
            if (value.isDocument()) {
                BsonDocument doc = value.asDocument();
                removeNullListElements(doc);
                if (doc.isEmpty()) {
                    iterator.remove();
                }
            } else if (value.isArray()) {
                BsonArray array = value.asArray();
                removeNullListElements(array);
                if (array.isEmpty()) {
                    iterator.remove();
                }
            }
        }
    }

    public static void removeNullListElements(BsonDocument document) {
        List<String> keysToRemove = new ArrayList<>();
        document.forEach((key, value) -> {
            if (value.isDocument()) {
                BsonDocument doc = value.asDocument();
                removeNullListElements(doc);
                if (doc.isEmpty()) {
                    keysToRemove.add(key);
                }
            } else if (value.isArray()) {
                BsonArray array = value.asArray();
                removeNullListElements(array);
                if (array.isEmpty()) {
                    keysToRemove.add(key);
                }
            }
        });
        keysToRemove.forEach(document::remove);
    }
}
