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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Conversion from BsonDocument to DynamoDB item.
 */
public class BsonDocumentToDdbAttributes {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(BsonDocumentToDdbAttributes.class);

  /**
   * TODO : This implementation has problems, refactor for V2 SDK.
   * Convert the given BsonDocument into DDB item.
   * This retrieves only the attributes provided in the list attributesToProject.
   *
   * @param bsonDocument The BsonDocument.
   * @return DDB item as attribute map.
   */
  public static Map<String, AttributeValue> getProjectedItem(final BsonDocument bsonDocument,
                                                             List<String> attributesToProject) {
    if (attributesToProject == null || attributesToProject.isEmpty()) {
      return getFullItem(bsonDocument);
    }
    Map<String, AttributeValue> map = new HashMap<>();
    for (String attribute : attributesToProject) {
      updateNewBsonDocumentByFieldKeyValue(attribute, bsonDocument, map);
    }
    removeNullListElements(map);
    return map;
  }

  private static void removeNullListElements(Map<String, AttributeValue> map) {
    for (Map.Entry<String, AttributeValue> entry : map.entrySet()) {
      if (entry.getValue().hasM()) {
        removeNullListElements(entry.getValue().m());
      } else if (entry.getValue().hasL()) {
        removeNullListElements(entry.getValue());
      }
    }
  }

  private static void removeNullListElements(AttributeValue listValue) {
    List<AttributeValue> curList = listValue.l();
    List<AttributeValue> newList =
        curList.stream().filter(Objects::nonNull).collect(Collectors.toList());
    listValue = listValue.toBuilder().l(newList).build();
    listValue.l().forEach(attributeValue -> {
      if (attributeValue.hasM()) {
        removeNullListElements(attributeValue.m());
      } else if (attributeValue.hasL()) {
        removeNullListElements(attributeValue);
      }
    });
  }

  /**
   * Convert the given BsonDocument into DDB item. This retrieves the full item, converting all
   * Document attributes to DDB item attributes.
   *
   * @param bsonDocument The BsonDocument.
   * @return DDB item as attribute map.
   */
  public static Map<String, AttributeValue> getFullItem(final BsonDocument bsonDocument) {
    Map<String, AttributeValue> attributesMap = new HashMap<>();
    if (bsonDocument == null) return attributesMap;
    for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
      updateMapEntries(entry, attributesMap);
    }
    return attributesMap;
  }

  private static void updateMapEntries(Map.Entry<String, BsonValue> entry,
      Map<String, AttributeValue> item) {
    BsonValue bsonValue = entry.getValue();
    item.put(entry.getKey(), getAttributeValue(bsonValue));
  }

  private static AttributeValue getAttributeValue(BsonValue bsonValue) {
    if (bsonValue.isString()) {
      return AttributeValue.builder().s(((BsonString) bsonValue).getValue()).build();
    } else if (bsonValue.isNumber() || bsonValue.isDecimal128()) {
      return getNumber((BsonNumber) bsonValue);
    } else if (bsonValue.isBinary()) {
      BsonBinary bsonBinary = (BsonBinary) bsonValue;
      return AttributeValue.builder().b(SdkBytes.fromByteArray(bsonBinary.getData())).build();
    } else if (bsonValue.isBoolean()) {
      return AttributeValue.builder().bool(((BsonBoolean) bsonValue).getValue()).build();
    } else if (bsonValue.isNull()) {
      return AttributeValue.builder().nul(true).build();
    } else if (bsonValue.isDocument()) {
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      if (bsonDocument.size() == 1 && bsonDocument.containsKey("$set")) {
        BsonValue value = bsonDocument.get("$set");
        if (!value.isArray()) {
          throw new IllegalArgumentException("$set is reserved for Set datatype");
        }
        BsonArray bsonArray = (BsonArray) value;
        if (bsonArray.isEmpty()) {
          throw new IllegalArgumentException("Set cannot be empty");
        }
        BsonValue firstElement = bsonArray.get(0);
        if (firstElement.isString()) {
          List<String> stringSet = new ArrayList<>();
          bsonArray.getValues().forEach(val -> stringSet.add(((BsonString) val).getValue()));
          return AttributeValue.builder().ss(stringSet).build();
        } else if (firstElement.isNumber() || firstElement.isDecimal128()) {
          List<String> numberSet = new ArrayList<>();
          bsonArray.getValues().forEach(val -> numberSet.add(
                  numberToString(getNumberFromBsonNumber((BsonNumber) val))));
          return AttributeValue.builder().ns(numberSet).build();
        } else if (firstElement.isBinary()) {
          List<SdkBytes> binarySet = new ArrayList<>();
          bsonArray.getValues().forEach(val -> binarySet.add(
                  SdkBytes.fromByteArray(((BsonBinary) val).getData())));
          return AttributeValue.builder().bs(binarySet).build();
        }
        throw new IllegalArgumentException("Invalid set type");
      } else {
        Map<String, AttributeValue> map = new HashMap<>();
        for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
          updateMapEntries(entry, map);
        }
        return AttributeValue.builder().m(map).build();
      }
    } else if (bsonValue.isArray()) {
      BsonArray bsonArray = (BsonArray) bsonValue;
      List<AttributeValue> attributeValueList = new ArrayList<>();
      for (BsonValue bsonArrayValue : bsonArray.getValues()) {
        attributeValueList.add(getAttributeValue(bsonArrayValue));
      }
      return AttributeValue.builder().l(attributeValueList).build();
    }
    LOGGER.error("Invalid data type of BsonValue: {}", bsonValue);
    throw new RuntimeException("Invalid data type of BsonValue");
  }

  private static void updateAttributeValue(BsonValue bsonValue, AttributeValue.Builder targetValue) {
    if (bsonValue.isString()) {
      targetValue.s(((BsonString) bsonValue).getValue());
    } else if (bsonValue.isNumber() || bsonValue.isDecimal128()) {
      targetValue.n(numberToString(getNumberFromBsonNumber((BsonNumber) bsonValue)));
    } else if (bsonValue.isBinary()) {
      BsonBinary bsonBinary = (BsonBinary) bsonValue;
      targetValue.b(SdkBytes.fromByteArray(bsonBinary.getData()));
    } else if (bsonValue.isBoolean()) {
      targetValue.bool(((BsonBoolean) bsonValue).getValue());
    } else if (bsonValue.isNull()) {
      targetValue.nul(true);
    } else if (bsonValue.isDocument()) {
      BsonDocument bsonDocument = (BsonDocument) bsonValue;
      if (bsonDocument.size() == 1 && bsonDocument.containsKey("$set")) {
        BsonValue value = bsonDocument.get("$set");
        if (!value.isArray()) {
          throw new IllegalArgumentException("$set is reserved for Set datatype");
        }
        BsonArray bsonArray = (BsonArray) value;
        if (bsonArray.isEmpty()) {
          throw new IllegalArgumentException("Set cannot be empty");
        }
        BsonValue firstElement = bsonArray.get(0);
        if (firstElement.isString()) {
          List<String> stringSet = new ArrayList<>();
          bsonArray.getValues().forEach(val -> stringSet.add(((BsonString) val).getValue()));
          targetValue.ss(stringSet);
        } else if (firstElement.isNumber() || firstElement.isDecimal128()) {
          List<String> numberSet = new ArrayList<>();
          bsonArray.getValues().forEach(val -> numberSet.add(
                  numberToString(getNumberFromBsonNumber((BsonNumber) val))));
          targetValue.ns(numberSet);
        } else if (firstElement.isBinary()) {
          List<SdkBytes> binarySet = new ArrayList<>();
          bsonArray.getValues().forEach(val -> binarySet.add(
                  SdkBytes.fromByteArray(((BsonBinary) val).getData())));
          targetValue.bs(binarySet);
        } else {
          throw new IllegalArgumentException("Invalid set type");
        }
      } else {
        Map<String, AttributeValue> map = new HashMap<>();
        for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
          updateMapEntries(entry, map);
        }
        targetValue.m(map);
      }
    } else if (bsonValue.isArray()) {
      BsonArray bsonArray = (BsonArray) bsonValue;
      List<AttributeValue> attributeValueList = new ArrayList<>();
      for (BsonValue bsonArrayValue : bsonArray.getValues()) {
        attributeValueList.add(getAttributeValue(bsonArrayValue));
      }
      targetValue.l(attributeValueList);
    } else {
      LOGGER.error("Invalid data type of BsonValue: {}", bsonValue);
      throw new RuntimeException("Invalid data type of BsonValue");
    }
  }

  private static AttributeValue getNumber(BsonNumber bsonNumber) {
    AttributeValue attributeValue = AttributeValue.builder()
            .n(numberToString(getNumberFromBsonNumber(bsonNumber))).build();
    return attributeValue;
  }

  private static Number getNumberFromBsonNumber(BsonNumber bsonNumber) {
    if (bsonNumber instanceof BsonInt32) {
      return ((BsonInt32) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonInt64) {
      return ((BsonInt64) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonDouble) {
      return ((BsonDouble) bsonNumber).getValue();
    } else if (bsonNumber instanceof BsonDecimal128) {
      return ((BsonDecimal128) bsonNumber).getValue().bigDecimalValue();
    } else {
      LOGGER.error("Unsupported BsonNumber type: {}", bsonNumber);
      throw new IllegalArgumentException("Unsupported BsonNumber type: " + bsonNumber);
    }
  }

  /**
   * Convert the given Number to String.
   *
   * @param number The Number object.
   * @return String represented number value.
   */
  private static String numberToString(Number number) {
    if (number instanceof Integer || number instanceof Short || number instanceof Byte) {
      return Integer.toString(number.intValue());
    } else if (number instanceof Long) {
      return Long.toString(number.longValue());
    } else if (number instanceof Double) {
      return Double.toString(number.doubleValue());
    } else if (number instanceof Float) {
      return Float.toString(number.floatValue());
    }
    throw new RuntimeException("Number type is not known for number: " + number);
  }

  /**
   * Traverse the given bsonDocument and extract the values for given attributeName into result.
   */
  private static void updateNewBsonDocumentByFieldKeyValue(final String documentFieldKey,
                                                           final BsonDocument bsonDocument, final Map<String, AttributeValue> map) {
    if ((documentFieldKey.contains(".") && bsonDocument.get(documentFieldKey) == null)
            || documentFieldKey.contains("[")) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < documentFieldKey.length(); i++) {
        if (documentFieldKey.charAt(i) == '.') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            return;
          }
          map.putIfAbsent(sb.toString(), AttributeValue.builder().m(new HashMap<>()).build());
          updateNestedAttributeVal(value, i, documentFieldKey, map.get(sb.toString()).toBuilder());
          return;
        } else if (documentFieldKey.charAt(i) == '[') {
          BsonValue value = bsonDocument.get(sb.toString());
          if (value == null) {
            return;
          }
          map.putIfAbsent(sb.toString(), AttributeValue.builder().l(new ArrayList<>()).build());
          updateNestedAttributeVal(value, i, documentFieldKey, map.get(sb.toString()).toBuilder());
          return;
        } else {
          sb.append(documentFieldKey.charAt(i));
        }
      }
    } else {
      map.put(documentFieldKey, getAttributeValue(bsonDocument.get(documentFieldKey)));
    }
  }

  public static void updateNestedAttributeVal(BsonValue value, int idx,
      final String documentFieldKey, AttributeValue.Builder attributeValue) {
    if (idx == documentFieldKey.length()) {
      updateAttributeValue(value, attributeValue);
      return;
    }
    int curIdx = idx;
    if (documentFieldKey.charAt(curIdx) == '.') {
      BsonDocument nestedDocument =
              value != null && value.isDocument() ? (BsonDocument) value : null;
      if (nestedDocument == null) {
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
          final Map<String, AttributeValue> map = attributeValue.build().m();
          map.putIfAbsent(sb.toString(), AttributeValue.builder().m(new HashMap<>()).build());
          updateNestedAttributeVal(nestedValue, curIdx, documentFieldKey, map.get(sb.toString()).toBuilder());
          return;
        } else if (documentFieldKey.charAt(curIdx) == '[') {
          BsonValue nestedValue = nestedDocument.get(sb.toString());
          if (nestedValue == null) {
            return;
          }
          final Map<String, AttributeValue> map = attributeValue.build().m();
          map.putIfAbsent(sb.toString(), AttributeValue.builder().l(new ArrayList<>()).build());
          updateNestedAttributeVal(nestedValue, curIdx, documentFieldKey, map.get(sb.toString()).toBuilder());
          return;
        } else {
          sb.append(documentFieldKey.charAt(curIdx));
        }
      }
      attributeValue.build().m()
              .put(sb.toString(), getAttributeValue(nestedDocument.get(sb.toString())));
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
      if (nestedArray == null) {
        LOGGER.warn("Incorrect access. Should have found nested list for value: {}", value);
        return;
      }
      if (arrayIdx >= nestedArray.size()) {
        LOGGER.warn(
            "Incorrect access. Nested list size {} is less than attempted index access at {}",
            nestedArray.size(), arrayIdx);
        return;
      }
      BsonValue valueAtIdx = nestedArray.get(arrayIdx);
      List<AttributeValue> list = attributeValue.build().l();
      if (list.size() <= arrayIdx) {
        for (int i = list.size(); i <= arrayIdx; i++) {
          list.add(null);
        }
      }
      if (curIdx == documentFieldKey.length()) {
        list.set(arrayIdx, getAttributeValue(valueAtIdx));
        return;
      }
      if (list.get(arrayIdx) == null) {
        if (documentFieldKey.charAt(curIdx) == '.') {
          list.set(arrayIdx, AttributeValue.builder().m(new HashMap<>()).build());
        } else if (documentFieldKey.charAt(curIdx) == '[') {
          list.set(arrayIdx, AttributeValue.builder().l(new ArrayList<>()).build());
        }
      }
      updateNestedAttributeVal(valueAtIdx, curIdx, documentFieldKey, list.get(arrayIdx).toBuilder());
      return;
    }
    LOGGER.warn("This is erroneous case. getNestedFieldVal should not be used for "
            + "top level document fields");
  }

}
