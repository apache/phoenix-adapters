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

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
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
    BsonDocument newDocument = new BsonDocument();
    for (String attribute : attributesToProject) {
      BsonDocumentConversionUtil.updateNewBsonDocumentByFieldKeyValue(attribute, bsonDocument,
              newDocument);
    }
    BsonDocumentConversionUtil.removeNullListElements(newDocument);
    return getFullItem(newDocument);
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
                  BsonNumberConversionUtil.numberToString(
                          BsonNumberConversionUtil.getNumberFromBsonNumber((BsonNumber) val))));
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

  private static AttributeValue getNumber(BsonNumber bsonNumber) {
    AttributeValue attributeValue = AttributeValue.builder()
            .n(BsonNumberConversionUtil.numberToString(
                    BsonNumberConversionUtil.getNumberFromBsonNumber(bsonNumber))).build();
    return attributeValue;
  }

}
