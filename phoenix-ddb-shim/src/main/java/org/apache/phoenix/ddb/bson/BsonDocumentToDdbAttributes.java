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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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

  // TODO: Utility to convert only specific Bson field key to Attribute value rather than
  // deserializing the whole document. To be used by Query/Scan APIs with projection expression.

  /**
   * Convert the given BsonDocument into DDB item. This retrieves the full item, converting all
   * Document attributes to DDB item attributes.
   *
   * @param bsonDocument The BsonDocument.
   * @return DDB item as attribute map.
   */
  public static Map<String, AttributeValue> getFullItem(final BsonDocument bsonDocument) {
    Map<String, AttributeValue> attributesMap = new HashMap<>();
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
      return new AttributeValue().withS(((BsonString) bsonValue).getValue());
    } else if (bsonValue.isNumber() || bsonValue.isDecimal128()) {
      return getNumber((BsonNumber) bsonValue);
    } else if (bsonValue.isBinary()) {
      BsonBinary bsonBinary = (BsonBinary) bsonValue;
      return new AttributeValue().withB(ByteBuffer.wrap(bsonBinary.getData()));
    } else if (bsonValue.isBoolean()) {
      return new AttributeValue().withBOOL(((BsonBoolean) bsonValue).getValue());
    } else if (bsonValue.isNull()) {
      return new AttributeValue().withNULL(true);
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
          AttributeValue attributeValue = new AttributeValue().withSS();
          bsonArray.getValues()
              .forEach(val -> attributeValue.withSS(((BsonString) val).getValue()));
          return attributeValue;
        } else if (firstElement.isNumber() || firstElement.isDecimal128()) {
          AttributeValue attributeValue = new AttributeValue().withNS();
          bsonArray.getValues().forEach(val -> attributeValue.withNS(
              numberToString(getNumberFromBsonNumber((BsonNumber) val))));
          return attributeValue;
        } else if (firstElement.isBinary()) {
          AttributeValue attributeValue = new AttributeValue().withBS();
          bsonArray.getValues().forEach(val -> attributeValue.withBS(
              ByteBuffer.wrap(((BsonBinary) val).getData())));
          return attributeValue;
        }
        throw new IllegalArgumentException("Invalid set type");
      } else {
        Map<String, AttributeValue> map = new HashMap<>();
        for (Map.Entry<String, BsonValue> entry : bsonDocument.entrySet()) {
          updateMapEntries(entry, map);
        }
        return new AttributeValue().withM(map);
      }
    } else if (bsonValue.isArray()) {
      BsonArray bsonArray = (BsonArray) bsonValue;
      List<AttributeValue> attributeValueList = new ArrayList<>();
      for (BsonValue bsonArrayValue : bsonArray.getValues()) {
        attributeValueList.add(getAttributeValue(bsonArrayValue));
      }
      return new AttributeValue().withL(attributeValueList);
    }
    LOGGER.error("Invalid  data type of BsonValue: {}", bsonValue);
    throw new RuntimeException("Invalid data type of BsonValue");
  }

  private static AttributeValue getNumber(BsonNumber bsonNumber) {
    AttributeValue attributeValue = new AttributeValue();
    attributeValue.withN(numberToString(getNumberFromBsonNumber(bsonNumber)));
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

}
