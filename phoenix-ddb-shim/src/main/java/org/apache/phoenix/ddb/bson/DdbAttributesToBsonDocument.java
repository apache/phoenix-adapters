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

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDecimal128;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNull;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Conversion from DynamoDB item to BsonDocument.
 */
public class DdbAttributesToBsonDocument {

  private static final Logger LOGGER =
      LoggerFactory.getLogger(DdbAttributesToBsonDocument.class);

  /**
   * Convert the given DDB item into Raw BsonDocument.
   *
   * @param item DDB item as Map of attribute name to attribute value.
   * @return Corresponding RawBsonDocument.
   */
  public static RawBsonDocument getRawBsonDocument(final Map<String, AttributeValue> item) {
    return new RawBsonDocumentCodec().decode(
        new BsonDocumentReader(getBsonDocument(item)),
        DecoderContext.builder().build());
  }

  /**
   * Retrieve updatable BsonDocument for the given DDB item.
   *
   * @param item DDB item as Map of attribute name to attribute value.
   * @return Corresponding BsonDocument.
   */
  public static BsonDocument getBsonDocument(final Map<String, AttributeValue> item) {
    if (item == null) return null;
    BsonDocument bsonDocument = new BsonDocument();
    for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
      updateBsonDocEntries(entry, bsonDocument);
    }
    return bsonDocument;
  }

  private static void updateBsonDocEntries(Map.Entry<String, AttributeValue> entry,
      BsonDocument bsonDocument) {
    AttributeValue attributeValue = entry.getValue();
    bsonDocument.append(entry.getKey(), getBsonValue(attributeValue));
  }

  private static BsonValue getBsonValue(AttributeValue attributeValue) {
    if (attributeValue.getS() != null) {
      return new BsonString(attributeValue.getS());
    } else if (attributeValue.getN() != null) {
      return getBsonNumber(attributeValue);
    } else if (attributeValue.getB() != null) {
      return new BsonBinary(attributeValue.getB().array());
    } else if (attributeValue.getBOOL() != null) {
      return new BsonBoolean(attributeValue.getBOOL());
    } else if (attributeValue.getNULL() != null) {
      return new BsonNull();
    } else if (attributeValue.getM() != null) {
      BsonDocument bsonDocument = new BsonDocument();
      for (Map.Entry<String, AttributeValue> entry : attributeValue.getM().entrySet()) {
        updateBsonDocEntries(entry, bsonDocument);
      }
      return bsonDocument;
    } else if (attributeValue.getL() != null) {
      BsonArray bsonArray = new BsonArray();
      for(AttributeValue listValue : attributeValue.getL()) {
        bsonArray.add(getBsonValue(listValue));
      }
      return bsonArray;
    } else if (attributeValue.getSS() != null) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonString> list = new ArrayList<>();
      attributeValue.getSS().forEach(val -> list.add(new BsonString(val)));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    } else if (attributeValue.getNS() != null) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonNumber> list = new ArrayList<>();
      attributeValue.getNS().forEach(val -> list.add(getBsonNumberFromNumber(val)));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    } else if (attributeValue.getBS() != null) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonBinary> list = new ArrayList<>();
      attributeValue.getBS().forEach(val -> list.add(new BsonBinary(val.array())));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    }
    LOGGER.error("Invalid data type for AttributeValue: {}", attributeValue);
    throw new RuntimeException("Invalid data type of AttributeValue");
  }

  private static BsonNumber getBsonNumber(AttributeValue attributeValue) {
    String number = attributeValue.getN();
    return getBsonNumberFromNumber(number);
  }

  private static BsonNumber getBsonNumberFromNumber(String strNum) {
    Number number = stringToNumber(strNum);
    BsonNumber bsonNumber;
    if (number instanceof Integer || number instanceof Short || number instanceof Byte) {
      bsonNumber = new BsonInt32(number.intValue());
    } else if (number instanceof Long) {
      bsonNumber = new BsonInt64(number.longValue());
    } else if (number instanceof Double || number instanceof Float) {
      bsonNumber = new BsonDouble(number.doubleValue());
    } else if (number instanceof BigDecimal) {
      bsonNumber = new BsonDecimal128(new Decimal128((BigDecimal) number));
    } else {
      LOGGER.error("Error while converting {} into BsonNumber", strNum);
      throw new IllegalArgumentException("Unsupported Number type: " + number);
    }
    return bsonNumber;
  }

  /**
   * Convert the given String to Number.
   *
   * @param number The String represented numeric value.
   * @return The Number object.
   */
  private static Number stringToNumber(String number) {
    try {
      return Integer.parseInt(number);
    } catch (NumberFormatException e) {
      // no-op
    }
    try {
      return Long.parseLong(number);
    } catch (NumberFormatException e) {
      // no-op
    }
    try {
      return Double.parseDouble(number);
    } catch (NumberFormatException e) {
      // no-op
    }
    try {
      return NumberFormat.getInstance().parse(number);
    } catch (ParseException e) {
      LOGGER.error("Unable to parse number string {} to number", number);
      return null;
    }
  }

}
