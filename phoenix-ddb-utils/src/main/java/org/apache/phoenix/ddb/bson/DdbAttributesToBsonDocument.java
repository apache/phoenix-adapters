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
import java.util.List;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.bson.BsonArray;
import org.bson.BsonBinary;
import org.bson.BsonBoolean;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonNull;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.RawBsonDocument;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.RawBsonDocumentCodec;
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
    if (attributeValue.s() != null) {
      return new BsonString(attributeValue.s());
    } else if (attributeValue.n() != null) {
      return getBsonNumber(attributeValue);
    } else if (attributeValue.b() != null) {
      return new BsonBinary(attributeValue.b().asByteArray());
    } else if (attributeValue.bool() != null) {
      return new BsonBoolean(attributeValue.bool());
    } else if (attributeValue.nul() != null) {
      return new BsonNull();
    } else if (attributeValue.hasM()) {
      BsonDocument bsonDocument = new BsonDocument();
      for (Map.Entry<String, AttributeValue> entry : attributeValue.m().entrySet()) {
        updateBsonDocEntries(entry, bsonDocument);
      }
      return bsonDocument;
    } else if (attributeValue.hasL()) {
      BsonArray bsonArray = new BsonArray();
      for(AttributeValue listValue : attributeValue.l()) {
        bsonArray.add(getBsonValue(listValue));
      }
      return bsonArray;
    } else if (attributeValue.hasSs()) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonString> list = new ArrayList<>();
      attributeValue.ss().forEach(val -> list.add(new BsonString(val)));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    } else if (attributeValue.hasNs()) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonNumber> list = new ArrayList<>();
      attributeValue.ns().forEach(val -> list.add(BsonNumberConversionUtil.getBsonNumberFromNumber(val)));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    } else if (attributeValue.hasBs()) {
      BsonDocument bsonDocument = new BsonDocument();
      List<BsonBinary> list = new ArrayList<>();
      attributeValue.bs().forEach(val -> list.add(new BsonBinary(val.asByteArray())));
      bsonDocument.put("$set", new BsonArray(list));
      return bsonDocument;
    }
    LOGGER.error("Invalid data type for AttributeValue: {}", attributeValue);
    throw new RuntimeException("Invalid data type of AttributeValue");
  }

  private static BsonNumber getBsonNumber(AttributeValue attributeValue) {
    String number = attributeValue.n();
    return BsonNumberConversionUtil.getBsonNumberFromNumber(number);
  }

}
