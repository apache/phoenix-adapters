/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.ddb;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.ddb.update.UpdateExpressionToBson;

public class UpdateExpressionConversionTest {

  @Test(timeout = 120000)
  public void test1() {

    String ddbUpdateExp = "SET Title = :newTitle, aCol = if_not_exists (aCol, :aCol) , "
        + "Id = :newId , NestedMap1.ColorList = :ColorList , "
        + "Id1 = :Id1 , NestedMap1.NList1[0] = :NList1_0 , "
        + "NestedList1[2][1].ISBN = :NestedList1_ISBN , "
        + "NestedMap1.NestedMap2.NewID = :newId , "
        + "NestedMap1.NestedMap2.NList[2] = :NList003 , "
        + "NestedMap1.NestedMap2.NList[0] = :NList001 ADD AddedId :attr5_0 , "
        + "NestedMap1.AddedId :attr5_0, NestedMap1.NestedMap2.Id :newIdNeg , "
        + "NestedList12[2][0] :NestedList12_00 , NestedList12[2][1] :NestedList12_01 ,"
        + "  Pictures  :AddedPics "
        + "REMOVE IdS, Id2, NestedMap1.Title , "
        + "NestedMap1.NestedMap2.InPublication , NestedList1[2][1].TitleSet1 "
        + "DELETE PictureBinarySet :PictureBinarySet01 , NestedMap1.NSet1 :NSet01 ,"
        + "  NestedList1[2][1].TitleSet2 :NestedList1TitleSet01";

    String expectedBsonUpdateExpression = "{\n"
        + "  \"$SET\": {\n"
        + "    \"Title\": \"Cycle_1234_new\",\n"
        + "    \"aCol\": {\n"
        + "       \"$IF_NOT_EXISTS\": {\n"
        + "         \"aCol\": {\n"
        + "           \"$set\": [\n"
        + "               1,\n"
        + "               2,\n"
        + "               3\n"
        + "             ]\n"
        + "           }\n"
        + "         }\n"
        + "    },\n"
        + "    \"Id\": \"12345\",\n"
        + "    \"NestedMap1.ColorList\": [\n"
        + "      \"Black\",\n"
        + "      {\n"
        + "        \"$binary\": {\n"
        + "          \"base64\": \"V2hpdGU=\",\n"
        + "          \"subType\": \"00\"\n"
        + "        }\n"
        + "      },\n"
        + "      \"Silver\"\n"
        + "    ],\n"
        + "    \"Id1\": {\n"
        + "      \"$binary\": {\n"
        + "        \"base64\": \"SURfMTAx\",\n"
        + "        \"subType\": \"00\"\n"
        + "      }\n"
        + "    },\n"
        + "    \"NestedMap1.NList1[0]\": {\n"
        + "      \"$set\": [\n"
        + "        \"Updated_set_01\",\n"
        + "        \"Updated_set_02\"\n"
        + "      ]\n"
        + "    },\n"
        + "    \"NestedList1[2][1].ISBN\": \"111-1111111122\",\n"
        + "    \"NestedMap1.NestedMap2.NewID\": \"12345\",\n"
        + "    \"NestedMap1.NestedMap2.NList[2]\": null,\n"
        + "    \"NestedMap1.NestedMap2.NList[0]\": 12.22\n"
        + "  },\n"
        + "  \"$UNSET\": {\n"
        + "    \"IdS\": null,\n"
        + "    \"Id2\": null,\n"
        + "    \"NestedMap1.Title\": null,\n"
        + "    \"NestedMap1.NestedMap2.InPublication\": null,\n"
        + "    \"NestedList1[2][1].TitleSet1\": null\n"
        + "  },\n"
        + "  \"$ADD\": {\n"
        + "    \"AddedId\": 10,\n"
        + "    \"NestedMap1.AddedId\": 10,\n"
        + "    \"NestedMap1.NestedMap2.Id\": -12345,\n"
        + "    \"NestedList12[2][0]\": {\n"
        + "      \"$set\": [\n"
        + "        \"xyz01234\",\n"
        + "        \"abc01234\"\n"
        + "      ]\n"
        + "    },\n"
        + "    \"NestedList12[2][1]\": {\n"
        + "      \"$set\": [\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"dmFsMDM=\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        },\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"dmFsMDQ=\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    \"Pictures\": {\n"
        + "      \"$set\": [\n"
        + "        \"1235@_rear.jpg\",\n"
        + "        \"xyz5@_rear.jpg\"\n"
        + "      ]\n"
        + "    }\n"
        + "  },\n"
        + "  \"$DELETE_FROM_SET\": {\n"
        + "    \"PictureBinarySet\": {\n"
        + "      \"$set\": [\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"MTIzX3JlYXIuanBn\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        },\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"eHl6X2Zyb250LmpwZw==\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        },\n"
        + "        {\n"
        + "          \"$binary\": {\n"
        + "            \"base64\": \"eHl6X2Zyb250LmpwZ19ubw==\",\n"
        + "            \"subType\": \"00\"\n"
        + "          }\n"
        + "        }\n"
        + "      ]\n"
        + "    },\n"
        + "    \"NestedMap1.NSet1\": {\n"
        + "      \"$set\": [\n"
        + "        -6830.5555,\n"
        + "        -48695\n"
        + "      ]\n"
        + "    },\n"
        + "    \"NestedList1[2][1].TitleSet2\": {\n"
        + "      \"$set\": [\n"
        + "        \"Book 1010 Title\",\n"
        + "        \"Book 1011 Title\"\n"
        + "      ]\n"
        + "    }\n"
        + "  }\n"
        + "}";


    Assert.assertEquals(RawBsonDocument.parse(expectedBsonUpdateExpression),
        UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
            getComparisonValuesMap()));
  }

  @Test
  public void test2() {
    String ddbUpdateExp = "SET bCol = if_not_exists(bCol, :aCol)";
    String expectedBsonUpdateExpression = "{\n" +
            "  \"$SET\": {\n" +
            "    \"bCol\": {\n" +
            "      \"$IF_NOT_EXISTS\": {\n" +
            "        \"bCol\": {\n" +
            "          \"$set\": [\n" +
            "            1,\n" +
            "            2,\n" +
            "            3\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expectedBsonUpdateExpression),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getComparisonValuesMap()));
  }

  @Test
  public void test3() {
    String ddbUpdateExp = "SET bCol = if_not_exists(bCol, :aCol)," +
            "aCol=if_not_exists( aCol,:aCol ), cCol =if_not_exists (cCol, :aCol)";
    String expectedBsonUpdateExpression = "{\n" +
            "  \"$SET\": {\n" +
            "    \"bCol\": {\n" +
            "      \"$IF_NOT_EXISTS\": {\n" +
            "        \"bCol\": {\n" +
            "          \"$set\": [\n" +
            "            1,\n" +
            "            2,\n" +
            "            3\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"aCol\": {\n" +
            "      \"$IF_NOT_EXISTS\": {\n" +
            "        \"aCol\": {\n" +
            "          \"$set\": [\n" +
            "            1,\n" +
            "            2,\n" +
            "            3\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    },\n" +
            "    \"cCol\": {\n" +
            "      \"$IF_NOT_EXISTS\": {\n" +
            "        \"cCol\": {\n" +
            "          \"$set\": [\n" +
            "            1,\n" +
            "            2,\n" +
            "            3\n" +
            "          ]\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expectedBsonUpdateExpression),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getComparisonValuesMap()));
  }

  @Test
  public void testListAppendPathAndLiteral() {
    String ddbUpdateExp = "SET myList = list_append(myList, :listVal)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"myList\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        \"myList\",\n" +
            "        [\"c\", \"d\"]\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  @Test
  public void testListAppendLiteralAndPath() {
    String ddbUpdateExp = "SET myList = list_append(:listVal, myList)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"myList\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        [\"c\", \"d\"],\n" +
            "        \"myList\"\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  @Test
  public void testListAppendBothLiterals() {
    String ddbUpdateExp = "SET myList = list_append(:listVal, :listVal2)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"myList\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        [\"c\", \"d\"],\n" +
            "        [\"e\", \"f\"]\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * Canonical create-or-append: if the target list does not exist, fall back to an empty
   * list as the first operand and then append the new literal items.
   */
  @Test
  public void testListAppendWithIfNotExists() {
    String ddbUpdateExp = "SET newQueue = list_append(if_not_exists(newQueue, :emptyList), :listVal)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"newQueue\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        {\n" +
            "          \"$IF_NOT_EXISTS\": {\n" +
            "            \"newQueue\": []\n" +
            "          }\n" +
            "        },\n" +
            "        [\"c\", \"d\"]\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * list_append combined with arithmetic and a remove in the same expression.
   */
  @Test
  public void testListAppendMixedWithArithmeticAndRemove() {
    String ddbUpdateExp =
            "SET myList = list_append(myList, :listVal), counter = counter + :one "
                    + "REMOVE oldField";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"myList\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        \"myList\",\n" +
            "        [\"c\", \"d\"]\n" +
            "      ]\n" +
            "    },\n" +
            "    \"counter\": {\n" +
            "      \"$ADD\": [\n" +
            "        \"counter\",\n" +
            "        1\n" +
            "      ]\n" +
            "    }\n" +
            "  },\n" +
            "  \"$UNSET\": {\n" +
            "    \"oldField\": null\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * Nested document path on the LHS and as the path operand.
   */
  @Test
  public void testListAppendNestedPath() {
    String ddbUpdateExp = "SET nested.queue = list_append(nested.queue, :listVal)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"nested.queue\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        \"nested.queue\",\n" +
            "        [\"c\", \"d\"]\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * if_not_exists as the SECOND operand (literal first). This prepends the new items
   * before the existing-or-fallback list.
   */
  @Test
  public void testListAppendLiteralAndIfNotExists() {
    String ddbUpdateExp = "SET newQueue = list_append(:listVal, if_not_exists(newQueue, :emptyList))";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"newQueue\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        [\"c\", \"d\"],\n" +
            "        {\n" +
            "          \"$IF_NOT_EXISTS\": {\n" +
            "            \"newQueue\": []\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * Create-or-append a list while atomically incrementing a counter in the same SET clause.
   */
  @Test
  public void testListAppendWithIfNotExistsAndCounterIncrement() {
    String ddbUpdateExp =
            "SET events = list_append(if_not_exists(events, :empty_list), :new_evts), "
                    + "updateCounter = updateCounter + :one";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"events\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        {\n" +
            "          \"$IF_NOT_EXISTS\": {\n" +
            "            \"events\": []\n" +
            "          }\n" +
            "        },\n" +
            "        [\"ev1\", \"ev2\"]\n" +
            "      ]\n" +
            "    },\n" +
            "    \"updateCounter\": {\n" +
            "      \"$ADD\": [\n" +
            "        \"updateCounter\",\n" +
            "        1\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":empty_list",
            AttributeValue.builder().l(Collections.emptyList()).build());
    attributeMap.put(":new_evts", AttributeValue.builder().l(
            AttributeValue.builder().s("ev1").build(),
            AttributeValue.builder().s("ev2").build()).build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);

    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values));
  }

  /**
   * Both operands of list_append are if_not_exists. Regression test for the
   * paren-counting splitter: there are top-level commas at depth 1 followed by
   * another open-paren on each side, which the old regex-based splitter could not
   * handle.
   */
  @Test
  public void testListAppendBothOperandsAreIfNotExists() {
    String ddbUpdateExp =
            "SET merged = list_append(if_not_exists(left, :emptyList), if_not_exists(right, :emptyList))";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"merged\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"left\":  [] } },\n" +
            "        { \"$IF_NOT_EXISTS\": { \"right\": [] } }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    getListAppendComparisonValuesMap()));
  }

  /**
   * Canonical counter-init pattern: increment a counter that may not exist yet, using
   * if_not_exists as one of the arithmetic operands. Common DDB usage but previously
   * only covered by IT tests.
   */
  @Test
  public void testArithmeticWithIfNotExistsCounterIncrement() {
    String ddbUpdateExp = "SET counter = if_not_exists(counter, :zero) + :one";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"counter\": {\n" +
            "      \"$ADD\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"counter\": 0 } },\n" +
            "        1\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":zero", AttributeValue.builder().n("0").build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values));
  }

  /**
   * Arithmetic with both operands being attribute paths (no placeholders, no
   * if_not_exists).
   */
  @Test
  public void testArithmeticPathPlusPath() {
    String ddbUpdateExp = "SET total = subtotal + tax";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"total\": {\n" +
            "      \"$ADD\": [\n" +
            "        \"subtotal\",\n" +
            "        \"tax\"\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
                    new BsonDocument()));
  }

  /**
   * Subtract with placeholder first and attribute path second
   * (e.g. {@code SET remaining = :limit - used}).
   */
  @Test
  public void testArithmeticLiteralMinusPath() {
    String ddbUpdateExp = "SET remaining = :limit - used";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"remaining\": {\n" +
            "      \"$SUBTRACT\": [\n" +
            "        100,\n" +
            "        \"used\"\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":limit", AttributeValue.builder().n("100").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values));
  }

  /**
   * Multiple SET clauses interleaving every supported function/operator shape:
   * plain placeholder, if_not_exists, arithmetic with if_not_exists, list_append,
   * and a bare nested-path assignment.
   */
  @Test
  public void testSetEveryShapeMixed() {
    String ddbUpdateExp = "SET title = :newTitle, "
            + "counter = if_not_exists(counter, :zero) + :one, "
            + "total = price - :discount, "
            + "events = list_append(if_not_exists(events, :emptyList), :newEvts), "
            + "nested.path = :nestedVal";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"title\": \"hello\",\n" +
            "    \"counter\": {\n" +
            "      \"$ADD\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"counter\": 0 } },\n" +
            "        1\n" +
            "      ]\n" +
            "    },\n" +
            "    \"total\": {\n" +
            "      \"$SUBTRACT\": [\n" +
            "        \"price\",\n" +
            "        5\n" +
            "      ]\n" +
            "    },\n" +
            "    \"events\": {\n" +
            "      \"$LIST_APPEND\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"events\": [] } },\n" +
            "        [\"e1\", \"e2\"]\n" +
            "      ]\n" +
            "    },\n" +
            "    \"nested.path\": \"deep\"\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":newTitle", AttributeValue.builder().s("hello").build());
    attributeMap.put(":zero", AttributeValue.builder().n("0").build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    attributeMap.put(":discount", AttributeValue.builder().n("5").build());
    attributeMap.put(":emptyList",
            AttributeValue.builder().l(Collections.emptyList()).build());
    attributeMap.put(":newEvts", AttributeValue.builder().l(
            AttributeValue.builder().s("e1").build(),
            AttributeValue.builder().s("e2").build()).build());
    attributeMap.put(":nestedVal", AttributeValue.builder().s("deep").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values));
  }

  @Test
  public void testListAppendWrongArityOne() {
    String ddbUpdateExp = "SET myList = list_append(:listVal)";
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
              getListAppendComparisonValuesMap());
      Assert.fail("Expected IllegalArgumentException for wrong arity (1 operand)");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
              e.getMessage().contains("Invalid UpdateExpression"));
    }
  }

  @Test
  public void testListAppendWrongArityThree() {
    String ddbUpdateExp = "SET myList = list_append(:listVal, :listVal2, myList)";
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
              getListAppendComparisonValuesMap());
      Assert.fail("Expected IllegalArgumentException for wrong arity (3 operands)");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
              e.getMessage().contains("Invalid UpdateExpression"));
    }
  }

  @Test
  public void testListAppendMissingPlaceholder() {
    String ddbUpdateExp = "SET myList = list_append(myList, :missing)";
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
              getListAppendComparisonValuesMap());
      Assert.fail("Expected RuntimeException for missing placeholder");
    } catch (RuntimeException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
              e.getMessage().contains(":missing"));
    }
  }

  @Test
  public void testListAppendNonArrayPlaceholder() {
    String ddbUpdateExp = "SET myList = list_append(myList, :notList)";
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
              getListAppendComparisonValuesMap());
      Assert.fail("Expected RuntimeException for non-list placeholder");
    } catch (RuntimeException e) {
      Assert.assertTrue("Unexpected message: " + e.getMessage(),
              e.getMessage().contains("must resolve to a List type"));
    }
  }

  /**
   * Tab and mixed whitespace between clause keywords.
   */
  @Test
  public void testWhitespaceTabBetweenClauses() {
    String ddbUpdateExp = "SET a = :v\tREMOVE b\tADD c :n\tDELETE d :s";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("hello").build());
    attributeMap.put(":n", AttributeValue.builder().n("1").build());
    attributeMap.put(":s", AttributeValue.builder().ss("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values);
    Assert.assertTrue(actual.containsKey("$SET"));
    Assert.assertTrue(actual.containsKey("$UNSET"));
    Assert.assertTrue(actual.containsKey("$ADD"));
    Assert.assertTrue(actual.containsKey("$DELETE_FROM_SET"));
    Assert.assertEquals("hello", actual.getDocument("$SET").getString("a").getValue());
    Assert.assertTrue(actual.getDocument("$UNSET").containsKey("b"));
  }

  /**
   * Arithmetic with both operands being if_not_exists.
   */
  @Test
  public void testArithmeticBothOperandsIfNotExists() {
    String ddbUpdateExp = "SET total = if_not_exists(a, :z) + if_not_exists(b, :z)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"total\": {\n" +
            "      \"$ADD\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"a\": 0 } },\n" +
            "        { \"$IF_NOT_EXISTS\": { \"b\": 0 } }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":z", AttributeValue.builder().n("0").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values));
  }

  /**
   * Arithmetic where the placeholder is the first operand and if_not_exists is the second.
   * Mirror of the existing testArithmeticWithIfNotExistsCounterIncrement.
   */
  @Test
  public void testArithmeticLiteralPlusIfNotExists() {
    String ddbUpdateExp = "SET counter = :one + if_not_exists(counter, :zero)";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"counter\": {\n" +
            "      \"$ADD\": [\n" +
            "        1,\n" +
            "        { \"$IF_NOT_EXISTS\": { \"counter\": 0 } }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":zero", AttributeValue.builder().n("0").build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values));
  }

  /**
   * Subtract with if_not_exists on the LHS and a placeholder on the RHS.
   */
  @Test
  public void testArithmeticIfNotExistsMinusLiteral() {
    String ddbUpdateExp = "SET remaining = if_not_exists(used, :zero) - :n";
    String expected = "{\n" +
            "  \"$SET\": {\n" +
            "    \"remaining\": {\n" +
            "      \"$SUBTRACT\": [\n" +
            "        { \"$IF_NOT_EXISTS\": { \"used\": 0 } },\n" +
            "        5\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":zero", AttributeValue.builder().n("0").build());
    attributeMap.put(":n", AttributeValue.builder().n("5").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Assert.assertEquals(RawBsonDocument.parse(expected),
            UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values));
  }

  /**
   * if_not_exists with one argument should be rejected by the grammar.
   */
  @Test
  public void testIfNotExistsWrongArityOne() {
    String ddbUpdateExp = "SET a = if_not_exists(a)";
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp,
              new BsonDocument());
      Assert.fail("Expected IllegalArgumentException for if_not_exists arity 1");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected: " + e.getMessage(),
              e.getMessage().contains("Invalid UpdateExpression"));
    }
  }

  /**
   * if_not_exists with three arguments should be rejected by the grammar.
   */
  @Test
  public void testIfNotExistsWrongArityThree() {
    String ddbUpdateExp = "SET a = if_not_exists(a, :v, :v2)";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    attributeMap.put(":v2", AttributeValue.builder().s("y").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values);
      Assert.fail("Expected IllegalArgumentException for if_not_exists arity 3");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected: " + e.getMessage(),
              e.getMessage().contains("Invalid UpdateExpression"));
    }
  }

  /**
   * ADD/DELETE clauses tolerate extra inter-token whitespace (multiple spaces, tabs).
   */
  @Test
  public void testAddDeleteExtraWhitespace() {
    String ddbUpdateExp = "ADD a   :v,  b\t:w DELETE c\t  :s";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().n("1").build());
    attributeMap.put(":w", AttributeValue.builder().n("2").build());
    attributeMap.put(":s", AttributeValue.builder().ss("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values);
    BsonDocument addDoc = actual.getDocument("$ADD");
    Assert.assertEquals(2, addDoc.size());
    Assert.assertTrue(addDoc.containsKey("a"));
    Assert.assertTrue(addDoc.containsKey("b"));
    Assert.assertTrue(actual.getDocument("$DELETE_FROM_SET").containsKey("c"));
  }

  /**
   * Attribute names that contain a clause keyword as a substring (ADDRESS, REMOTE, SETUP)
   * should not confuse the clause-splitting regex. Word-boundary anchored.
   */
  @Test
  public void testAttributeNameContainingKeywordSubstring() {
    String ddbUpdateExp = "SET ADDRESS = :v, REMOTE = :v2 ADD SETUP :n";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("home").build());
    attributeMap.put(":v2", AttributeValue.builder().s("ssh").build());
    attributeMap.put(":n", AttributeValue.builder().n("1").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values);
    BsonDocument setDoc = actual.getDocument("$SET");
    Assert.assertEquals("home", setDoc.getString("ADDRESS").getValue());
    Assert.assertEquals("ssh", setDoc.getString("REMOTE").getValue());
    Assert.assertEquals(1, actual.getDocument("$ADD").getInt32("SETUP").getValue());
  }

  /**
   * All four clauses in canonical AWS order (SET REMOVE ADD DELETE) should each populate
   * their respective BSON section.
   */
  @Test
  public void testAllFourClausesCanonicalOrder() {
    String ddbUpdateExp = "SET a = :v REMOVE b ADD c :n DELETE d :s";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    attributeMap.put(":n", AttributeValue.builder().n("1").build());
    attributeMap.put(":s", AttributeValue.builder().ss("y").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values);
    Assert.assertEquals(4, actual.size());
    Assert.assertTrue(actual.containsKey("$SET"));
    Assert.assertTrue(actual.containsKey("$UNSET"));
    Assert.assertTrue(actual.containsKey("$ADD"));
    Assert.assertTrue(actual.containsKey("$DELETE_FROM_SET"));
  }

  /**
   * Path overlap: parent path in one clause vs nested-field path in another. DDB rejects.
   */
  @Test
  public void testPathOverlapParentAndNestedField() {
    String ddbUpdateExp = "SET a.b = :v REMOVE a";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values);
      Assert.fail("Expected IllegalArgumentException for parent/child path overlap");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected: " + e.getMessage(),
              e.getMessage().contains("paths overlap"));
    }
  }

  /**
   * Path overlap: parent path vs nested-array-index path.
   */
  @Test
  public void testPathOverlapParentAndArrayIndex() {
    String ddbUpdateExp = "SET a[0] = :v REMOVE a";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values);
      Assert.fail("Expected IllegalArgumentException for parent/array-index path overlap");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected: " + e.getMessage(),
              e.getMessage().contains("paths overlap"));
    }
  }

  /**
   * Two paths sharing only a string-prefix (no separator boundary) must NOT be flagged
   * as overlapping. {@code a} and {@code ab} are independent attribute names.
   */
  @Test
  public void testPathPrefixWithoutSeparatorDoesNotOverlap() {
    String ddbUpdateExp = "SET a = :v, ab = :v";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values);
    Assert.assertEquals(2, actual.getDocument("$SET").size());
  }

  /**
   * Sibling paths under the same parent must NOT be flagged as overlapping.
   */
  @Test
  public void testPathSiblingsDoNotOverlap() {
    String ddbUpdateExp = "SET a.b = :v, a.c = :v";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values);
    Assert.assertEquals(2, actual.getDocument("$SET").size());
  }

  /**
   * Duplicate clause keyword is detected with a clear message (not the misleading
   * "does not include key value pairs separated by =" that the {@code =}-split would emit).
   */
  @Test
  public void testDuplicateClauseKeywordParserMessage() {
    String ddbUpdateExp = "SET a = :v SET b = :v2";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    attributeMap.put(":v2", AttributeValue.builder().s("y").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values);
      Assert.fail("Expected IllegalArgumentException for duplicate SET keyword");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected: " + e.getMessage(),
              e.getMessage().contains("appears more than once"));
    }
  }

  /**
   * Undeclared {@code :placeholder} surfaces a clear parser-level error rather than an NPE.
   */
  @Test
  public void testUndeclaredPlaceholderInSet() {
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(
              "SET a = :missing", new BsonDocument());
      Assert.fail("Expected IllegalArgumentException for undeclared placeholder");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected: " + e.getMessage(),
              e.getMessage().contains(":missing")
                      && e.getMessage().contains("not declared"));
    }
  }

  /**
   * Alias path inside REMOVE: {@code REMOVE #a.b}.
   */
  @Test
  public void testAliasPathInRemove() {
    String ddbUpdateExp = "REMOVE #a.b";
    Map<String, String> aliases = Collections.singletonMap("#a", "topMap");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, new BsonDocument(), aliases);
    Assert.assertTrue(actual.getDocument("$UNSET").containsKey("topMap.b"));
  }

  /**
   * Alias path in ADD: {@code ADD #c :v}.
   */
  @Test
  public void testAliasPathInAdd() {
    String ddbUpdateExp = "ADD #c :v";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().n("3").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = Collections.singletonMap("#c", "counter");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    Assert.assertEquals(3, actual.getDocument("$ADD").getInt32("counter").getValue());
  }

  /**
   * Alias path in DELETE: {@code DELETE #s :v}.
   */
  @Test
  public void testAliasPathInDelete() {
    String ddbUpdateExp = "DELETE #s :v";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().ss("a").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = Collections.singletonMap("#s", "myStringSet");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    Assert.assertTrue(actual.getDocument("$DELETE_FROM_SET").containsKey("myStringSet"));
  }

  /**
   * All four clause keywords are case-insensitive (matches DDB). One representative case-mix per
   * clause; each clause maps to its own lexer fragment so a future regression in any single
   * keyword would surface here.
   */
  @Test
  public void testClauseKeywordsAreCaseInsensitive() {
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    attributeMap.put(":n", AttributeValue.builder().n("1").build());
    attributeMap.put(":s", AttributeValue.builder().ss("y").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);

    Map<String, String> setForms = new HashMap<>();
    setForms.put("set a = :v",     "$SET");
    setForms.put("Set a = :v",     "$SET");
    setForms.put("sEt a = :v",     "$SET");
    setForms.put("remove a",       "$UNSET");
    setForms.put("Remove a",       "$UNSET");
    setForms.put("REMOVE a",       "$UNSET");
    setForms.put("add a :n",       "$ADD");
    setForms.put("Add a :n",       "$ADD");
    setForms.put("aDd a :n",       "$ADD");
    setForms.put("delete a :s",    "$DELETE_FROM_SET");
    setForms.put("Delete a :s",    "$DELETE_FROM_SET");
    setForms.put("dELETE a :s",    "$DELETE_FROM_SET");

    for (Map.Entry<String, String> e : setForms.entrySet()) {
      BsonDocument actual = UpdateExpressionToBson.toBsonUpdateDocument(e.getKey(), values);
      Assert.assertTrue("expr=" + e.getKey() + " expected key " + e.getValue(),
              actual.containsKey(e.getValue()));
      Assert.assertTrue("expr=" + e.getKey() + " expected attribute 'a'",
              actual.getDocument(e.getValue()).containsKey("a"));
    }
  }

  /**
   * Function names ({@code if_not_exists}, {@code list_append}) are case-sensitive per AWS docs.
   * Mixed-case must be rejected by the lexer.
   */
  @Test
  public void testFunctionNamesAreCaseSensitive() {
    String[] mixedCaseForms = {
            "SET a = If_Not_Exists(a, :v)",
            "SET a = IF_NOT_EXISTS(a, :v)",
            "SET a = list_APPEND(a, :v)",
            "SET a = LIST_APPEND(a, :v)",
    };
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    for (String expr : mixedCaseForms) {
      try {
        UpdateExpressionToBson.toBsonUpdateDocument(expr, values);
        Assert.fail("Expected IllegalArgumentException for case-mixed function in: " + expr);
      } catch (IllegalArgumentException e) {
        // expected
      }
    }
  }

  /**
   * {@code list_append} is not a valid arithmetic operand: lists cannot be added/subtracted.
   * Visitor rejects at the parser layer.
   */
  @Test
  public void testListAppendAsArithmeticOperandRejected() {
    String ddbUpdateExp = "SET col = list_append(a, :v) + list_append(:w, b)";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().l(
            AttributeValue.builder().s("x").build()).build());
    attributeMap.put(":w", AttributeValue.builder().l(
            AttributeValue.builder().s("y").build()).build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values);
      Assert.fail("Expected IllegalArgumentException for list_append in arithmetic operand");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected: " + e.getMessage(),
              e.getMessage().contains("list_append")
                      && e.getMessage().contains("arithmetic"));
    }
  }

  /**
   * Arithmetic with an {@code if_not_exists} fallback that resolves to a non-numeric
   * placeholder must be rejected at the parser layer (not silently emitted to the BSON
   * evaluator).
   */
  @Test
  public void testArithmeticIfNotExistsNonNumericFallback() {
    String ddbUpdateExp = "SET counter = if_not_exists(c, :s) + :one";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":s", AttributeValue.builder().s("notANumber").build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    try {
      UpdateExpressionToBson.toBsonUpdateDocument(ddbUpdateExp, values);
      Assert.fail("Expected IllegalArgumentException for non-numeric if_not_exists fallback");
    } catch (IllegalArgumentException e) {
      Assert.assertTrue("Unexpected: " + e.getMessage(),
              e.getMessage().contains(":s") && e.getMessage().contains("number"));
    }
  }

  /**
   * Alias-as-keyword: an ExpressionAttributeName whose VALUE happens to be a clause keyword
   * (e.g. {@code "ADD"}) must NOT confuse the parser. The lexer never sees the resolved name.
   */
  @Test
  public void testAliasResolvingToClauseKeyword() {
    String ddbUpdateExp = "SET #attr = :v REMOVE other";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("hello").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = new HashMap<>();
    aliases.put("#attr", "ADD");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    BsonDocument setDoc = actual.getDocument("$SET");
    Assert.assertEquals(1, setDoc.size());
    Assert.assertEquals("hello", setDoc.getString("ADD").getValue());
    Assert.assertTrue(actual.getDocument("$UNSET").containsKey("other"));
  }

  /**
   * Alias followed by a bare attribute name in a nested path: {@code #a.bareField}.
   */
  @Test
  public void testAliasThenBareNamePath() {
    String ddbUpdateExp = "SET #a.bareField = :v";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = Collections.singletonMap("#a", "topMap");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    Assert.assertEquals("x",
            actual.getDocument("$SET").getString("topMap.bareField").getValue());
  }

  /**
   * Bare attribute name followed by an alias: {@code bareField.#a}.
   */
  @Test
  public void testBareNameThenAliasPath() {
    String ddbUpdateExp = "SET bareField.#a = :v";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = Collections.singletonMap("#a", "deep");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    Assert.assertEquals("x",
            actual.getDocument("$SET").getString("bareField.deep").getValue());
  }

  /**
   * Alias with array index immediately followed by another alias: {@code #l[0].#nested}.
   */
  @Test
  public void testAliasArrayIndexThenAliasPath() {
    String ddbUpdateExp = "SET #l[0].#nested = :v";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("x").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = new HashMap<>();
    aliases.put("#l", "myList");
    aliases.put("#nested", "deepField");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    Assert.assertEquals("x",
            actual.getDocument("$SET").getString("myList[0].deepField").getValue());
  }

  /**
   * Alias path used inside {@code if_not_exists}: {@code if_not_exists(#a.b, :v)}.
   */
  @Test
  public void testAliasPathInsideIfNotExists() {
    String ddbUpdateExp = "SET col = if_not_exists(#a.b, :v)";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("fallback").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = Collections.singletonMap("#a", "topMap");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    BsonDocument inner = actual.getDocument("$SET")
            .getDocument("col").getDocument("$IF_NOT_EXISTS");
    Assert.assertEquals("fallback", inner.getString("topMap.b").getValue());
  }

  /**
   * Alias inside the {@code if_not_exists} that lives inside {@code list_append}:
   * {@code list_append(if_not_exists(#l, :empty), :items)}.
   */
  @Test
  public void testAliasInsideIfNotExistsInsideListAppend() {
    String ddbUpdateExp = "SET col = list_append(if_not_exists(#l, :empty), :items)";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":empty",
            AttributeValue.builder().l(Collections.emptyList()).build());
    attributeMap.put(":items", AttributeValue.builder().l(
            AttributeValue.builder().s("a").build()).build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = Collections.singletonMap("#l", "events");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    BsonArray operands = actual.getDocument("$SET")
            .getDocument("col").getArray("$LIST_APPEND");
    BsonDocument inner = operands.get(0).asDocument().getDocument("$IF_NOT_EXISTS");
    Assert.assertTrue("expected fallback under resolved alias key 'events'",
            inner.containsKey("events"));
  }

  /**
   * Multi-step path with aliases mixed with bare names and an array index, e.g.
   * {@code #pr.#5star[1].name}.
   */
  @Test
  public void testNestedAliasPath() {
    String ddbUpdateExp = "SET #pr.#5star[1].name = :v";
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":v", AttributeValue.builder().s("Alice").build());
    BsonDocument values = DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
    Map<String, String> aliases = new HashMap<>();
    aliases.put("#pr", "ProductReviews");
    aliases.put("#5star", "FiveStar");
    BsonDocument actual = UpdateExpressionToBson
            .toBsonUpdateDocument(ddbUpdateExp, values, aliases);
    Assert.assertEquals("Alice",
            actual.getDocument("$SET").getString("ProductReviews.FiveStar[1].name").getValue());
  }

  private static BsonDocument getListAppendComparisonValuesMap() {
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":listVal", AttributeValue.builder().l(
            AttributeValue.builder().s("c").build(),
            AttributeValue.builder().s("d").build()).build());
    attributeMap.put(":listVal2", AttributeValue.builder().l(
            AttributeValue.builder().s("e").build(),
            AttributeValue.builder().s("f").build()).build());
    attributeMap.put(":emptyList",
            AttributeValue.builder().l(Collections.emptyList()).build());
    attributeMap.put(":one", AttributeValue.builder().n("1").build());
    attributeMap.put(":notList", AttributeValue.builder().s("scalar").build());
    return DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
  }

  private static BsonDocument getComparisonValuesMap() {
    Map<String, AttributeValue> attributeMap = new HashMap<>();
    attributeMap.put(":aCol", AttributeValue.builder().ns("1", "2", "3").build());
    attributeMap.put(":newTitle", AttributeValue.builder().s("Cycle_1234_new").build());
    attributeMap.put(":newId", AttributeValue.builder().s("12345").build());
    attributeMap.put(":newIdNeg", AttributeValue.builder().n("-12345").build());
    attributeMap.put(":ColorList", AttributeValue.builder().l(
        AttributeValue.builder().s("Black").build(),
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("White"))).build(),
        AttributeValue.builder().s("Silver").build()
    ).build());
    attributeMap.put(":Id1",
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("ID_101"))).build());
    attributeMap.put(":NList001", AttributeValue.builder().n("12.22").build());
    attributeMap.put(":NList003", AttributeValue.builder().nul(true).build());
    attributeMap.put(":NList004", AttributeValue.builder().bool(true).build());
    attributeMap.put(":attr5_0", AttributeValue.builder().n("10").build());
    attributeMap.put(":NList1_0", AttributeValue.builder().ss("Updated_set_01", "Updated_set_02").build());
    attributeMap.put(":NestedList1_ISBN", AttributeValue.builder().s("111-1111111122").build());
    attributeMap.put(":NestedList12_00", AttributeValue.builder().ss("xyz01234", "abc01234").build());
    attributeMap.put(":NestedList12_01", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("val03")),
        SdkBytes.fromByteArray(Bytes.toBytes("val04"))
    ).build());
    attributeMap.put(":AddedPics", AttributeValue.builder().ss(
        "1235@_rear.jpg",
        "xyz5@_rear.jpg").build());
    attributeMap.put(":PictureBinarySet01", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("123_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("xyz_front.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("xyz_front.jpg_no"))
    ).build());
    attributeMap.put(":NSet01", AttributeValue.builder().ns("-6830.5555", "-48695").build());
    attributeMap.put(":NestedList1TitleSet01", AttributeValue.builder().ss("Book 1010 Title",
        "Book 1011 Title").build());
    return DdbAttributesToBsonDocument.getRawBsonDocument(attributeMap);
  }

}
