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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import org.bson.BsonDocument;
import org.bson.RawBsonDocument;
import org.junit.Assert;
import org.junit.Test;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.ddb.bson.UpdateExpressionDdbToBson;

public class UpdateExpressionConversionTest {

  @Test(timeout = 120000)
  public void test1() {

    String ddbUpdateExp = "SET Title = :newTitle, Id = :newId , " +
        "NestedMap1.ColorList = :ColorList , "
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
        UpdateExpressionDdbToBson.getBsonDocumentForUpdateExpression(ddbUpdateExp,
            getComparisonValuesMap()));
  }

  private static BsonDocument getComparisonValuesMap() {
    Map<String, AttributeValue> attributeMap = new HashMap<>();
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
