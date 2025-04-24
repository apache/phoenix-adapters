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

import java.util.HashMap;
import java.util.Map;

import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;

public class DocumentDdbAttributesTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDdbAttributesTest.class);

  @Test(timeout = 120000)
  public void test1() {
    Map<String, AttributeValue> item1 = getItem1();
    BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item1);
    LOGGER.info("Converted document: {}", bsonDocument);
    Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDocument);
    Assert.assertEquals(item1, item2);
  }

  @Test(timeout = 120000)
  public void test2() {
    Map<String, AttributeValue> item1 = getItem2();
    BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item1);
    LOGGER.info("Converted document: {}", bsonDocument);
    Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDocument);
    Assert.assertEquals(item1, item2);
  }

  @Test(timeout = 120000)
  public void test3() {
    Map<String, AttributeValue> item1 = getItem3();
    BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item1);
    LOGGER.info("Converted document: {}", bsonDocument);
    Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDocument);
    Assert.assertEquals(item1, item2);
  }

  @Test(timeout = 120000)
  public void test4() {
    Map<String, AttributeValue> item1 = getItem4();
    BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item1);
    LOGGER.info("Converted document: {}", bsonDocument);
    Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDocument);
    Assert.assertEquals(item1, item2);
  }

  protected static Map<String, AttributeValue> getItem1() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", AttributeValue.builder().s("str_val_0").build());
    commonAttributeValues1(item);
    return item;
  }

  protected static Map<String, AttributeValue> getItemWithBinary1() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_0"))).build());
    item.put("pk2", AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_2"))).build());
    commonAttributeValues1(item);
    return item;
  }

  private static void commonAttributeValues1(Map<String, AttributeValue> item) {
    item.put("attr_1", AttributeValue.builder().n("1295.03").build());
    item.put("attr_5", AttributeValue.builder().l(
        AttributeValue.builder().n("1234").build(),
        AttributeValue.builder().s("str001").build(),
        AttributeValue.builder().b(SdkBytes.fromByteArray(
            new byte[] {0, 1, 2, 3, 4, 5})).build()).build());
    Map<String, AttributeValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", AttributeValue.builder().s("str_val_0").build());
    nMap1.put("n_attr_1", AttributeValue.builder().n("1295.03").build());
    String bytesFieldVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesFieldVal1.getBytes();
    nMap1.put("n_attr_2", AttributeValue.builder().b(SdkBytes.fromByteArray(
        bytesAttrVal1)).build());
    nMap1.put("n_attr_3", AttributeValue.builder().bool(true).build());
    nMap1.put("n_attr_4", AttributeValue.builder().nul(true).build());
    item.put("attr_6", AttributeValue.builder().m(nMap1).build());
    item.put("Id", AttributeValue.builder().n("101.01").build());
    item.put("IdS", AttributeValue.builder().s("101.01").build());
    item.put("Id2", AttributeValue.builder().n("101.01").build());
    item.put("ColorBytes",
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
    item.put("RelatedItems",
        AttributeValue.builder().ns("1234", "-485.45582904", "123.0948", "0.111").build());
    item.put("Pictures", AttributeValue.builder().ss(
        "http://example1.com/products/123_rear.jpg",
        "http://example2.com/products/xyz_rear.jpg",
        "http://example3.com/products/123_front.jpg",
        "http://example4.com/products/123_front.jpg"
    ).build());
    item.put("PictureBinarySet", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("http://example1.com/products/123_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("http://example2.com/products/xyz_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("http://example3.com/products/123_front.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("http://example4.com/products/123_front.jpg"))
    ).build());
    item.put("Title", AttributeValue.builder().s("Book 101 Title").build());
    item.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    item.put("InPublication", AttributeValue.builder().bool(false).build());
    Map<String, AttributeValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", AttributeValue.builder().n("101.01").build());
    nestedMap1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
    nestedMap1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    nestedMap1.put("InPublication", AttributeValue.builder().bool(false).build());
    nestedMap1.put("NList1",
        AttributeValue.builder().l(AttributeValue.builder().s("NListVal01").build(),
            AttributeValue.builder().n("-0.00234").build()).build());
    item.put("NestedMap1", AttributeValue.builder().m(nestedMap1).build());
    Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", AttributeValue.builder().n("101.01").build());
    nestedList1Map1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
    nestedList1Map1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    nestedList1Map1.put("InPublication", AttributeValue.builder().bool(false).build());
    nestedList1Map1.put("IdSet",
        AttributeValue.builder().ns("204850.697038475",
            "-3947860.4863946",
            "20576024",
            "19306873",
            "4869067048362759068").build());
    nestedList1Map1.put("TitleSet",
        AttributeValue.builder().ss("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title").build());
    nestedList1Map1.put("BinaryTitleSet", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1010 Title Binary")),
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1011 Title Binary")),
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1111 Title Binary"))
    ).build());
    item.put("NestedList1",
        AttributeValue.builder().l(AttributeValue.builder().n("-485.34").build(),
            AttributeValue.builder().s("1234abcd").build(),
            AttributeValue.builder().l(AttributeValue.builder().s("xyz0123").build(),
                AttributeValue.builder().m(nestedList1Map1).build()).build()).build());
  }

  protected static Map<String, AttributeValue> getItem2() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", AttributeValue.builder().s("str_val_0").build());
    commonAttributeValues2(item);
    return item;
  }

  protected static Map<String, AttributeValue> getItemWithBinary2() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_0"))).build());
    item.put("pk2", AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_2"))).build());
    commonAttributeValues2(item);
    return item;
  }

  private static void commonAttributeValues2(Map<String, AttributeValue> item) {
    item.put("attr_1", AttributeValue.builder().n("1295.03").build());
    item.put("attr_5", AttributeValue.builder().l(
        AttributeValue.builder().n("1234").build(),
        AttributeValue.builder().s("str001").build(),
        AttributeValue.builder().b(SdkBytes.fromByteArray(
            new byte[] {0, 1, 2, 3, 4, 5})).build()).build());
    Map<String, AttributeValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", AttributeValue.builder().s("str_val_0").build());
    nMap1.put("n_attr_1", AttributeValue.builder().n("1295.03").build());
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", AttributeValue.builder().b(SdkBytes.fromByteArray(
        bytesAttrVal1)).build());
    nMap1.put("n_attr_3", AttributeValue.builder().bool(true).build());
    nMap1.put("n_attr_4", AttributeValue.builder().nul(true).build());
    item.put("attr_6", AttributeValue.builder().m(nMap1).build());
    item.put("Id", AttributeValue.builder().n("-102.01").build());
    item.put("IdS", AttributeValue.builder().s("101.01").build());
    item.put("Id2", AttributeValue.builder().n("101234.9921").build());
    item.put("ColorBytes",
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
    item.put("RelatedItems",
        AttributeValue.builder().ns("1234", "-485.45582904", "123.0948", "0.111").build());
    item.put("Pictures", AttributeValue.builder().ss(
        "1123_rear.jpg",
        "2xyz_rear.jpg",
        "3123_front.jpg",
        "4xyz_front.jpg"
    ).build());
    item.put("PictureBinarySet", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("1123_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("2xyz_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("3123_front.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("4xyz_front.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("5123abc_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("6xyzabc_rear.jpg"))
    ).build());
    item.put("Title", AttributeValue.builder().s("Book 101 Title").build());
    item.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    item.put("InPublication", AttributeValue.builder().bool(false).build());
    Map<String, AttributeValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", AttributeValue.builder().n("101.01").build());
    nestedMap1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
    nestedMap1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    nestedMap1.put("InPublication", AttributeValue.builder().bool(false).build());
    nestedMap1.put("NList1",
        AttributeValue.builder().l(AttributeValue.builder().s("NListVal01").build(),
            AttributeValue.builder().n("-0.00234").build(),
            AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("to_be_removed"))).build()).build());
    Map<String, AttributeValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", AttributeValue.builder().n("101.22").build());
    nestedMap2.put("Title", AttributeValue.builder().s("Book 10122 Title").build());
    nestedMap2.put("ISBN", AttributeValue.builder().s("111-1111111111999").build());
    nestedMap2.put("InPublication", AttributeValue.builder().bool(true).build());
    nestedMap2.put("NList",
        AttributeValue.builder().l(AttributeValue.builder().s("NListVal01").build(),
            AttributeValue.builder().n("-0.00234").build()).build());
    nestedMap1.put("NestedMap2",
        AttributeValue.builder().m(nestedMap2).build());
    nestedMap1.put("NSet1",
        AttributeValue.builder().ns("123.45", "9586.7778", "-124", "-6830.5555", "10238",
            "-48695").build());
    item.put("NestedMap1", AttributeValue.builder().m(nestedMap1).build());
    Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", AttributeValue.builder().n("9991234.0231").build());
    nestedList1Map1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
    nestedList1Map1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    nestedList1Map1.put("InPublication", AttributeValue.builder().bool(false).build());
    nestedList1Map1.put("IdSet",
        AttributeValue.builder().ns("204850.69703847",
            "-3945786.4869476",
            "20576024",
            "19306873",
            "486906704836274959").build());
    nestedList1Map1.put("TitleSet1",
        AttributeValue.builder().ss("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title").build());
    nestedList1Map1.put("TitleSet2",
        AttributeValue.builder().ss("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title").build());
    nestedList1Map1.put("BinaryTitleSet", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1010 Title Binary")),
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1011 Title Binary")),
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1111 Title Binary"))
    ).build());
    item.put("NestedList1",
        AttributeValue.builder().l(AttributeValue.builder().n("-485.34").build(),
            AttributeValue.builder().s("1234abcd").build(),
            AttributeValue.builder().l(AttributeValue.builder().s("xyz0123").build(),
                AttributeValue.builder().m(nestedList1Map1).build()).build()).build());
    item.put("NestedList12",
        AttributeValue.builder().l(AttributeValue.builder().n("-485.34").build(),
            AttributeValue.builder().s("1234abcd").build(),
            AttributeValue.builder().l(
                AttributeValue.builder().ss("xyz0123").build(),
                AttributeValue.builder().bs(
                    SdkBytes.fromByteArray(Bytes.toBytes("val01")),
                    SdkBytes.fromByteArray(Bytes.toBytes("val02")),
                    SdkBytes.fromByteArray(Bytes.toBytes("val03"))).build()).build()).build());
  }

  private static Map<String, AttributeValue> getItem3() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", AttributeValue.builder().s("str_val_0").build());
    item.put("AddedId", AttributeValue.builder().n("10").build());
    item.put("attr_1", AttributeValue.builder().n("1295.03").build());
    item.put("Id1",
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("ID_101"))).build());
    item.put("attr_5", AttributeValue.builder().l(
        AttributeValue.builder().n("1234").build(),
        AttributeValue.builder().s("str001").build(),
        AttributeValue.builder().b(SdkBytes.fromByteArray(
            new byte[] {0, 1, 2, 3, 4, 5})).build()).build());
    Map<String, AttributeValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", AttributeValue.builder().s("str_val_0").build());
    nMap1.put("n_attr_1", AttributeValue.builder().n("1295.03123").build());
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", AttributeValue.builder().b(SdkBytes.fromByteArray(
        bytesAttrVal1)).build());
    nMap1.put("n_attr_3", AttributeValue.builder().bool(true).build());
    nMap1.put("n_attr_4", AttributeValue.builder().nul(true).build());
    item.put("attr_6", AttributeValue.builder().m(nMap1).build());
    item.put("Id", AttributeValue.builder().s("12345").build());
    item.put("ColorBytes",
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
    item.put("RelatedItems",
        AttributeValue.builder().ns("1234",
            "-485.45582904",
            "123.0948",
            "0.111").build());
    item.put("Pictures", AttributeValue.builder().ss(
        "123_rear.jpg",
        "1235@_rear.jpg",
        "xyz5@_rear.jpg",
        "xyz_rear.jpg",
        "123_front.jpg",
        "xyz_front.jpg"
    ).build());
    item.put("PictureBinarySet", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("xyz_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("123_front.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("123abc_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("xyzabc_rear.jpg"))
    ).build());
    item.put("Title", AttributeValue.builder().s("Cycle_1234_new").build());
    item.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    item.put("InPublication", AttributeValue.builder().bool(false).build());
    Map<String, AttributeValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", AttributeValue.builder().n("101.01").build());
    nestedMap1.put("AddedId", AttributeValue.builder().n("10").build());
    nestedMap1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    nestedMap1.put("InPublication", AttributeValue.builder().bool(false).build());
    nestedMap1.put("NList1", AttributeValue.builder().l(
        AttributeValue.builder().ss("Updated_set_01", "Updated_set_02").build(),
        AttributeValue.builder().n("-0.00234").build()).build());
    nestedMap1.put("ColorList", AttributeValue.builder().l(
        AttributeValue.builder().s("Black").build(),
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("White"))).build(),
        AttributeValue.builder().s("Silver").build()
    ).build());
    Map<String, AttributeValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", AttributeValue.builder().n("-12243.78").build());
    nestedMap2.put("NewID", AttributeValue.builder().s("12345").build());
    nestedMap2.put("Title", AttributeValue.builder().s("Book 10122 Title").build());
    nestedMap2.put("ISBN", AttributeValue.builder().s("111-1111111111999").build());
    nestedMap2.put("NList",
        AttributeValue.builder().l(
            AttributeValue.builder().n("12.22").build(),
            AttributeValue.builder().n("-0.00234").build(),
            AttributeValue.builder().nul(true).build()).build());
    nestedMap1.put("NestedMap2",
        AttributeValue.builder().m(nestedMap2).build());
    nestedMap1.put("NSet1",
        AttributeValue.builder().ns("123.45", "9586.7778", "-124", "10238").build());
    item.put("NestedMap1", AttributeValue.builder().m(nestedMap1).build());
    Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", AttributeValue.builder().n("101.01").build());
    nestedList1Map1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
    nestedList1Map1.put("ISBN", AttributeValue.builder().s("111-1111111122").build());
    nestedList1Map1.put("InPublication", AttributeValue.builder().bool(false).build());
    nestedList1Map1.put("IdSet",
        AttributeValue.builder().ns("204850.697038423",
            "-9457860.98693947",
            "20576024",
            "19306873",
            "9067048362749590684").build());
    nestedList1Map1.put("TitleSet2",
        AttributeValue.builder().ss(
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title").build());
    nestedList1Map1.put("BinaryTitleSet", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1010 Title Binary")),
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1011 Title Binary")),
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1111 Title Binary"))
    ).build());
    item.put("NestedList1",
        AttributeValue.builder().l(AttributeValue.builder().n("-485.34").build(),
            AttributeValue.builder().s("1234abcd").build(),
            AttributeValue.builder().l(AttributeValue.builder().s("xyz0123").build(),
                AttributeValue.builder().m(nestedList1Map1).build()).build()).build());
    item.put("NestedList12",
        AttributeValue.builder().l(
            AttributeValue.builder().n("-485.34").build(),
            AttributeValue.builder().s("1234abcd").build(),
            AttributeValue.builder().l(
                AttributeValue.builder().ss("xyz0123", "xyz01234", "abc01234").build(),
                AttributeValue.builder().bs(
                    SdkBytes.fromByteArray(Bytes.toBytes("val01")),
                    SdkBytes.fromByteArray(Bytes.toBytes("val02")),
                    SdkBytes.fromByteArray(Bytes.toBytes("val03")),
                    SdkBytes.fromByteArray(Bytes.toBytes("val04"))).build(),
                AttributeValue.builder().ns(
                    "-234.56",
                    "123",
                    "93756.93475960549",
                    "9375572.6870899").build()).build()).build());
    return item;
  }

  private static Map<String, AttributeValue> getItem4() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", AttributeValue.builder().s("str_val_0").build());
    item.put("AddedId", AttributeValue.builder().n("10").build());
    item.put("attr_1", AttributeValue.builder().n("1295.03").build());
    item.put("Id1", AttributeValue.builder().s("12345").build());
    item.put("attr_5", AttributeValue.builder().l(
        AttributeValue.builder().n("1224").build(),
        AttributeValue.builder().s("str001").build(),
        AttributeValue.builder().b(SdkBytes.fromByteArray(
            new byte[] {0, 1, 2, 3, 4, 5})).build()).build());
    Map<String, AttributeValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", AttributeValue.builder().s("str_val_0").build());
    nMap1.put("n_attr_1", AttributeValue.builder().n("1223456.03").build());
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", AttributeValue.builder().b(SdkBytes.fromByteArray(
        bytesAttrVal1)).build());
    nMap1.put("n_attr_3", AttributeValue.builder().bool(true).build());
    nMap1.put("n_attr_4", AttributeValue.builder().nul(true).build());
    item.put("attr_6", AttributeValue.builder().m(nMap1).build());
    item.put("Id", AttributeValue.builder().s("12345").build());
    item.put("ColorBytes",
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
    item.put("RelatedItems",
        AttributeValue.builder().ns("1234",
            "-485.45582904",
            "123.0948",
            "0.111").build());
    item.put("Pictures", AttributeValue.builder().ss(
        "123_rear.jpg",
        "1235@_rear.jpg",
        "xyz5@_rear.jpg",
        "xyz_rear.jpg",
        "123_front.jpg",
        "xyz_front.jpg"
    ).build());
    item.put("PictureBinarySet", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("xyz_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("123_front.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("123abc_rear.jpg")),
        SdkBytes.fromByteArray(Bytes.toBytes("xyzabc_rear.jpg"))
    ).build());
    item.put("Title", AttributeValue.builder().s("Cycle_1234_new").build());
    item.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    item.put("InPublication", AttributeValue.builder().bool(false).build());
    Map<String, AttributeValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", AttributeValue.builder().n("101.01").build());
    nestedMap1.put("AddedId", AttributeValue.builder().n("10").build());
    nestedMap1.put("ISBN", AttributeValue.builder().s("111-1111111111").build());
    nestedMap1.put("InPublication", AttributeValue.builder().bool(false).build());
    nestedMap1.put("NList1", AttributeValue.builder().l(
        AttributeValue.builder().ss("Updated_set_01", "Updated_set_02").build(),
        AttributeValue.builder().n("-0.00234").build()).build());
    nestedMap1.put("ColorList", AttributeValue.builder().l(
        AttributeValue.builder().s("Black").build(),
        AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("White"))).build(),
        AttributeValue.builder().s("Silver").build()
    ).build());
    Map<String, AttributeValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", AttributeValue.builder().n("-12243.78").build());
    nestedMap2.put("NewID", AttributeValue.builder().s("12345").build());
    nestedMap2.put("Title", AttributeValue.builder().s("Book 10122 Title").build());
    nestedMap2.put("ISBN", AttributeValue.builder().s("111-1111111111999").build());
    nestedMap2.put("NList",
        AttributeValue.builder().l(
            AttributeValue.builder().n("12.22").build(),
            AttributeValue.builder().n("-0.00234").build(),
            AttributeValue.builder().nul(true).build()).build());
    nestedMap1.put("NestedMap2",
        AttributeValue.builder().m(nestedMap2).build());
    nestedMap1.put("NSet1",
        AttributeValue.builder().ns("123.45",
            "9586.7778",
            "-124",
            "10238",
            "-0.29475029304").build());
    item.put("NestedMap1", AttributeValue.builder().m(nestedMap1).build());
    Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", AttributeValue.builder().n("101.01").build());
    nestedList1Map1.put("Title", AttributeValue.builder().s("Book 101 Title").build());
    nestedList1Map1.put("ISBN", AttributeValue.builder().s("111-1111111122").build());
    nestedList1Map1.put("InPublication", AttributeValue.builder().bool(false).build());
    nestedList1Map1.put("IdSet",
        AttributeValue.builder().ns("2048502.69703847",
            "-45786.4869476",
            "20576024",
            "19306873",
            "83627495940380684").build());
    nestedList1Map1.put("TitleSet2",
        AttributeValue.builder().ss(
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title").build());
    nestedList1Map1.put("BinaryTitleSet", AttributeValue.builder().bs(
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1010 Title Binary")),
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1011 Title Binary")),
        SdkBytes.fromByteArray(Bytes.toBytes("Book 1111 Title Binary"))
    ).build());
    item.put("NestedList1",
        AttributeValue.builder().l(AttributeValue.builder().n("-473.11999999999995").build(),
            AttributeValue.builder().s("1234abcd").build(),
            AttributeValue.builder().l(AttributeValue.builder().s("xyz0123").build(),
                AttributeValue.builder().m(nestedList1Map1).build()).build(),
            AttributeValue.builder().nul(true).build(),
            AttributeValue.builder().bool(true).build()).build());
    item.put("NestedList12",
        AttributeValue.builder().l(
            AttributeValue.builder().n("-485.34").build(),
            AttributeValue.builder().s("1234abcd").build(),
            AttributeValue.builder().l(
                AttributeValue.builder().ss("xyz0123", "xyz01234", "abc01234").build(),
                AttributeValue.builder().bs(
                    SdkBytes.fromByteArray(Bytes.toBytes("val01")),
                    SdkBytes.fromByteArray(Bytes.toBytes("val02")),
                    SdkBytes.fromByteArray(Bytes.toBytes("val03")),
                    SdkBytes.fromByteArray(Bytes.toBytes("val04"))).build()).build()).build());
    return item;
  }

}
