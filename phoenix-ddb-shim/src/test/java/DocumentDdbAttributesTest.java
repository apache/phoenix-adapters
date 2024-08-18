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

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.bson.BsonDocument;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;

public class DocumentDdbAttributesTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(DocumentDdbAttributesTest.class);

  @Test
  public void test1() {
    Map<String, AttributeValue> item1 = getItem1();
    BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item1);
    LOGGER.info("Converted document: {}", bsonDocument);
    Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDocument);
    Assert.assertEquals(item1, item2);
  }

  @Test
  public void test2() {
    Map<String, AttributeValue> item1 = getItem2();
    BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item1);
    LOGGER.info("Converted document: {}", bsonDocument);
    Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDocument);
    Assert.assertEquals(item1, item2);
  }

  @Test
  public void test3() {
    Map<String, AttributeValue> item1 = getItem3();
    BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item1);
    LOGGER.info("Converted document: {}", bsonDocument);
    Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDocument);
    Assert.assertEquals(item1, item2);
  }

  @Test
  public void test4() {
    Map<String, AttributeValue> item1 = getItem4();
    BsonDocument bsonDocument = DdbAttributesToBsonDocument.getBsonDocument(item1);
    LOGGER.info("Converted document: {}", bsonDocument);
    Map<String, AttributeValue> item2 = BsonDocumentToDdbAttributes.getFullItem(bsonDocument);
    Assert.assertEquals(item1, item2);
  }

  private static Map<String, AttributeValue> getItem1() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", new AttributeValue().withS("str_val_0"));
    item.put("attr_1", new AttributeValue().withN("1295.03"));
    item.put("attr_5", new AttributeValue().withL(
        new AttributeValue().withN("1234"),
        new AttributeValue().withS("str001"),
        new AttributeValue().withB(ByteBuffer.wrap(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, AttributeValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", new AttributeValue().withS("str_val_0"));
    nMap1.put("n_attr_1", new AttributeValue().withN("1295.03"));
    String bytesFieldVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesFieldVal1.getBytes();
    nMap1.put("n_attr_2", new AttributeValue().withB(ByteBuffer.wrap(
        bytesAttrVal1)));
    nMap1.put("n_attr_3", new AttributeValue().withBOOL(true));
    nMap1.put("n_attr_4", new AttributeValue().withNULL(true));
    item.put("attr_6", new AttributeValue().withM(nMap1));
    item.put("Id", new AttributeValue().withN("101.01"));
    item.put("IdS", new AttributeValue().withS("101.01"));
    item.put("Id2", new AttributeValue().withN("101.01"));
    item.put("ColorBytes",
        new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("Black"))));
    item.put("RelatedItems",
        new AttributeValue().withNS("1234", "-485.45582904", "123.0948", "0.111"));
    item.put("Pictures", new AttributeValue().withSS(
        "http://example.com/products/123_rear.jpg",
        "http://example.com/products/xyz_rear.jpg",
        "http://example.com/products/123_front.jpg",
        "http://example.com/products/123_front.jpg"
    ));
    item.put("PictureBinarySet", new AttributeValue().withBS(
        ByteBuffer.wrap(Bytes.toBytes("http://example.com/products/123_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("http://example.com/products/xyz_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("http://example.com/products/123_front.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("http://example.com/products/123_front.jpg"))
    ));
    item.put("Title", new AttributeValue().withS("Book 101 Title"));
    item.put("ISBN", new AttributeValue().withS("111-1111111111"));
    item.put("InPublication", new AttributeValue().withBOOL(false));
    Map<String, AttributeValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", new AttributeValue().withN("101.01"));
    nestedMap1.put("Title", new AttributeValue().withS("Book 101 Title"));
    nestedMap1.put("ISBN", new AttributeValue().withS("111-1111111111"));
    nestedMap1.put("InPublication", new AttributeValue().withBOOL(false));
    nestedMap1.put("NList1",
        new AttributeValue().withL(new AttributeValue().withS("NListVal01"),
            new AttributeValue().withN("-0.00234")));
    item.put("NestedMap1", new AttributeValue().withM(nestedMap1));
    Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new AttributeValue().withN("101.01"));
    nestedList1Map1.put("Title", new AttributeValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new AttributeValue().withS("111-1111111111"));
    nestedList1Map1.put("InPublication", new AttributeValue().withBOOL(false));
    nestedList1Map1.put("IdSet",
        new AttributeValue().withNS("204850.697038475",
            "-3947860.4863946",
            "20576024",
            "19306873",
            "4869067048362759068"));
    nestedList1Map1.put("TitleSet",
        new AttributeValue().withSS("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title"));
    nestedList1Map1.put("BinaryTitleSet", new AttributeValue().withBS(
        ByteBuffer.wrap(Bytes.toBytes("Book 1010 Title Binary")),
        ByteBuffer.wrap(Bytes.toBytes("Book 1011 Title Binary")),
        ByteBuffer.wrap(Bytes.toBytes("Book 1111 Title Binary"))
    ));
    item.put("NestedList1",
        new AttributeValue().withL(new AttributeValue().withN("-485.34"),
            new AttributeValue().withS("1234abcd"),
            new AttributeValue().withL(new AttributeValue().withS("xyz0123"),
                new AttributeValue().withM(nestedList1Map1))));
    return item;
  }

  private static Map<String, AttributeValue> getItem2() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", new AttributeValue().withS("str_val_0"));
    item.put("attr_1", new AttributeValue().withN("1295.03"));
    item.put("attr_5", new AttributeValue().withL(
        new AttributeValue().withN("1234"),
        new AttributeValue().withS("str001"),
        new AttributeValue().withB(ByteBuffer.wrap(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, AttributeValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", new AttributeValue().withS("str_val_0"));
    nMap1.put("n_attr_1", new AttributeValue().withN("1295.03"));
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", new AttributeValue().withB(ByteBuffer.wrap(
        bytesAttrVal1)));
    nMap1.put("n_attr_3", new AttributeValue().withBOOL(true));
    nMap1.put("n_attr_4", new AttributeValue().withNULL(true));
    item.put("attr_6", new AttributeValue().withM(nMap1));
    item.put("Id", new AttributeValue().withN("-102.01"));
    item.put("IdS", new AttributeValue().withS("101.01"));
    item.put("Id2", new AttributeValue().withN("101234.9921"));
    item.put("ColorBytes",
        new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("Black"))));
    item.put("RelatedItems",
        new AttributeValue().withNS("1234", "-485.45582904", "123.0948", "0.111"));
    item.put("Pictures", new AttributeValue().withSS(
        "123_rear.jpg",
        "xyz_rear.jpg",
        "123_front.jpg",
        "xyz_front.jpg"
    ));
    item.put("PictureBinarySet", new AttributeValue().withBS(
        ByteBuffer.wrap(Bytes.toBytes("123_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("xyz_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("123_front.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("xyz_front.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("123abc_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("xyzabc_rear.jpg"))
    ));
    item.put("Title", new AttributeValue().withS("Book 101 Title"));
    item.put("ISBN", new AttributeValue().withS("111-1111111111"));
    item.put("InPublication", new AttributeValue().withBOOL(false));
    Map<String, AttributeValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", new AttributeValue().withN("101.01"));
    nestedMap1.put("Title", new AttributeValue().withS("Book 101 Title"));
    nestedMap1.put("ISBN", new AttributeValue().withS("111-1111111111"));
    nestedMap1.put("InPublication", new AttributeValue().withBOOL(false));
    nestedMap1.put("NList1",
        new AttributeValue().withL(new AttributeValue().withS("NListVal01"),
            new AttributeValue().withN("-0.00234"),
            new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("to_be_removed")))));
    Map<String, AttributeValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", new AttributeValue().withN("101.22"));
    nestedMap2.put("Title", new AttributeValue().withS("Book 10122 Title"));
    nestedMap2.put("ISBN", new AttributeValue().withS("111-1111111111999"));
    nestedMap2.put("InPublication", new AttributeValue().withBOOL(true));
    nestedMap2.put("NList",
        new AttributeValue().withL(new AttributeValue().withS("NListVal01"),
            new AttributeValue().withN("-0.00234")));
    nestedMap1.put("NestedMap2",
        new AttributeValue().withM(nestedMap2));
    nestedMap1.put("NSet1",
        new AttributeValue().withNS("123.45", "9586.7778", "-124", "-6830.5555", "10238",
            "-48695"));
    item.put("NestedMap1", new AttributeValue().withM(nestedMap1));
    Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new AttributeValue().withN("9991234.0231"));
    nestedList1Map1.put("Title", new AttributeValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new AttributeValue().withS("111-1111111111"));
    nestedList1Map1.put("InPublication", new AttributeValue().withBOOL(false));
    nestedList1Map1.put("IdSet",
        new AttributeValue().withNS("204850.69703847",
            "-3945786.4869476",
            "20576024",
            "19306873",
            "486906704836274959"));
    nestedList1Map1.put("TitleSet1",
        new AttributeValue().withSS("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title"));
    nestedList1Map1.put("TitleSet2",
        new AttributeValue().withSS("Book 1010 Title", "Book 1011 Title",
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title"));
    nestedList1Map1.put("BinaryTitleSet", new AttributeValue().withBS(
        ByteBuffer.wrap(Bytes.toBytes("Book 1010 Title Binary")),
        ByteBuffer.wrap(Bytes.toBytes("Book 1011 Title Binary")),
        ByteBuffer.wrap(Bytes.toBytes("Book 1111 Title Binary"))
    ));
    item.put("NestedList1",
        new AttributeValue().withL(new AttributeValue().withN("-485.34"),
            new AttributeValue().withS("1234abcd"),
            new AttributeValue().withL(new AttributeValue().withS("xyz0123"),
                new AttributeValue().withM(nestedList1Map1))));
    item.put("NestedList12",
        new AttributeValue().withL(new AttributeValue().withN("-485.34"),
            new AttributeValue().withS("1234abcd"),
            new AttributeValue().withL(
                new AttributeValue().withSS("xyz0123"),
                new AttributeValue().withBS(
                    ByteBuffer.wrap(Bytes.toBytes("val01")),
                    ByteBuffer.wrap(Bytes.toBytes("val02")),
                    ByteBuffer.wrap(Bytes.toBytes("val03"))))));
    return item;
  }

  private static Map<String, AttributeValue> getItem3() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", new AttributeValue().withS("str_val_0"));
    item.put("AddedId", new AttributeValue().withN("10"));
    item.put("attr_1", new AttributeValue().withN("1295.03"));
    item.put("Id1",
        new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("ID_101"))));
    item.put("attr_5", new AttributeValue().withL(
        new AttributeValue().withN("1234"),
        new AttributeValue().withS("str001"),
        new AttributeValue().withB(ByteBuffer.wrap(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, AttributeValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", new AttributeValue().withS("str_val_0"));
    nMap1.put("n_attr_1", new AttributeValue().withN("1295.03123"));
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", new AttributeValue().withB(ByteBuffer.wrap(
        bytesAttrVal1)));
    nMap1.put("n_attr_3", new AttributeValue().withBOOL(true));
    nMap1.put("n_attr_4", new AttributeValue().withNULL(true));
    item.put("attr_6", new AttributeValue().withM(nMap1));
    item.put("Id", new AttributeValue().withS("12345"));
    item.put("ColorBytes",
        new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("Black"))));
    item.put("RelatedItems",
        new AttributeValue().withNS("1234",
            "-485.45582904",
            "123.0948",
            "0.111"));
    item.put("Pictures", new AttributeValue().withSS(
        "123_rear.jpg",
        "1235@_rear.jpg",
        "xyz5@_rear.jpg",
        "xyz_rear.jpg",
        "123_front.jpg",
        "xyz_front.jpg"
    ));
    item.put("PictureBinarySet", new AttributeValue().withBS(
        ByteBuffer.wrap(Bytes.toBytes("xyz_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("123_front.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("123abc_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("xyzabc_rear.jpg"))
    ));
    item.put("Title", new AttributeValue().withS("Cycle_1234_new"));
    item.put("ISBN", new AttributeValue().withS("111-1111111111"));
    item.put("InPublication", new AttributeValue().withBOOL(false));
    Map<String, AttributeValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", new AttributeValue().withN("101.01"));
    nestedMap1.put("AddedId", new AttributeValue().withN("10"));
    nestedMap1.put("ISBN", new AttributeValue().withS("111-1111111111"));
    nestedMap1.put("InPublication", new AttributeValue().withBOOL(false));
    nestedMap1.put("NList1", new AttributeValue().withL(
        new AttributeValue().withSS("Updated_set_01", "Updated_set_02"),
        new AttributeValue().withN("-0.00234")));
    nestedMap1.put("ColorList", new AttributeValue().withL(
        new AttributeValue().withS("Black"),
        new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("White"))),
        new AttributeValue().withS("Silver")
    ));
    Map<String, AttributeValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", new AttributeValue().withN("-12243.78"));
    nestedMap2.put("NewID", new AttributeValue().withS("12345"));
    nestedMap2.put("Title", new AttributeValue().withS("Book 10122 Title"));
    nestedMap2.put("ISBN", new AttributeValue().withS("111-1111111111999"));
    nestedMap2.put("NList",
        new AttributeValue().withL(
            new AttributeValue().withN("12.22"),
            new AttributeValue().withN("-0.00234"),
            new AttributeValue().withNULL(true)));
    nestedMap1.put("NestedMap2",
        new AttributeValue().withM(nestedMap2));
    nestedMap1.put("NSet1",
        new AttributeValue().withNS("123.45", "9586.7778", "-124", "10238"));
    item.put("NestedMap1", new AttributeValue().withM(nestedMap1));
    Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new AttributeValue().withN("101.01"));
    nestedList1Map1.put("Title", new AttributeValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new AttributeValue().withS("111-1111111122"));
    nestedList1Map1.put("InPublication", new AttributeValue().withBOOL(false));
    nestedList1Map1.put("IdSet",
        new AttributeValue().withNS("204850.697038423",
            "-9457860.98693947",
            "20576024",
            "19306873",
            "9067048362749590684"));
    nestedList1Map1.put("TitleSet2",
        new AttributeValue().withSS(
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title"));
    nestedList1Map1.put("BinaryTitleSet", new AttributeValue().withBS(
        ByteBuffer.wrap(Bytes.toBytes("Book 1010 Title Binary")),
        ByteBuffer.wrap(Bytes.toBytes("Book 1011 Title Binary")),
        ByteBuffer.wrap(Bytes.toBytes("Book 1111 Title Binary"))
    ));
    item.put("NestedList1",
        new AttributeValue().withL(new AttributeValue().withN("-485.34"),
            new AttributeValue().withS("1234abcd"),
            new AttributeValue().withL(new AttributeValue().withS("xyz0123"),
                new AttributeValue().withM(nestedList1Map1))));
    item.put("NestedList12",
        new AttributeValue().withL(
            new AttributeValue().withN("-485.34"),
            new AttributeValue().withS("1234abcd"),
            new AttributeValue().withL(
                new AttributeValue().withSS("xyz0123", "xyz01234", "abc01234"),
                new AttributeValue().withBS(
                    ByteBuffer.wrap(Bytes.toBytes("val01")),
                    ByteBuffer.wrap(Bytes.toBytes("val02")),
                    ByteBuffer.wrap(Bytes.toBytes("val03")),
                    ByteBuffer.wrap(Bytes.toBytes("val04"))),
                new AttributeValue().withNS(
                    "-234.56",
                    "123",
                    "93756.93475960549",
                    "9375572.6870899"))));
    return item;
  }

  private static Map<String, AttributeValue> getItem4() {
    Map<String, AttributeValue> item = new HashMap<>();
    item.put("attr_0", new AttributeValue().withS("str_val_0"));
    item.put("AddedId", new AttributeValue().withN("10"));
    item.put("attr_1", new AttributeValue().withN("1295.03"));
    item.put("Id1", new AttributeValue().withS("12345"));
    item.put("attr_5", new AttributeValue().withL(
        new AttributeValue().withN("1224"),
        new AttributeValue().withS("str001"),
        new AttributeValue().withB(ByteBuffer.wrap(
            new byte[] {0, 1, 2, 3, 4, 5}))));
    Map<String, AttributeValue> nMap1 = new HashMap<>();
    nMap1.put("n_attr_0", new AttributeValue().withS("str_val_0"));
    nMap1.put("n_attr_1", new AttributeValue().withN("1223456.03"));
    String bytesAttributeVal1 = "2048u5nblwjeiWFGTH(4bf930";
    byte[] bytesAttrVal1 = bytesAttributeVal1.getBytes();
    nMap1.put("n_attr_2", new AttributeValue().withB(ByteBuffer.wrap(
        bytesAttrVal1)));
    nMap1.put("n_attr_3", new AttributeValue().withBOOL(true));
    nMap1.put("n_attr_4", new AttributeValue().withNULL(true));
    item.put("attr_6", new AttributeValue().withM(nMap1));
    item.put("Id", new AttributeValue().withS("12345"));
    item.put("ColorBytes",
        new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("Black"))));
    item.put("RelatedItems",
        new AttributeValue().withNS("1234",
            "-485.45582904",
            "123.0948",
            "0.111"));
    item.put("Pictures", new AttributeValue().withSS(
        "123_rear.jpg",
        "1235@_rear.jpg",
        "xyz5@_rear.jpg",
        "xyz_rear.jpg",
        "123_front.jpg",
        "xyz_front.jpg"
    ));
    item.put("PictureBinarySet", new AttributeValue().withBS(
        ByteBuffer.wrap(Bytes.toBytes("xyz_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("123_front.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("123abc_rear.jpg")),
        ByteBuffer.wrap(Bytes.toBytes("xyzabc_rear.jpg"))
    ));
    item.put("Title", new AttributeValue().withS("Cycle_1234_new"));
    item.put("ISBN", new AttributeValue().withS("111-1111111111"));
    item.put("InPublication", new AttributeValue().withBOOL(false));
    Map<String, AttributeValue> nestedMap1 = new HashMap<>();
    nestedMap1.put("Id", new AttributeValue().withN("101.01"));
    nestedMap1.put("AddedId", new AttributeValue().withN("10"));
    nestedMap1.put("ISBN", new AttributeValue().withS("111-1111111111"));
    nestedMap1.put("InPublication", new AttributeValue().withBOOL(false));
    nestedMap1.put("NList1", new AttributeValue().withL(
        new AttributeValue().withSS("Updated_set_01", "Updated_set_02"),
        new AttributeValue().withN("-0.00234")));
    nestedMap1.put("ColorList", new AttributeValue().withL(
        new AttributeValue().withS("Black"),
        new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("White"))),
        new AttributeValue().withS("Silver")
    ));
    Map<String, AttributeValue> nestedMap2 = new HashMap<>();
    nestedMap2.put("Id", new AttributeValue().withN("-12243.78"));
    nestedMap2.put("NewID", new AttributeValue().withS("12345"));
    nestedMap2.put("Title", new AttributeValue().withS("Book 10122 Title"));
    nestedMap2.put("ISBN", new AttributeValue().withS("111-1111111111999"));
    nestedMap2.put("NList",
        new AttributeValue().withL(
            new AttributeValue().withN("12.22"),
            new AttributeValue().withN("-0.00234"),
            new AttributeValue().withNULL(true)));
    nestedMap1.put("NestedMap2",
        new AttributeValue().withM(nestedMap2));
    nestedMap1.put("NSet1",
        new AttributeValue().withNS("123.45",
            "9586.7778",
            "-124",
            "10238",
            "-0.29475029304"));
    item.put("NestedMap1", new AttributeValue().withM(nestedMap1));
    Map<String, AttributeValue> nestedList1Map1 = new HashMap<>();
    nestedList1Map1.put("Id", new AttributeValue().withN("101.01"));
    nestedList1Map1.put("Title", new AttributeValue().withS("Book 101 Title"));
    nestedList1Map1.put("ISBN", new AttributeValue().withS("111-1111111122"));
    nestedList1Map1.put("InPublication", new AttributeValue().withBOOL(false));
    nestedList1Map1.put("IdSet",
        new AttributeValue().withNS("2048502.69703847",
            "-45786.4869476",
            "20576024",
            "19306873",
            "83627495940380684"));
    nestedList1Map1.put("TitleSet2",
        new AttributeValue().withSS(
            "Book 1111 Title", "Book 1200 Title", "Book 1201 Title"));
    nestedList1Map1.put("BinaryTitleSet", new AttributeValue().withBS(
        ByteBuffer.wrap(Bytes.toBytes("Book 1010 Title Binary")),
        ByteBuffer.wrap(Bytes.toBytes("Book 1011 Title Binary")),
        ByteBuffer.wrap(Bytes.toBytes("Book 1111 Title Binary"))
    ));
    item.put("NestedList1",
        new AttributeValue().withL(new AttributeValue().withN("-473.11999999999995"),
            new AttributeValue().withS("1234abcd"),
            new AttributeValue().withL(new AttributeValue().withS("xyz0123"),
                new AttributeValue().withM(nestedList1Map1)),
            new AttributeValue().withNULL(true),
            new AttributeValue().withBOOL(true)));
    item.put("NestedList12",
        new AttributeValue().withL(
            new AttributeValue().withN("-485.34"),
            new AttributeValue().withS("1234abcd"),
            new AttributeValue().withL(
                new AttributeValue().withSS("xyz0123", "xyz01234", "abc01234"),
                new AttributeValue().withBS(
                    ByteBuffer.wrap(Bytes.toBytes("val01")),
                    ByteBuffer.wrap(Bytes.toBytes("val02")),
                    ByteBuffer.wrap(Bytes.toBytes("val03")),
                    ByteBuffer.wrap(Bytes.toBytes("val04"))))));
    return item;
  }

}
