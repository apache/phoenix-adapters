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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.bson.BsonDocument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class PutItemIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(PutItemIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
    }

    @AfterClass
    public static void stopLocalDynamoDb() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().stop();
        if (restServer != null) {
            restServer.stop();
        }
        ServerUtil.ConnectionFactory.shutdown();
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            if (utility != null) {
                utility.shutdownMiniCluster();
            }
            ServerMetadataCacheTestImpl.resetCache();
        }
        System.setProperty("java.io.tmpdir", tmpDir);
    }

    @Test(timeout = 120000)
    public void putItemHashKeyTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        //query from dynamo
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("attr_0 = :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", AttributeValue.builder().s("str_val_0").build());
        qr.expressionAttributeValues(exprAttrVal);
        QueryResponse result = dynamoDbClient.query(qr.build());
        Assert.assertEquals(1, result.items().size());
        Map<String, AttributeValue> dynamoItem = result.items().get(0);

        // query phoenix and compare row to item
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT * FROM " + PhoenixUtils.getFullTableName(tableName, true));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), item.get("attr_0").s());
            BsonDocument bsonDoc = (BsonDocument) rs.getObject(2);
            Map<String, AttributeValue> phoenixItem =
                    BsonDocumentToDdbAttributes.getFullItem(bsonDoc);
            Assert.assertEquals(item, phoenixItem);

            //TODO: uncomment when we have utility to compare sets,
            // dynamo represents sets as Lists and this assert will
            // fail because of order of elements
            //Assert.assertEquals(dynamoItem, phoenixItem);
        }
    }

    @Test(timeout = 120000)
    public void putItemHashRangeKeyTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        //query from dynamo
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("attr_0 = :val1 AND attr_1 = :val2");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val1", AttributeValue.builder().s("str_val_0").build());
        exprAttrVal.put(":val2", AttributeValue.builder().n("1295.03").build());
        qr.expressionAttributeValues(exprAttrVal);
        QueryResponse result = dynamoDbClient.query(qr.build());
        Assert.assertEquals(1, result.items().size());
        Map<String, AttributeValue> dynamoItem = result.items().get(0);

        // query phoenix and compare row to item
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT * FROM " + PhoenixUtils.getFullTableName(tableName, true));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), item.get("attr_0").s());
            Assert.assertEquals(rs.getDouble(2), Double.parseDouble(item.get("attr_1").n()), 0.0);
            BsonDocument bsonDoc = (BsonDocument) rs.getObject(3);
            Map<String, AttributeValue> phoenixItem =
                    BsonDocumentToDdbAttributes.getFullItem(bsonDoc);
            Assert.assertEquals(item, phoenixItem);

            //TODO: uncomment when we have utility to compare sets
            //Assert.assertEquals(dynamoItem, item2);
        }
    }

    @Test(timeout = 120000)
    public void putItemWithGlobalIndexWithHashKeyTest() throws Exception {
        // create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        // add index on Title
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, "g_IDX_" + tableName,
                        "Title", ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        //query from dynamo
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("attr_0 = :val1 AND attr_1 = :val2");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val1", AttributeValue.builder().s("str_val_0").build());
        exprAttrVal.put(":val2", AttributeValue.builder().n("1295.03").build());
        qr.expressionAttributeValues(exprAttrVal);
        QueryResponse result = dynamoDbClient.query(qr.build());
        Assert.assertEquals(1, result.items().size());
        Map<String, AttributeValue> dynamoItem = result.items().get(0);

        // query phoenix and compare row to item
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT * FROM " + PhoenixUtils.getFullTableName(tableName, true));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), item.get("attr_0").s());
            Assert.assertEquals(rs.getDouble(2), Double.parseDouble(item.get("attr_1").n()), 0.0);
            BsonDocument bsonDoc = (BsonDocument) rs.getObject(3);
            Map<String, AttributeValue> phoenixItem =
                    BsonDocumentToDdbAttributes.getFullItem(bsonDoc);
            Assert.assertEquals(item, phoenixItem);

            //TODO: uncomment when we have utility to compare sets
            //Assert.assertEquals(dynamoItem, phoenixItem);

            // check index row (Title, attr_0, attr1, COL)
            rs = connection.createStatement().executeQuery(
                    "SELECT * FROM " + PhoenixUtils.getFullTableName(
                            tableName + "_g_IDX_" + tableName, true));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getString(1), item.get("Title").s());
            Assert.assertEquals(rs.getString(2), item.get("attr_0").s());
            Assert.assertEquals(rs.getDouble(3), Double.parseDouble(item.get("attr_1").n()), 0.0);
            bsonDoc = (BsonDocument) rs.getObject(4);
            Map<String, AttributeValue> indexItem =
                    BsonDocumentToDdbAttributes.getFullItem(bsonDoc);
            Assert.assertEquals(item, indexItem);

            //TODO: uncomment when we have utility to compare sets
            //Assert.assertEquals(dynamoItem, indexItem);
        }
    }

    @Test(timeout = 120000)
    public void putItemIndexSortedTest() throws Exception {
        // create table [attr_0, idx_attr]
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        // add index on Title
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_iDX_" + tableName,
                        "idx_attr", ScalarAttributeType.N, null, null);
        phoenixDBClientV2.createTable(createTableRequest);

        // put some items
        phoenixDBClientV2.putItem(getPutItemRequestForIndexSortingTest(tableName, "val1", "123"));
        phoenixDBClientV2.putItem(
                getPutItemRequestForIndexSortingTest(tableName, "val2", "123.0001"));
        phoenixDBClientV2.putItem(
                getPutItemRequestForIndexSortingTest(tableName, "val3", "-123.01"));
        phoenixDBClientV2.putItem(getPutItemRequestForIndexSortingTest(tableName, "val4", "-123"));
        phoenixDBClientV2.putItem(
                getPutItemRequestForIndexSortingTest(tableName, "val5", "122.999"));
        phoenixDBClientV2.putItem(
                getPutItemRequestForIndexSortingTest(tableName, "val6", "-122.9999"));
        phoenixDBClientV2.putItem(
                getPutItemRequestForIndexSortingTest(tableName, "val7", "122.9999"));
        phoenixDBClientV2.putItem(getPutItemRequestForIndexSortingTest(tableName, "val8", "0"));
        phoenixDBClientV2.putItem(getPutItemRequestForIndexSortingTest(tableName, "val9", "0.123"));

        // check index rows are sorted
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery(
                    "SELECT * FROM " + PhoenixUtils.getFullTableName(
                            tableName + "_G_iDX_" + tableName, true));
            Assert.assertTrue(rs.next());
            Double val = rs.getDouble(1);
            while (rs.next()) {
                Assert.assertTrue(rs.getDouble(1) > val);
                val = rs.getDouble(1);
            }
        }
    }

    private PutItemRequest getPutItemRequestForIndexSortingTest(String tableName, String k,
            String v) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s(k).build());
        item.put("idx_attr", AttributeValue.builder().n(v).build());
        return PutItemRequest.builder().tableName(tableName).item(item).build();
    }

    @Test(timeout = 120000)
    public void putItemWithConditionCheckFailureTest1() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_not_exists(attr_0)")
                .returnValuesOnConditionCheckFailure("ALL_OLD")
                .build();

        dynamoDbClient.putItem(putItemRequest);
        phoenixDBClientV2.putItem(putItemRequest);

        Map<String, AttributeValue> conditionCheckFailedItem = null;
        try {
            dynamoDbClient.putItem(putItemRequest);
            Assert.fail("Expected ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            conditionCheckFailedItem = e.item();
        }

        try {
            phoenixDBClientV2.putItem(putItemRequest);
            Assert.fail("Expected ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertTrue(
                    ItemComparator.areItemsEqual(conditionCheckFailedItem, e.item()));
        }

    }

    @Test(timeout = 120000)
    public void testPutItemReturnValuesValidation() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
            DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> simpleItem = new HashMap<>();
        simpleItem.put("attr_0", AttributeValue.builder().s("test_value").build());
        simpleItem.put("simple_attr", AttributeValue.builder().s("simple_value").build());

        // Test NONE - should succeed
        PutItemRequest putItemRequest =
            PutItemRequest.builder().tableName(tableName).item(simpleItem)
                .returnValues(ReturnValue.NONE).build();
        PutItemResponse dynamoResult1 = dynamoDbClient.putItem(putItemRequest);
        PutItemResponse phoenixResult1 = phoenixDBClientV2.putItem(putItemRequest);
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Test ALL_OLD - should succeed (put the same item again to test returning old values)
        putItemRequest = PutItemRequest.builder().tableName(tableName).item(simpleItem)
            .returnValues(ReturnValue.ALL_OLD).build();
        PutItemResponse dynamoResult2 = dynamoDbClient.putItem(putItemRequest);
        PutItemResponse phoenixResult2 = phoenixDBClientV2.putItem(putItemRequest);
        // Both should return the old item and should match exactly for simple data types
        Assert.assertEquals("Both clients should return the same old item",
            dynamoResult2.attributes(), phoenixResult2.attributes());

        // Test ALL_NEW - should fail with same error status code in both clients
        putItemRequest = PutItemRequest.builder().tableName(tableName).item(simpleItem)
            .returnValues(ReturnValue.ALL_NEW).build();
        int dynamoStatusCode = -1;
        int phoenixStatusCode = -1;
        try {
            dynamoDbClient.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for ALL_NEW");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for ALL_NEW");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage()
                .contains("ReturnValues value 'ALL_NEW' is not valid for PUT_ITEM operation"));
        }
        Assert.assertEquals("Status codes should match for ALL_NEW validation error",
            dynamoStatusCode, phoenixStatusCode);

        // Test UPDATED_OLD - should fail with same error status code in both clients
        putItemRequest = PutItemRequest.builder().tableName(tableName).item(simpleItem)
            .returnValues(ReturnValue.UPDATED_OLD).build();
        dynamoStatusCode = -1;
        phoenixStatusCode = -1;
        try {
            dynamoDbClient.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for UPDATED_OLD");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for UPDATED_OLD");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage()
                .contains("UPDATED_OLD or UPDATED_NEW is not supported for ReturnValue"));
        }
        Assert.assertEquals("Status codes should match for UPDATED_OLD validation error",
            dynamoStatusCode, phoenixStatusCode);

        // Test UPDATED_NEW - should fail with same error status code in both clients
        putItemRequest = PutItemRequest.builder().tableName(tableName).item(simpleItem)
            .returnValues(ReturnValue.UPDATED_NEW).build();
        dynamoStatusCode = -1;
        phoenixStatusCode = -1;
        try {
            dynamoDbClient.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for UPDATED_NEW");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for UPDATED_NEW");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage()
                .contains("UPDATED_OLD or UPDATED_NEW is not supported for ReturnValue"));
        }
        Assert.assertEquals("Status codes should match for UPDATED_NEW validation error",
            dynamoStatusCode, phoenixStatusCode);

        // Test invalid value - should fail with same error status code in both clients
        putItemRequest = PutItemRequest.builder().tableName(tableName).item(simpleItem)
            .returnValues("INVALID_VALUE").build();
        dynamoStatusCode = -1;
        phoenixStatusCode = -1;
        try {
            dynamoDbClient.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage().contains(
                "ReturnValues value 'INVALID_VALUE' is not valid for PUT_ITEM operation"));
        }
        Assert.assertEquals("Status codes should match for INVALID_VALUE validation error",
            dynamoStatusCode, phoenixStatusCode);
    }

    @Test(timeout = 120000)
    public void testPutItemReturnValuesOnConditionCheckFailureValidation() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
            DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();

        // Test NONE - should succeed (validation passes)
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item)
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.NONE).build();
        PutItemResponse dynamoResult1 = dynamoDbClient.putItem(putItemRequest);
        PutItemResponse phoenixResult1 = phoenixDBClientV2.putItem(putItemRequest);
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Test ALL_OLD - should succeed (validation passes)
        putItemRequest = PutItemRequest.builder().tableName(tableName).item(item)
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build();
        PutItemResponse dynamoResult2 = dynamoDbClient.putItem(putItemRequest);
        PutItemResponse phoenixResult2 = phoenixDBClientV2.putItem(putItemRequest);
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        // Test invalid value - should fail with same error status code in both clients
        putItemRequest = PutItemRequest.builder().tableName(tableName).item(item)
            .returnValuesOnConditionCheckFailure("INVALID_VALUE").build();
        int dynamoStatusCode = -1;
        int phoenixStatusCode = -1;
        try {
            dynamoDbClient.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.putItem(putItemRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage().contains(
                "ReturnValuesOnConditionCheckFailure value 'INVALID_VALUE' is not valid"));
        }
        Assert.assertEquals("Status codes should match for INVALID_VALUE validation error",
            dynamoStatusCode, phoenixStatusCode);
    }

    @Test(timeout = 120000)
    public void putItemWithConditionCheckFailureTest2() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_not_exists(attr_0)")
                .returnValuesOnConditionCheckFailure("NONE")
                .build();

        dynamoDbClient.putItem(putItemRequest);
        phoenixDBClientV2.putItem(putItemRequest);

        Map<String, AttributeValue> conditionCheckFailedItem = null;
        try {
            dynamoDbClient.putItem(putItemRequest);
            Assert.fail("Expected ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            conditionCheckFailedItem = e.item();
        }

        try {
            phoenixDBClientV2.putItem(putItemRequest);
            Assert.fail("Expected ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertTrue(
                    ItemComparator.areItemsEqual(conditionCheckFailedItem, e.item()));
        }
    }
}
