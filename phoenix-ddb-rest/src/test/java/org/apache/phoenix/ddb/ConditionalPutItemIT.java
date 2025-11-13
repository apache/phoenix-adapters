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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import org.bson.BsonDocument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ConditionalPutItemIT {
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
    public void simpleConditionalPutFailureTest() throws Exception {
        // create table [attr_0, attr_1]
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);

        // add index on attr_1
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName,
                        "attr_1", ScalarAttributeType.N, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // put (val1, 123)
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("val1").build());
        item1.put("attr_1", AttributeValue.builder().n("123").build());
        PutItemRequest item1PutRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(item1PutRequest);
        dynamoDbClient.putItem(item1PutRequest);

        // conditional put (val1, 999) if attr_1 > 200
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("val1").build());
        item2.put("attr_1", AttributeValue.builder().n("999").build());
        // request
        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        // expression
        condPutRequest.conditionExpression("#1 > :0");
        // expression names
        Map<String, String> exprAttrNames = new HashMap();
        exprAttrNames.put("#1", "attr_1");
        condPutRequest.expressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":0", AttributeValue.builder().n("200").build());
        condPutRequest.expressionAttributeValues(exprAttrVals);

        Map<String, AttributeValue> dynamoExceptionItem = null;
        try {
            dynamoDbClient.putItem(condPutRequest.build());
        } catch (ConditionalCheckFailedException e) {
            dynamoExceptionItem = e.item();
        }
        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
            Assert.fail(
                    "PutItem should throw exception when condition check fails and return value is not set.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(dynamoExceptionItem, e.item());
        }

        //query from dynamo
        verifyResult1(tableName, item1);
    }

    @Test(timeout = 120000)
    public void simpleConditionalPutFailureTestWithItem() throws Exception {
        // create table [attr_0, attr_1]
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);

        // add index on attr_1
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName,
                        "attr_1", ScalarAttributeType.N, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // put (val1, 123)
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("val1").build());
        item1.put("attr_1", AttributeValue.builder().n("123").build());
        PutItemRequest item1PutRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(item1PutRequest);
        dynamoDbClient.putItem(item1PutRequest);

        // conditional put (val1, 999) if attr_1 > 200
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("val1").build());
        item2.put("attr_1", AttributeValue.builder().n("999").build());
        // request
        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        // expression
        condPutRequest.conditionExpression("#1 > :0");
        // expression names
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr_1");
        condPutRequest.expressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", AttributeValue.builder().n("200").build());
        condPutRequest.expressionAttributeValues(exprAttrVals);
        condPutRequest.returnValuesOnConditionCheckFailure(
                ReturnValuesOnConditionCheckFailure.ALL_OLD);

        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(item1, e.item());
        }
        try {
            dynamoDbClient.putItem(condPutRequest.build());
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(item1, e.item());
        }

        verifyResult1(tableName, item1);
    }

    private void verifyResult1(String tableName, Map<String, AttributeValue> item1)
            throws SQLException {
        //query from dynamo
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("attr_0 = :v");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v", AttributeValue.builder().s("val1").build());
        qr.expressionAttributeValues(exprAttrVal);
        QueryResponse result = dynamoDbClient.query(qr.build());
        Assert.assertEquals(1, result.items().size());
        Map<String, AttributeValue> dynamoItem = result.items().get(0);

        // check phoenix row, there should be no update
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery(
                    "SELECT * FROM " + PhoenixUtils.getFullTableName(tableName, true) + " WHERE \"attr_0\" = 'val1'");
            Assert.assertTrue(rs.next());
            BsonDocument rowBsonDoc = (BsonDocument) rs.getObject(2);
            Assert.assertEquals(rowBsonDoc.get("attr_1").asInt32().getValue(), 123);
            Map<String, AttributeValue> phoenixItem =
                    BsonDocumentToDdbAttributes.getFullItem(rowBsonDoc);
            Assert.assertEquals(phoenixItem, dynamoItem);

            //check no update to index row [123, val1, (val1, 123)]
            rs = connection.createStatement().executeQuery(
                    "SELECT * FROM " + PhoenixUtils.getFullTableName(
                            tableName + "_G_IDX_" + tableName, true));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getLong(1), Long.parseLong(item1.get("attr_1").n()));
            Assert.assertEquals(rs.getString(2), item1.get("attr_0").s());
        }
    }

    @Test(timeout = 120000)
    public void simpleConditionalPutSuccessTest() throws Exception {
        // create table [attr_0, idx_attr]
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // put (val1, 123)
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("val1").build());
        item1.put("attr_1", AttributeValue.builder().n("123").build());
        PutItemRequest item1PutRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(item1PutRequest);
        dynamoDbClient.putItem(item1PutRequest);

        // conditional put (val1, 999) if attr_1 != 200
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("val1").build());
        item2.put("attr_1", AttributeValue.builder().n("999").build());
        // request
        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        // expression
        condPutRequest.conditionExpression("#1 <> :0");
        // expression names
        Map<String, String> exprAttrNames = new HashMap();
        exprAttrNames.put("#1", "attr_1");
        condPutRequest.expressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":0", AttributeValue.builder().n("200").build());
        condPutRequest.expressionAttributeValues(exprAttrVals);

        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
        } catch (ConditionalCheckFailedException e) {
            Assert.fail("PutItem should not throw exception when condition check passes.");
        }
        dynamoDbClient.putItem(condPutRequest.build());

        //query from dynamo
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("attr_0 = :v");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v", AttributeValue.builder().s("val1").build());
        qr.expressionAttributeValues(exprAttrVal);
        QueryResponse result = dynamoDbClient.query(qr.build());
        Assert.assertEquals(1, result.items().size());
        Map<String, AttributeValue> dynamoItem = result.items().get(0);

        // check phoenix row, there should be update to the row
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery(
                    "SELECT * FROM " + PhoenixUtils.getFullTableName(tableName, true) + " WHERE \"attr_0\" = 'val1'");
            Assert.assertTrue(rs.next());
            BsonDocument rowBsonDoc = (BsonDocument) rs.getObject(2);
            Assert.assertEquals(rowBsonDoc.get("attr_1").asInt32().getValue(), 999);
            Map<String, AttributeValue> phoenixItem =
                    BsonDocumentToDdbAttributes.getFullItem(rowBsonDoc);
            Assert.assertEquals(phoenixItem, dynamoItem);
        }
    }

    @Test(timeout = 120000)
    public void compositeConditionalPutFailureTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item [str_val_0, item]
        Map<String, AttributeValue> item1 = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        //conditional put
        Map<String, AttributeValue> item2 = DocumentDdbAttributesTest.getItem2();
        // request
        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        // expression
        condPutRequest.conditionExpression(
                "attribute_not_exists(#6) AND attribute_not_exists(#7) AND "
                        + "attribute_not_exists(#8) AND #0 = :0 AND #1 = :1 AND #2 = :2 "
                        + "AND #3 = :3 AND attribute_not_exists(#4) AND attribute_not_exists(#5)");
        // expression names
        Map<String, String> exprAttrNames = new HashMap();
        exprAttrNames.put("#0", "attr_1");
        exprAttrNames.put("#1", "ColorBytes");
        exprAttrNames.put("#2", "Title");
        exprAttrNames.put("#3", "InPublication");
        exprAttrNames.put("#4", "NestedMap2");
        exprAttrNames.put("#5", "ISBN");
        exprAttrNames.put("#6", "[");
        exprAttrNames.put("#7", "-");
        exprAttrNames.put("#8", ">");
        condPutRequest.expressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":0", AttributeValue.builder().n("1295.03").build());
        exprAttrVals.put(":1",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
        exprAttrVals.put(":2", AttributeValue.builder().s("Book 101 Title ").build());
        exprAttrVals.put(":3", AttributeValue.builder().bool(false).build());
        condPutRequest.expressionAttributeValues(exprAttrVals);

        Map<String, AttributeValue> dynamoExceptionItem = null;
        try {
            dynamoDbClient.putItem(condPutRequest.build());
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoExceptionItem = e.item();
        }
        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(dynamoExceptionItem, e.item());
        }

        //query from dynamo
        verifyResult2(tableName, AttributeValue.builder().s("str_val_0").build(), 2, item1);
    }

    @Test(timeout = 120000)
    public void compositeConditionalPutFailureTestWithSortKey() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.B,
                        "pk2", ScalarAttributeType.B);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item [str_val_0, item]
        Map<String, AttributeValue> item1 = DocumentDdbAttributesTest.getItemWithBinary1();
        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        //conditional put
        Map<String, AttributeValue> item2 = DocumentDdbAttributesTest.getItemWithBinary2();
        // request
        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        // expression
        condPutRequest.conditionExpression("#0 = :0 AND #1 = :1 AND #2 = :2 AND #3 = :3 AND "
                + "attribute_not_exists(#4) AND attribute_not_exists(#5)");
        // expression names
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_1");
        exprAttrNames.put("#1", "ColorBytes");
        exprAttrNames.put("#2", "Title");
        exprAttrNames.put("#3", "InPublication");
        exprAttrNames.put("#4", "NestedMap2");
        exprAttrNames.put("#5", "ISBN");
        condPutRequest.expressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", AttributeValue.builder().n("1295.03").build());
        exprAttrVals.put(":1",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
        exprAttrVals.put(":2", AttributeValue.builder().s("Book 101 Title ").build());
        exprAttrVals.put(":3", AttributeValue.builder().bool(false).build());
        condPutRequest.expressionAttributeValues(exprAttrVals);

        Map<String, AttributeValue> dynamoExceptionItem = null;
        try {
            dynamoDbClient.putItem(condPutRequest.build());
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoExceptionItem = e.item();
        }
        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(dynamoExceptionItem, e.item());
        }

        verifyResult2(tableName,
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_0")))
                        .build(), 3, item1);
    }

    private void verifyResult2(String tableName, AttributeValue str_val_0, int columnIndex,
            Map<String, AttributeValue> item1) throws SQLException {
        //query from dynamo
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("attr_0 = :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", str_val_0);
        qr.expressionAttributeValues(exprAttrVal);
        QueryResponse result = dynamoDbClient.query(qr.build());
        Assert.assertEquals(1, result.items().size());
        Map<String, AttributeValue> dynamoItem = result.items().get(0);

        // check phoenix row, there should be no update to the row
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery(
                    "SELECT * FROM " + PhoenixUtils.getFullTableName(tableName, true) + " WHERE \"attr_0\" = 'str_val_0'");
            Assert.assertTrue(rs.next());
            Map<String, AttributeValue> phoenixItem = BsonDocumentToDdbAttributes.getFullItem(
                    (BsonDocument) rs.getObject(columnIndex));
            Assert.assertEquals(item1, phoenixItem);

            //TODO: uncomment when we have utility to compare sets
            //Assert.assertEquals(dynamoItem, phoenixItem);
        }
    }

    @Test(timeout = 120000)
    public void compositeConditionalPutFailureTestWithSortKeyAndReturnRow() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.B,
                        "pk2", ScalarAttributeType.B);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item [str_val_0, item]
        Map<String, AttributeValue> item1 = DocumentDdbAttributesTest.getItemWithBinary1();
        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        //conditional put
        Map<String, AttributeValue> item2 = DocumentDdbAttributesTest.getItemWithBinary2();
        // request
        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        // expression
        condPutRequest.conditionExpression("#0 = :0 AND #1 = :1 AND #2 = :2 AND #3 = :3 AND "
                + "attribute_not_exists(#4) AND attribute_not_exists(#5)");
        // expression names
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_1");
        exprAttrNames.put("#1", "ColorBytes");
        exprAttrNames.put("#2", "Title");
        exprAttrNames.put("#3", "InPublication");
        exprAttrNames.put("#4", "NestedMap2");
        exprAttrNames.put("#5", "ISBN");
        condPutRequest.expressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", AttributeValue.builder().n("1295.03").build());
        exprAttrVals.put(":1",
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("Black"))).build());
        exprAttrVals.put(":2", AttributeValue.builder().s("Book 101 Title ").build());
        exprAttrVals.put(":3", AttributeValue.builder().bool(false).build());
        condPutRequest.expressionAttributeValues(exprAttrVals);
        condPutRequest.returnValuesOnConditionCheckFailure(
                ReturnValuesOnConditionCheckFailure.ALL_OLD);

        try {
            dynamoDbClient.putItem(condPutRequest.build());
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            // TODO: uncomment when we have utility to compare sets
            // Assert.assertEquals(item1, e.item());
        }
        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(item1, e.item());
        }

        //query from dynamo
        verifyResult2(tableName,
                AttributeValue.builder().b(SdkBytes.fromByteArray(Bytes.toBytes("str_val_0")))
                        .build(), 3, item1);
    }

    @Test(timeout = 120000)
    public void compositeConditionalPutSuccessTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        // add index on Id2
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName,
                        "Id2", ScalarAttributeType.N, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item [str_val_0, item]
        Map<String, AttributeValue> item1 = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        //conditional put
        Map<String, AttributeValue> item2 = DocumentDdbAttributesTest.getItem2();
        // request
        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        // expression
        condPutRequest.conditionExpression("(#0 = :0) AND "
                + "(((attribute_not_exists(#1) AND attribute_not_exists(#2)) OR attribute_exists(#3)) "
                + "OR (#4 = :1))");
        // expression names
        Map<String, String> exprAttrNames = new HashMap();
        exprAttrNames.put("#0", "attr_1");
        exprAttrNames.put("#1", "attr_5");
        exprAttrNames.put("#2", "attr_6");
        exprAttrNames.put("#3", "not_found");
        exprAttrNames.put("#4", "Id2");
        condPutRequest.expressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":0", AttributeValue.builder().n("1295.03").build());
        exprAttrVals.put(":1", AttributeValue.builder().n("101.01").build());
        condPutRequest.expressionAttributeValues(exprAttrVals);

        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
        } catch (ConditionalCheckFailedException e) {
            Assert.fail("PutItem should not throw exception when condition check passes.");
        }
        dynamoDbClient.putItem(condPutRequest.build());

        //query from dynamo
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("attr_0 = :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", AttributeValue.builder().s("str_val_0").build());
        qr.expressionAttributeValues(exprAttrVal);
        QueryResponse result = dynamoDbClient.query(qr.build());
        Assert.assertEquals(1, result.items().size());
        Map<String, AttributeValue> dynamoItem = result.items().get(0);

        // check phoenix row, there should be update to the row
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery(
                    "SELECT * FROM " + PhoenixUtils.getFullTableName(tableName, true) + " WHERE \"attr_0\" = 'str_val_0'");
            Assert.assertTrue(rs.next());
            Map<String, AttributeValue> phoenixItem =
                    BsonDocumentToDdbAttributes.getFullItem((BsonDocument) rs.getObject(2));
            Assert.assertEquals(phoenixItem, item2);

            // check index row is updated (Id2, attr_0, COL)
            rs = connection.createStatement().executeQuery(
                    "SELECT * FROM " + PhoenixUtils.getFullTableName(
                            tableName + "_G_IDX_" + tableName, true));
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getDouble(1), Double.parseDouble(item2.get("Id2").n()), 0.0);
            Assert.assertEquals(rs.getString(2), item2.get("attr_0").s());

            //TODO: uncomment when we have utility to compare sets
            //Assert.assertEquals(dynamoItem, phoenixItem);
            //Assert.assertEquals(dynamoItem, indexRowItem);
        }
    }

    @Test
    public void conditionExpressionOnPKTest() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hk", ScalarAttributeType.S, "sk", ScalarAttributeType.S);

        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        /**
         * item does not exist, using attribute_not_exists(hk) AND attribute_not_exists(sk)
         * should insert the item.
         */
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("hk", AttributeValue.builder().s("str0").build());
        item.put("sk", AttributeValue.builder().s("str1").build());
        PutItemRequest pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_not_exists(hk) AND attribute_not_exists(sk)").build();
        phoenixDBClientV2.putItem(pir);
        dynamoDbClient.putItem(pir);

        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse ddbResponse = dynamoDbClient.scan(scanRequest);
        ScanResponse phoenixResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(ddbResponse.items(), phoenixResponse.items());
        Assert.assertEquals(ddbResponse.items().size(), phoenixResponse.items().size());

        /**
         * item exists, same PutItemRequest should throw ConditionalCheckFailedException.
         */
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}

        /**
         * item exists, using only attribute_not_exists(hk) or only attribute_not_exists(sk)
         * should throw ConditionalCheckFailedException.
         */
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_not_exists(hk)").build();
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}

        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_not_exists(sk)").build();
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}

        /**
         * item does not exist, using attribute_not_exists(hk) or attribute_not_exists(sk)
         * should insert the item.
         */
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("hk", AttributeValue.builder().s("str0").build());
        item2.put("sk", AttributeValue.builder().s("str11").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item2)
                .conditionExpression("attribute_not_exists(hk)").build();
        phoenixDBClientV2.putItem(pir);
        dynamoDbClient.putItem(pir);
        ddbResponse = dynamoDbClient.scan(scanRequest);
        phoenixResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(ddbResponse.items().size(), phoenixResponse.items().size());
        for (Map<String, AttributeValue> ddbItem : ddbResponse.items()) {
            Assert.assertTrue(phoenixResponse.items().contains(ddbItem));
        }

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("hk", AttributeValue.builder().s("str00").build());
        item3.put("sk", AttributeValue.builder().s("str11").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item3)
                .conditionExpression("attribute_not_exists(sk)").build();
        phoenixDBClientV2.putItem(pir);
        dynamoDbClient.putItem(pir);
        ddbResponse = dynamoDbClient.scan(scanRequest);
        phoenixResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(ddbResponse.items().size(), phoenixResponse.items().size());
        for (Map<String, AttributeValue> ddbItem : ddbResponse.items()) {
            Assert.assertTrue(phoenixResponse.items().contains(ddbItem));
        }

        /**
         * item exists, using attribute_exists(hk) and/or attribute_exists(sk)
         * should override the item
         */
        item.put("newCol", AttributeValue.builder().s("foo").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .conditionExpression("attribute_exists(hk) AND attribute_exists(sk)").build();
        phoenixDBClientV2.putItem(pir);
        dynamoDbClient.putItem(pir);
        ddbResponse = dynamoDbClient.scan(scanRequest);
        phoenixResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(ddbResponse.items().size(), phoenixResponse.items().size());
        for (Map<String, AttributeValue> ddbItem : ddbResponse.items()) {
            Assert.assertTrue(phoenixResponse.items().contains(ddbItem));
        }
        Map<String, AttributeValue> itemKey= new HashMap<>();
        itemKey.put("hk", AttributeValue.builder().s("str0").build());
        itemKey.put("sk", AttributeValue.builder().s("str1").build());
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(itemKey).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());

        item3.put("newCol", AttributeValue.builder().s("foo").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item3)
                .conditionExpression("attribute_exists(hk)").build();
        phoenixDBClientV2.putItem(pir);
        dynamoDbClient.putItem(pir);
        ddbResponse = dynamoDbClient.scan(scanRequest);
        phoenixResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(ddbResponse.items().size(), phoenixResponse.items().size());
        for (Map<String, AttributeValue> ddbItem : ddbResponse.items()) {
            Assert.assertTrue(phoenixResponse.items().contains(ddbItem));
        }
        Map<String, AttributeValue> item3Key= new HashMap<>();
        item3Key.put("hk", AttributeValue.builder().s("str00").build());
        item3Key.put("sk", AttributeValue.builder().s("str11").build());
        gir = GetItemRequest.builder().tableName(tableName).key(item3Key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());

        item2.put("newCol", AttributeValue.builder().s("foo").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item2)
                .conditionExpression("attribute_exists(sk)").build();
        phoenixDBClientV2.putItem(pir);
        dynamoDbClient.putItem(pir);
        ddbResponse = dynamoDbClient.scan(scanRequest);
        phoenixResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(ddbResponse.items().size(), phoenixResponse.items().size());
        for (Map<String, AttributeValue> ddbItem : ddbResponse.items()) {
            Assert.assertTrue(phoenixResponse.items().contains(ddbItem));
        }
        Map<String, AttributeValue> item2Key= new HashMap<>();
        item2Key.put("hk", AttributeValue.builder().s("str0").build());
        item2Key.put("sk", AttributeValue.builder().s("str11").build());
        gir = GetItemRequest.builder().tableName(tableName).key(item2Key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());

        /**
         * item does not exist, using attribute_exists(hk) and/or attribute_exists(sk)
         * should throw ConditionalCheckFailedException
         */
        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put("hk", AttributeValue.builder().s("str00").build());
        item4.put("sk", AttributeValue.builder().s("str12").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item4)
                .conditionExpression("attribute_exists(hk) AND attribute_exists(sk)").build();
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item4)
                .conditionExpression("attribute_exists(hk)").build();
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item4)
                .conditionExpression("attribute_exists(sk)").build();
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
    }

    @Test(timeout = 120000)
    public void conditionalPutWithReturnValuesAllOldConditionTrueTest() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("val1").build());
        item1.put("attr_1", AttributeValue.builder().n("123").build());
        PutItemRequest item1PutRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(item1PutRequest);
        dynamoDbClient.putItem(item1PutRequest);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("val1").build());
        item2.put("attr_1", AttributeValue.builder().n("999").build());

        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        condPutRequest.conditionExpression("#1 = :0");
        condPutRequest.returnValues(ReturnValue.ALL_OLD);
        condPutRequest.returnValuesOnConditionCheckFailure(
                ReturnValuesOnConditionCheckFailure.ALL_OLD);

        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr_1");
        condPutRequest.expressionAttributeNames(exprAttrNames);

        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", AttributeValue.builder().n("123").build());
        condPutRequest.expressionAttributeValues(exprAttrVals);

        PutItemResponse dynamoResponse = dynamoDbClient.putItem(condPutRequest.build());
        PutItemResponse phoenixResponse = phoenixDBClientV2.putItem(condPutRequest.build());

        Assert.assertEquals("Returned items should match between DynamoDB and Phoenix",
                dynamoResponse.attributes(), phoenixResponse.attributes());
        Assert.assertEquals("Returned item should be the original item", item1,
                dynamoResponse.attributes());

        verifyItemUpdated(tableName, "val1", "999");

        condPutRequest = PutItemRequest.builder().tableName(tableName).item(item2);
        condPutRequest.conditionExpression("#1 = :0");
        condPutRequest.returnValues(ReturnValue.ALL_OLD);
        condPutRequest.returnValuesOnConditionCheckFailure(
                ReturnValuesOnConditionCheckFailure.ALL_OLD);

        exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr_1");
        condPutRequest.expressionAttributeNames(exprAttrNames);

        exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", AttributeValue.builder().n("200").build());
        condPutRequest.expressionAttributeValues(exprAttrVals);

        Map<String, AttributeValue> dynamoExceptionItem = null;
        try {
            dynamoDbClient.putItem(condPutRequest.build());
            Assert.fail("DynamoDB PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoExceptionItem = e.item();
        }

        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
            Assert.fail("Phoenix PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals("Exception items should match between DynamoDB and Phoenix",
                    dynamoExceptionItem, e.item());
        }

        verifyItemNotUpdated(tableName, "val1", "999");
    }

    @Test(timeout = 120000)
    public void conditionalPutWithReturnValuesAllOldNewItemTest() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("val1").build());
        item1.put("attr_1", AttributeValue.builder().n("999").build());

        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item1);
        condPutRequest.conditionExpression("attribute_not_exists(attr_0)");
        condPutRequest.returnValues(ReturnValue.ALL_OLD);
        condPutRequest.returnValuesOnConditionCheckFailure(
                ReturnValuesOnConditionCheckFailure.ALL_OLD);

        PutItemResponse dynamoResponse = dynamoDbClient.putItem(condPutRequest.build());
        PutItemResponse phoenixResponse = phoenixDBClientV2.putItem(condPutRequest.build());

        Assert.assertEquals("Returned items should match between DynamoDB and Phoenix",
                dynamoResponse.attributes(), phoenixResponse.attributes());
        Assert.assertTrue("Returned item should be empty for new item",
                dynamoResponse.attributes().isEmpty());

        verifyItemExists(tableName, "val1", "999");
    }

    @Test(timeout = 120000)
    public void conditionalPutWhereItemDoesNotExistTest() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hk", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("hk", AttributeValue.builder().s("val1").build());
        item1.put("payload", AttributeValue.builder().n("999").build());

        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item1);
        condPutRequest.conditionExpression("attribute_exists(hk) AND attribute_not_exists(payload)");

        try {
            dynamoDbClient.putItem(condPutRequest.build());
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException" +
                    " when item does not exist and condition expression has attribute_exists(key).");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException " +
                    " when item does not exist and condition expression has attribute_exists(key).");
        } catch (ConditionalCheckFailedException e) {}

        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":p", AttributeValue.builder().n("999").build());
        condPutRequest.conditionExpression("payload = :p");
        condPutRequest.expressionAttributeValues(exprAttrValues);
        try {
            dynamoDbClient.putItem(condPutRequest.build());
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException" +
                    " when item does not exist and condition expression has equality check for non key attr.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(condPutRequest.build());
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException" +
                    " when item does not exist and condition expression has equality check for non key attr.");
        } catch (ConditionalCheckFailedException e) {}

        // We need to let the upsert go through if the item does not exist and
        // the condition expression does not have attribute_exists for a key and has
        // attribute_not_exists for some other attribute.
        // UPDATE_ONLY will not do the update and UPDATE will insert null
        // because of ELSE COL in its case expression.
        // TODO: uncomment after phoenix fix
        /*
        condPutRequest.conditionExpression("attribute_not_exists(payload)");
        dynamoDbClient.putItem(condPutRequest.build());
        phoenixDBClientV2.putItem(condPutRequest.build());
        ScanRequest sr = ScanRequest.builder().tableName(tableName).build();
        Assert.assertEquals(dynamoDbClient.scan(sr).items(), phoenixDBClientV2.scan(sr).items());
        */
    }

    @Test(timeout = 120000)
    public void conditionalPut() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item0 = new HashMap<>();
        item0.put("hk", AttributeValue.builder().s("foo").build());
        item0.put("sk", AttributeValue.builder().s("bar").build());
        item0.put("status", AttributeValue.builder().s("Active").build());
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item0).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("hk", AttributeValue.builder().s("abc").build());
        item1.put("sk", AttributeValue.builder().s("def").build());
        item1.put("updateCount", AttributeValue.builder().n("1").build());
        item1.put("status", AttributeValue.builder().s("Active").build());
        pir = PutItemRequest.builder().tableName(tableName).item(item1).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#s", "status");
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":status", AttributeValue.builder().s("Active").build());
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("hk", AttributeValue.builder().s("xyz").build());
        item2.put("sk", AttributeValue.builder().s("pqr").build());
        item2.put("updateCount", AttributeValue.builder().n("2").build());
        item2.put("status", AttributeValue.builder().s("Paused").build());

        // item not does exist
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item2)
                .conditionExpression("attribute_exists(sk) and attribute_not_exists(updateCount) and #s = :status")
                .expressionAttributeValues(exprAttrValues)
                .expressionAttributeNames(exprAttrNames)
                .build();
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}

        // item exists and updateCount attribute exists
        item1.put("status", AttributeValue.builder().s("Paused").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item1)
                .conditionExpression("attribute_exists(sk) and attribute_not_exists(updateCount) and #s = :status")
                .expressionAttributeValues(exprAttrValues)
                .expressionAttributeNames(exprAttrNames)
                .build();
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}

        // item exists, updateCount attribute does not exist but status does not match
        item0.put("status", AttributeValue.builder().s("Active").build());
        exprAttrValues.put(":status", AttributeValue.builder().s("Paused").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item0)
                .conditionExpression("attribute_exists(sk) and attribute_not_exists(updateCount) and #s = :status")
                .expressionAttributeValues(exprAttrValues)
                .expressionAttributeNames(exprAttrNames)
                .build();
        try {
            dynamoDbClient.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}
        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem should have thrown ConditionalCheckFailedException.");
        } catch (ConditionalCheckFailedException e) {}

        // item exists, updateCount attribute does not exist and status matches
        exprAttrValues.put(":status", AttributeValue.builder().s("Active").build());
        item0.put("status", AttributeValue.builder().s("Paused").build());
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item0)
                .conditionExpression("attribute_exists(sk) and attribute_not_exists(updateCount) and #s = :status")
                .expressionAttributeValues(exprAttrValues)
                .expressionAttributeNames(exprAttrNames)
                .build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        Assert.assertEquals(dynamoDbClient.scan(scanRequest).items(), phoenixDBClientV2.scan(scanRequest).items());
    }


    @Test(timeout = 120000)
    public void conditionalPutWithReturnValuesAllOldComplexConditionTest() throws Exception {
        // create table [attr_0, attr_1, attr_2]
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("val1").build());
        item1.put("attr_1", AttributeValue.builder().n("123").build());
        item1.put("attr_2", AttributeValue.builder().s("test").build());
        PutItemRequest item1PutRequest =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(item1PutRequest);
        dynamoDbClient.putItem(item1PutRequest);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("val1").build());
        item2.put("attr_1", AttributeValue.builder().n("456").build());
        item2.put("attr_2", AttributeValue.builder().s("updated").build());

        PutItemRequest.Builder condPutRequest =
                PutItemRequest.builder().tableName(tableName).item(item2);
        condPutRequest.conditionExpression("#1 > :0 AND #2 = :1");
        condPutRequest.returnValues(ReturnValue.ALL_OLD);
        condPutRequest.returnValuesOnConditionCheckFailure(
                ReturnValuesOnConditionCheckFailure.ALL_OLD);

        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "attr_2");
        condPutRequest.expressionAttributeNames(exprAttrNames);

        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", AttributeValue.builder().n("100").build());
        exprAttrVals.put(":1", AttributeValue.builder().s("test").build());
        condPutRequest.expressionAttributeValues(exprAttrVals);

        PutItemResponse dynamoResponse = dynamoDbClient.putItem(condPutRequest.build());
        PutItemResponse phoenixResponse = phoenixDBClientV2.putItem(condPutRequest.build());

        Assert.assertEquals("Returned items should match between DynamoDB and Phoenix",
                dynamoResponse.attributes(), phoenixResponse.attributes());
        Assert.assertEquals("Returned item should be the original item", item1,
                dynamoResponse.attributes());

        verifyItemUpdated(tableName, "val1", "456");
    }

    private void verifyItemUpdated(String tableName, String key, String expectedValue)
            throws SQLException {
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse dynamoScanResponse = dynamoDbClient.scan(scanRequest);
        ScanResponse phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        Assert.assertEquals("Scan response item counts should match",
                dynamoScanResponse.items().size(), phoenixScanResponse.items().size());
        Assert.assertEquals("Scan response items should match", dynamoScanResponse.items(),
                phoenixScanResponse.items());

        Assert.assertEquals("Should have exactly one item", 1, dynamoScanResponse.items().size());
        Map<String, AttributeValue> item = dynamoScanResponse.items().get(0);
        Assert.assertEquals("Item key should match", key, item.get("attr_0").s());
        Assert.assertEquals("Item value should be updated", expectedValue, item.get("attr_1").n());
    }

    private void verifyItemNotUpdated(String tableName, String key, String expectedValue)
            throws SQLException {
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse dynamoScanResponse = dynamoDbClient.scan(scanRequest);
        ScanResponse phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        Assert.assertEquals("Scan response item counts should match",
                dynamoScanResponse.items().size(), phoenixScanResponse.items().size());
        Assert.assertEquals("Scan response items should match", dynamoScanResponse.items(),
                phoenixScanResponse.items());

        Assert.assertEquals("Should have exactly one item", 1, dynamoScanResponse.items().size());
        Map<String, AttributeValue> item = dynamoScanResponse.items().get(0);
        Assert.assertEquals("Item key should match", key, item.get("attr_0").s());
        Assert.assertEquals("Item value should NOT be updated", expectedValue,
                item.get("attr_1").n());
    }

    private void verifyItemExists(String tableName, String key, String expectedValue)
            throws SQLException {
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse dynamoScanResponse = dynamoDbClient.scan(scanRequest);
        ScanResponse phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        Assert.assertEquals("Scan response item counts should match",
                dynamoScanResponse.items().size(), phoenixScanResponse.items().size());
        Assert.assertEquals("Scan response items should match", dynamoScanResponse.items(),
                phoenixScanResponse.items());

        Assert.assertEquals("Should have exactly one item", 1, dynamoScanResponse.items().size());
        Map<String, AttributeValue> item = dynamoScanResponse.items().get(0);
        Assert.assertEquals("Item key should match", key, item.get("attr_0").s());
        Assert.assertEquals("Item value should match", expectedValue, item.get("attr_1").n());
    }

}
