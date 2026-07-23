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

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

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
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Test class for Query API using legacy KeyConditions parameter instead of KeyConditionExpression.
 * This class mirrors the tests from org.apache.phoenix.ddb.QueryIT but uses KeyConditions for backward compatibility testing.
 */
public class Query2IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(Query2IT.class);

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
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        TestUtils.awaitPhoenixReady(url);

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
    public void queryLimitAndFilterTestWithKeyConditions() throws Exception {
        //create table
        final String tableName = "42We149X_TaBlE...--___";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request using KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // attr_0 = "B" AND attr_1 < 4 (KeyConditions always use AND logic)
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("B").build()).build());

        keyConditions.put("attr_1", Condition.builder().comparisonOperator("LT")
                .attributeValueList(AttributeValue.builder().n("4").build()).build());

        qr.keyConditions(keyConditions);

        // Use QueryFilter instead of FilterExpression (legacy parameter)
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put("Id2", Condition.builder().comparisonOperator("LE")
                .attributeValueList(AttributeValue.builder().n("1000.10").build()).build());
        qr.queryFilter(queryFilter);

        // query result, should return 2 items
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 2);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());

        // limit, should return 1 item
        qr.limit(1);
        phoenixResult = phoenixDBClientV2.query(qr.build());
        dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
    }

    @Test(timeout = 120000)
    public void queryBetweenTestWithKeyConditions() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request using KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // attr_0 = "B"
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("B").build()).build());

        // attr_1 BETWEEN 1 AND 5
        keyConditions.put("attr_1", Condition.builder().comparisonOperator("BETWEEN")
                .attributeValueList(AttributeValue.builder().n("1").build(),
                        AttributeValue.builder().n("5").build()).build());

        qr.keyConditions(keyConditions);

        // Use QueryFilter instead of FilterExpression (legacy parameter)
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put("Id1", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("0").build()).build());
        qr.queryFilter(queryFilter);

        // query result, should return 1 item
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void queryBeginsWithTestWithKeyConditions() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "title", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request using KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // attr_0 = "B"
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("B").build()).build());

        // title BEGINS_WITH "Title4"
        keyConditions.put("title", Condition.builder().comparisonOperator("BEGINS_WITH")
                .attributeValueList(AttributeValue.builder().s("Title4").build()).build());

        qr.keyConditions(keyConditions);

        // query result, should return 1 item
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void queryBinaryBeginsWithTestWithKeyConditions() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "title", ScalarAttributeType.B);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("pk1").build());
        item1.put("title",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3, 4, 5}))
                        .build());
        item1.put("val", AttributeValue.builder().n("100").build());
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("pk1").build());
        item2.put("title",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {5, 4, 3, 2, 1}))
                        .build());
        item2.put("val", AttributeValue.builder().n("300").build());
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);

        //query request using KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // attr_0 = "pk1"
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("pk1").build()).build());

        // title BEGINS_WITH {1,2,3}
        keyConditions.put("title", Condition.builder().comparisonOperator("BEGINS_WITH")
                .attributeValueList(
                        AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3}))
                                .build()).build());

        qr.keyConditions(keyConditions);

        // query result, should return 1 item
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void querySimpleProjectionTestWithKeyConditions() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);

        //query request using KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // attr_0 = "B"
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("B").build()).build());

        qr.keyConditions(keyConditions);

        String projectionExpr = "Id2, title, #proj";
        qr.projectionExpression(projectionExpr);
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#proj", "A.B");
        qr.expressionAttributeNames(exprAttrNames);

        // query result, should return 1 item with only the projected attributes
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void queryScanIndexForwardTestWithKeyConditions() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request using KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // attr_0 = "B"
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("B").build()).build());

        // attr_1 > 1
        keyConditions.put("attr_1", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("1").build()).build());

        qr.keyConditions(keyConditions);
        qr.scanIndexForward(false);

        // query result, should return 3 items in descending order of sort key
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 3);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (int i = 0; i < phoenixResult.items().size() - 1; i++) {
            int sortKeyVal1 = Integer.parseInt(phoenixResult.items().get(i).get("attr_1").n());
            int sortKeyVal2 = Integer.parseInt(phoenixResult.items().get(i + 1).get("attr_1").n());
            Assert.assertTrue(sortKeyVal1 >= sortKeyVal2);
        }
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void queryLastEvaluatedKeyPagingWithHashSortKeysTestWithKeyConditions()
            throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("B").build()).build());
        keyConditions.put("attr_1", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("1").build()).build());

        QueryRequest.Builder qr =
                QueryRequest.builder().tableName(tableName).keyConditions(keyConditions).limit(2);
        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);

        qr = QueryRequest.builder().tableName(tableName).keyConditions(keyConditions).limit(2)
                .scanIndexForward(false);
        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
    }

    @Test(timeout = 120000)
    public void queryLastEvaluatedKeyPagingWithHashKeyTestWithKeyConditions() throws Exception {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request using KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // attr_0 = "B"
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("B").build()).build());

        qr.keyConditions(keyConditions);
        qr.limit(1);
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());

        while (phoenixResult.count() > 0) {
            Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
            Assert.assertEquals(dynamoResult.items(), phoenixResult.items());
            Assert.assertEquals(dynamoResult.lastEvaluatedKey(), phoenixResult.lastEvaluatedKey());
            qr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
            phoenixResult = phoenixDBClientV2.query(qr.build());
            dynamoResult = dynamoDbClient.query(qr.build());
        }
    }

    @Test(timeout = 120000)
    public void testKeyConditionsAndKeyConditionExpressionCannotBeBothPresent() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);

        //query request with both KeyConditions and KeyConditionExpression - should fail
        try {
            QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
            Map<String, Condition> keyConditions = new HashMap<>();

            keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                    .attributeValueList(AttributeValue.builder().s("test").build()).build());

            qr.keyConditions(keyConditions);
            qr.keyConditionExpression("attr_0 = :val");
            Map<String, AttributeValue> exprAttrVal = new HashMap<>();
            exprAttrVal.put(":val", AttributeValue.builder().s("test").build());
            qr.expressionAttributeValues(exprAttrVal);

            phoenixDBClientV2.query(qr.build());
            Assert.fail(
                    "Should have thrown an exception when both KeyConditions and KeyConditionExpression are present");
        } catch (Exception e) {
            // Expected - should throw validation error
            Assert.assertTrue("Exception should mention both parameters cannot be specified",
                    e.getMessage().contains("Cannot specify both") || e.getMessage()
                            .contains("KeyCondition"));
        }
    }

    @Test(timeout = 120000)
    public void testKeyConditionsAndKeyConditionExpressionValidationError() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("test").build()).build());

        qr.keyConditions(keyConditions);
        qr.keyConditionExpression("attr_0 = :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", AttributeValue.builder().s("test").build());
        qr.expressionAttributeValues(exprAttrVal);

        QueryRequest queryRequest = qr.build();

        try {
            dynamoDbClient.query(queryRequest);
            Assert.fail("Should have thrown an exception");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.query(queryRequest);
            Assert.fail("Should have thrown an exception");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
    }

    @Test(timeout = 120000)
    public void testQueryFilterWithConditionalOperatorAND() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "partition_id", ScalarAttributeType.S,
                        "sort_id", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert test data
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("partition_id", AttributeValue.builder().s("pk1").build());
        item1.put("sort_id", AttributeValue.builder().s("sk1").build());
        item1.put("attr1", AttributeValue.builder().s("value1").build());
        item1.put("attr2", AttributeValue.builder().n("10").build());
        PutItemRequest putRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putRequest1);
        dynamoDbClient.putItem(putRequest1);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("partition_id", AttributeValue.builder().s("pk1").build());
        item2.put("sort_id", AttributeValue.builder().s("sk2").build());
        item2.put("attr1", AttributeValue.builder().s("value2").build());
        item2.put("attr2", AttributeValue.builder().n("20").build());
        PutItemRequest putRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(putRequest2);
        dynamoDbClient.putItem(putRequest2);

        // Query with KeyConditions and QueryFilter using ConditionalOperator AND
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);

        // KeyConditions (partition key)
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put("partition_id", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("pk1").build()).build());
        qr.keyConditions(keyConditions);

        // QueryFilter with multiple conditions
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put("attr1", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("value1").build()).build());
        queryFilter.put("attr2", Condition.builder().comparisonOperator("GE")
                .attributeValueList(AttributeValue.builder().n("10").build()).build());
        qr.queryFilter(queryFilter);
        qr.conditionalOperator("AND"); // Both conditions must match

        QueryRequest queryRequest = qr.build();

        // Execute query on both DynamoDB and Phoenix
        QueryResponse phoenixResponse = phoenixDBClientV2.query(queryRequest);
        QueryResponse dynamoResponse = dynamoDbClient.query(queryRequest);

        // Compare results
        Assert.assertNotNull("Phoenix response should not be null", phoenixResponse);
        Assert.assertNotNull("DynamoDB response should not be null", dynamoResponse);
        Assert.assertEquals("Count should match between DynamoDB and Phoenix",
                dynamoResponse.count(), phoenixResponse.count());
        Assert.assertEquals("Should find 1 item (attr1=value1 AND attr2>=10)", 1,
                phoenixResponse.count().intValue());
        Assert.assertEquals("Items should match between DynamoDB and Phoenix",
                dynamoResponse.items(), phoenixResponse.items());
    }

    @Test(timeout = 120000)
    public void testQueryFilterWithConditionalOperatorOR() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "partition_id", ScalarAttributeType.S,
                        "sort_id", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert test data
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("partition_id", AttributeValue.builder().s("pk1").build());
        item1.put("sort_id", AttributeValue.builder().s("sk1").build());
        item1.put("attr1", AttributeValue.builder().s("special").build());
        item1.put("attr2", AttributeValue.builder().n("5").build());
        PutItemRequest putRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putRequest1);
        dynamoDbClient.putItem(putRequest1);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("partition_id", AttributeValue.builder().s("pk1").build());
        item2.put("sort_id", AttributeValue.builder().s("sk2").build());
        item2.put("attr1", AttributeValue.builder().s("normal").build());
        item2.put("attr2", AttributeValue.builder().n("25").build());
        PutItemRequest putRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(putRequest2);
        dynamoDbClient.putItem(putRequest2);

        // Query with QueryFilter using ConditionalOperator OR
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);

        // KeyConditions (partition key)
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put("partition_id", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("pk1").build()).build());
        qr.keyConditions(keyConditions);

        // QueryFilter with OR conditions
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put("attr1", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("special").build()).build());
        queryFilter.put("attr2", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("20").build()).build());
        qr.queryFilter(queryFilter);
        qr.conditionalOperator("OR"); // Either condition can match

        QueryRequest queryRequest = qr.build();

        // Execute query on both DynamoDB and Phoenix
        QueryResponse phoenixResponse = phoenixDBClientV2.query(queryRequest);
        QueryResponse dynamoResponse = dynamoDbClient.query(queryRequest);

        // Compare results
        Assert.assertNotNull("Phoenix response should not be null", phoenixResponse);
        Assert.assertNotNull("DynamoDB response should not be null", dynamoResponse);
        Assert.assertEquals("Count should match between DynamoDB and Phoenix",
                dynamoResponse.count(), phoenixResponse.count());
        Assert.assertEquals("Should find 2 items (attr1=special OR attr2>20)", 2,
                phoenixResponse.count().intValue());
        Assert.assertEquals("Items should match between DynamoDB and Phoenix",
                dynamoResponse.items(), phoenixResponse.items());
    }

    @Test(timeout = 120000)
    public void testQueryFilterAndFilterExpressionValidationError() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Query request with both QueryFilter and FilterExpression - should fail
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);

        // KeyConditions
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("test").build()).build());
        qr.keyConditions(keyConditions);

        // QueryFilter (legacy)
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put("attr1", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("value").build()).build());
        qr.queryFilter(queryFilter);

        // FilterExpression (modern)
        qr.filterExpression("attr2 = :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", AttributeValue.builder().s("test").build());
        qr.expressionAttributeValues(exprAttrVal);

        try {
            phoenixDBClientV2.query(qr.build());
            Assert.fail("Should have thrown an exception");
        } catch (DynamoDbException e) {
            Assert.assertEquals("Status code should be 400 for validation error", 400,
                    e.statusCode());
        }
        try {
            dynamoDbClient.query(qr.build());
            Assert.fail("Should have thrown an exception");
        } catch (DynamoDbException e) {
            Assert.assertEquals("Status code should be 400 for validation error", 400,
                    e.statusCode());
        }
    }

    @Test(timeout = 120000)
    public void querySinglePartitionKeyTestWithKeyConditions() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "partition_id", ScalarAttributeType.S,
                        null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue>[] items = new Map[5];
        for (int i = 0; i < 5; i++) {
            items[i] = new HashMap<>();
            items[i].put("partition_id", AttributeValue.builder().s("pk" + i).build());
            items[i].put("value", AttributeValue.builder().s("value" + i).build());
            items[i].put("n0123", AttributeValue.builder().n(String.valueOf(i * 100)).build());
            items[i].put("ss00", AttributeValue.builder().s(UUID.randomUUID().toString()).build());
            items[i].put("b012", AttributeValue.builder()
                    .b(SdkBytes.fromByteArray(Bytes.toBytes(UUID.randomUUID().toString())))
                    .build());

            PutItemRequest putItemRequest =
                    PutItemRequest.builder().tableName(tableName).item(items[i]).build();

            phoenixDBClientV2.putItem(putItemRequest);
            dynamoDbClient.putItem(putItemRequest);
        }

        for (int i = 0; i < 5; i++) {
            QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
            Map<String, Condition> keyConditions = new HashMap<>();

            // partition_id = "pk" + i
            keyConditions.put("partition_id", Condition.builder().comparisonOperator("EQ")
                    .attributeValueList(AttributeValue.builder().s("pk" + i).build()).build());

            qr.keyConditions(keyConditions);

            Map<String, AttributeValue> lastEvaluatedKey = null;

            do {
                qr.exclusiveStartKey(lastEvaluatedKey);

                QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
                QueryResponse dynamoResult = dynamoDbClient.query(qr.build());

                Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
                if (dynamoResult.count() > 0) {
                    Assert.assertEquals("Number of items should match", dynamoResult.items().size(),
                            phoenixResult.items().size());
                    for (int j = 0; j < dynamoResult.items().size(); j++) {
                        Assert.assertTrue("Items at position " + j + " should match",
                                ItemComparator.areItemsEqual(dynamoResult.items().get(j),
                                        phoenixResult.items().get(j)));
                    }
                }
                Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());

                lastEvaluatedKey = phoenixResult.lastEvaluatedKey();
            } while (!lastEvaluatedKey.isEmpty());
        }

        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // partition_id = "non_existent"
        keyConditions.put("partition_id", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("non_existent").build()).build());

        qr.keyConditions(keyConditions);

        Map<String, AttributeValue> lastEvaluatedKey = null;

        do {
            if (lastEvaluatedKey != null) {
                qr.exclusiveStartKey(lastEvaluatedKey);
            }

            QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
            QueryResponse dynamoResult = dynamoDbClient.query(qr.build());

            Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
            if (dynamoResult.count() > 0) {
                Assert.assertEquals("Number of items should match", dynamoResult.items().size(),
                        phoenixResult.items().size());
                for (int j = 0; j < dynamoResult.items().size(); j++) {
                    Assert.assertTrue("Items at position " + j + " should match",
                            ItemComparator.areItemsEqual(dynamoResult.items().get(j),
                                    phoenixResult.items().get(j)));
                }
            }
            Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());

            lastEvaluatedKey = phoenixResult.lastEvaluatedKey();
        } while (!lastEvaluatedKey.isEmpty());
    }

    public static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("A").build());
        item.put("attr_1", AttributeValue.builder().n("1").build());
        item.put("Id1", AttributeValue.builder().n("-5").build());
        item.put("Id2", AttributeValue.builder().n("10.10").build());
        item.put("title", AttributeValue.builder().s("Title1").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Alice").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar",
                AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 1").build());
        return item;
    }

    public static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("B").build());
        item.put("attr_1", AttributeValue.builder().n("2").build());
        item.put("Id1", AttributeValue.builder().n("-15").build());
        item.put("Id2", AttributeValue.builder().n("150.10").build());
        item.put("title", AttributeValue.builder().s("Title2").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Bob1").build());
        Map<String, AttributeValue> reviewMap2 = new HashMap<>();
        reviewMap2.put("reviewer", AttributeValue.builder().s("Bob2").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder()
                .l(AttributeValue.builder().m(reviewMap1).build(),
                        AttributeValue.builder().m(reviewMap2).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 2").build());
        return item;
    }

    public static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("B").build());
        item.put("attr_1", AttributeValue.builder().n("3").build());
        item.put("Id1", AttributeValue.builder().n("11").build());
        item.put("Id2", AttributeValue.builder().n("1000.10").build());
        item.put("title", AttributeValue.builder().s("Title3").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Carl").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar",
                AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 3").build());
        return item;
    }

    public static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("B").build());
        item.put("attr_1", AttributeValue.builder().n("4").build());
        item.put("Id1", AttributeValue.builder().n("-23").build());
        item.put("Id2", AttributeValue.builder().n("99.10").build());
        item.put("title", AttributeValue.builder().s("Title40").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Drake").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar",
                AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 4").build());
        return item;
    }
}
