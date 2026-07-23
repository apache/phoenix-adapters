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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class GetItemIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetItemIT.class);

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
        DriverManager.registerDriver(new PhoenixTestDriver());
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
    public void testGetWithConditionPut() throws Exception {
        CreateTableRequest request = CreateTableRequest.builder().tableName("Xyzzz..--__")
                .billingMode(BillingMode.PAY_PER_REQUEST).globalSecondaryIndexes(Arrays.asList(
                        GlobalSecondaryIndex.builder().indexName("outstanding_tasks").keySchema(
                                        Arrays.asList(KeySchemaElement.builder()
                                                        .attributeName("outstanding_tasks_hk").keyType(
                                                                KeyType.HASH)
                                                        .build(),
                                                KeySchemaElement.builder().attributeName("execute_after")
                                                        .keyType(KeyType.RANGE).build()

                                        )).projection(
                                        Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build()

                )).attributeDefinitions(Arrays.asList(
                        AttributeDefinition.builder().attributeName("hk")
                                .attributeType(ScalarAttributeType.B).build(),
                        AttributeDefinition.builder().attributeName("sk")
                                .attributeType(ScalarAttributeType.B).build(),
                        AttributeDefinition.builder().attributeName("outstanding_tasks_hk")
                                .attributeType(ScalarAttributeType.B).build(),
                        AttributeDefinition.builder().attributeName("execute_after")
                                .attributeType(ScalarAttributeType.B).build()

                )).keySchema(Arrays.asList(
                        KeySchemaElement.builder().attributeName("hk").keyType(KeyType.HASH)
                                .build(),
                        KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE)
                                .build()

                )).build();
        dynamoDbClient.createTable(request);
        phoenixDBClientV2.createTable(request);

        Map<String, AttributeValue> map = new HashMap<>();
        map.put("hk", AttributeValue.builder().b(SdkBytes.fromByteArray(
                        new byte[] {3, 83, 72, 65, 82, 69, 68, 46, 109, 101, 116, 97, 100, 97, 116, 97, 46,
                                100, 101, 118, 46, 68, 111, 99, 117, 109, 101, 110, 116, 115, 0, 1}))
                .build());
        map.put("sk", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0})).build());
        map.put("submitted_datetime", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[] {-47, -121, 37, -84, 75, -64})).build());
        map.put("task_count", AttributeValue.builder().n("1").build());
        map.put("outstanding_tasks", AttributeValue.builder().ns(Arrays.asList("0")).build());
        map.put("custom_params", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[] {1, -128, 0, 0, 0, -128, 0, 0, 1})).build());

        Map<String, String> expressionAttributes = new HashMap<>();
        expressionAttributes.put("#0", "hk");
        expressionAttributes.put("#1", "sk");

        PutItemRequest putItemRequest = PutItemRequest.builder().tableName("Xyzzz..--__")
                .item(map)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .conditionExpression("attribute_not_exists(#0) AND attribute_not_exists(#1)")
                .expressionAttributeNames(expressionAttributes)
                .build();
        dynamoDbClient.putItem(putItemRequest);
        phoenixDBClientV2.putItem(putItemRequest);

        Map<String, AttributeValue> keys = new HashMap<>();
        keys.put("hk", AttributeValue.builder().b(SdkBytes.fromByteArray(
                        new byte[] {3, 83, 72, 65, 82, 69, 68, 46, 109, 101, 116, 97, 100, 97, 116, 97, 46,
                                100, 101, 118, 46, 68, 111, 99, 117, 109, 101, 110, 116, 115, 0, 1}))
                .build());
        keys.put("sk", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0})).build());

        GetItemRequest getItemRequest =
                GetItemRequest.builder().tableName("Xyzzz..--__").key(keys).consistentRead(true)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();
        GetItemResponse getItemResponse1 = dynamoDbClient.getItem(getItemRequest);
        GetItemResponse getItemResponse2 = phoenixDBClientV2.getItem(getItemRequest);

        Assert.assertEquals(getItemResponse2.item(), getItemResponse1.item());

        keys.put("hk", AttributeValue.builder().b(SdkBytes.fromByteArray(
                        new byte[] {3, 83, 72, 65, 82, 69, 68, 46, 109, 101, 116, 97, 100, 97, 116, 97, 46,
                                100, 101, 118, 46, 68, 111, 99, 117, 109, 101, 110, 116, 115, 0, 1}))
                .build());
        keys.put("sk", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0, 0})).build());
        getItemRequest =
                GetItemRequest.builder().tableName("Xyzzz..--__").key(keys).consistentRead(true)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();
        getItemResponse1 = dynamoDbClient.getItem(getItemRequest);
        getItemResponse2 = phoenixDBClientV2.getItem(getItemRequest);

        Assert.assertEquals(getItemResponse2.item(), getItemResponse1.item());

    }

    @Test(timeout = 120000)
    public void testBinaryGetItemAndQuery() {
        CreateTableRequest request = CreateTableRequest.builder()
                .tableName("Tablex2048hfg.shLinux")
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .globalSecondaryIndexes(Arrays.asList(
                        GlobalSecondaryIndex.builder()
                                .indexName("outstanding_tasks")
                                .keySchema(Arrays.asList(
                                        KeySchemaElement.builder()
                                                .attributeName("outstanding_tasks_hk")
                                                .keyType(KeyType.HASH)
                                                .build(),
                                        KeySchemaElement.builder()
                                                .attributeName("execute_after")
                                                .keyType(KeyType.RANGE)
                                                .build()
                                ))
                                .projection(Projection.builder()
                                        .projectionType(ProjectionType.ALL)
                                        .build())
                                .build()
                ))
                .attributeDefinitions(Arrays.asList(
                        AttributeDefinition.builder()
                                .attributeName("hk")
                                .attributeType(ScalarAttributeType.B)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName("sk")
                                .attributeType(ScalarAttributeType.B)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName("outstanding_tasks_hk")
                                .attributeType(ScalarAttributeType.B)
                                .build(),
                        AttributeDefinition.builder()
                                .attributeName("execute_after")
                                .attributeType(ScalarAttributeType.B)
                                .build()
                ))
                .keySchema(Arrays.asList(
                        KeySchemaElement.builder()
                                .attributeName("hk")
                                .keyType(KeyType.HASH)
                                .build(),
                        KeySchemaElement.builder()
                                .attributeName("sk")
                                .keyType(KeyType.RANGE)
                                .build()
                ))
                .build();
        dynamoDbClient.createTable(request);
        phoenixDBClientV2.createTable(request);

        Map<String, AttributeValue> keyMap = new HashMap<>();
        keyMap.put("hk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{7, 83, 72, 65, 82, 69, 68, 46, 115, 99,
                        111, 112, 101, 115, 46, 68, 101, 118, 84, 101, 110, 97, 110, 116,
                        115, 0, 1}))
                .build());
        keyMap.put("sk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{0}))
                .build());

        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName("Tablex2048hfg.shLinux")
                .key(keyMap)
                .consistentRead(true)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build();
        GetItemResponse getItemResponse1 = dynamoDbClient.getItem(getItemRequest);
        GetItemResponse getItemResponse2 = phoenixDBClientV2.getItem(getItemRequest);
        Assert.assertEquals(getItemResponse2.item(), getItemResponse1.item());

        keyMap = new HashMap<>();
        keyMap.put("hk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{7, 83, 72, 65, 82, 69, 68, 46, 115, 99, 111,
                        112, 101, 115, 46, 67, 108, 111, 117, 100, 115, 0, 1}))
                .build());
        keyMap.put("sk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{0}))
                .build());

        getItemRequest = GetItemRequest.builder()
                .tableName("Tablex2048hfg.shLinux")
                .key(keyMap)
                .consistentRead(true)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build();
        getItemResponse1 = dynamoDbClient.getItem(getItemRequest);
        getItemResponse2 = phoenixDBClientV2.getItem(getItemRequest);
        Assert.assertEquals(getItemResponse2.item(), getItemResponse1.item());

        Map<String, AttributeValue> itemMap = new HashMap<>();
        itemMap.put("hk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{7, 83, 72, 65, 82, 69, 68, 46, 115, 99, 111,
                        112, 101, 115, 46, 68, 101, 118, 84, 101, 110, 97, 110, 116, 115, 0, 1}))
                .build());
        itemMap.put("sk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{0, -128}))
                .build());
        itemMap.put("outstanding_tasks_hk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{0}))
                .build());
        itemMap.put("execute_after", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{-47, -121, 46, -95, 74, -48}))
                .build());
        itemMap.put("scheduled_execute_after", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{-47, -121, 46, -95, 74, -48}))
                .build());

        Map<String, String> expressionAttributeNamesMap = new HashMap<>();
        expressionAttributeNamesMap.put("#0", "hk");
        expressionAttributeNamesMap.put("#1", "sk");

        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName("Tablex2048hfg.shLinux")
                .item(itemMap)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .conditionExpression("attribute_not_exists(#0) AND attribute_not_exists(#1)")
                .expressionAttributeNames(expressionAttributeNamesMap)
                .build();
        dynamoDbClient.putItem(putItemRequest);
        phoenixDBClientV2.putItem(putItemRequest);

        keyMap = new HashMap<>();
        keyMap.put("hk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{7, 83, 72, 65, 82, 69, 68, 46, 115, 99, 111,
                        112, 101, 115, 46, 68, 101, 118, 84, 101, 110, 97, 110, 116, 115, 0, 1}))
                .build());
        keyMap.put("sk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{0}))
                .build());

        getItemRequest = GetItemRequest.builder()
                .tableName("Tablex2048hfg.shLinux")
                .key(keyMap)
                .consistentRead(true)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build();
        getItemResponse1 = dynamoDbClient.getItem(getItemRequest);
        getItemResponse2 = phoenixDBClientV2.getItem(getItemRequest);
        Assert.assertEquals(getItemResponse2.item(), getItemResponse1.item());

        keyMap = new HashMap<>();
        keyMap.put("hk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{7, 83, 72, 65, 82, 69, 68, 46, 115, 99, 111,
                        112, 101, 115, 46, 68, 101, 118, 84, 101, 110, 97, 110, 116, 115, 0, 1}))
                .build());
        keyMap.put("sk", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{0, -128}))
                .build());

        getItemRequest = GetItemRequest.builder()
                .tableName("Tablex2048hfg.shLinux")
                .key(keyMap)
                .consistentRead(true)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .build();
        getItemResponse1 = dynamoDbClient.getItem(getItemRequest);
        getItemResponse2 = phoenixDBClientV2.getItem(getItemRequest);
        Assert.assertEquals(getItemResponse2.item(), getItemResponse1.item());

        TestUtils.waitForEventualConsistentIndex();

        QueryRequest.Builder qr = QueryRequest.builder().tableName("Tablex2048hfg.shLinux");
        qr.indexName("outstanding_tasks");
        qr.keyConditionExpression("#0 = :v0 AND #1 = :v1");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "outstanding_tasks_hk");
        exprAttrNames.put("#1", "execute_after");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{0}))
                .build());
        exprAttrVal.put(":v1", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{-47, -121, 46, -95, 74, -48}))
                .build());
        qr.expressionAttributeValues(exprAttrVal);

        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(new Integer(1), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());

        qr = QueryRequest.builder().tableName("Tablex2048hfg.shLinux");
        qr.indexName("outstanding_tasks");
        qr.keyConditionExpression("#0 = :v0 AND #1 = :v1");
        exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "outstanding_tasks_hk");
        exprAttrNames.put("#1", "execute_after");
        qr.expressionAttributeNames(exprAttrNames);
        exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{0}))
                .build());
        exprAttrVal.put(":v1", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(new byte[]{-47, -121, 46, -95, 74, -48, 0}))
                .build());
        qr.expressionAttributeValues(exprAttrVal);

        phoenixResult = phoenixDBClientV2.query(qr.build());
        dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(new Integer(0), phoenixResult.count());
    }

    @Test(timeout = 120000)
    public void testWithPartitionAndSortCol() throws Exception {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "SubjectNumber", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem5()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("SubjectNumber", AttributeValue.builder().n("20").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "LastPostDateTime, Message";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }

    @Test(timeout = 120000)
    public void testWithOnlyPartitionCol() throws Exception {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //create key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "Message, Tag";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }

    @Test(timeout = 120000)
    public void testWithTwoItemsHavingSamePartitionColNames() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "Subject", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put multiple items
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        phoenixDBClientV2.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest2);

        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        phoenixDBClientV2.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest3);

        //create key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "LastPostDateTime, Message, Tag";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }

    @Test(timeout = 120000)
    public void testWithNoResultFound() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "Subject", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Phoenix").build());
        key.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "LastPostDateTime, Message, Tag";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }

    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("201303190436").build());
        item.put("Message", AttributeValue.builder()
                .s("I want to update multiple items in a single call. What's the best way to do that?")
                .build());
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Phoenix").build());
        item.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("201303190429").build());
        item.put("Message", AttributeValue.builder()
                .s("I want to update multiple items in a single call. What's the best way to do that?")
                .build());
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Subject", AttributeValue.builder().s("How do I update a single items?").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("2013031906422").build());
        item.put("Message", AttributeValue.builder()
                .s("I want to update multiple items in a single call. What's the best way to do that?")
                .build());
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("201303190317").build());
        item.put("Message", AttributeValue.builder()
                .s("I want to update multiple items in a single call. What's the best way to do that?")
                .build());
        return item;
    }

    private static Map<String, AttributeValue> getItem5() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("SubjectNumber", AttributeValue.builder().n("20").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("201303190436").build());
        item.put("Message", AttributeValue.builder()
                .s("I want to update multiple items in a single call. What's the best way to do that?")
                .build());
        return item;
    }

    @Test(timeout = 120000)
    public void testGetItemWithAttributesToGet() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "Subject", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        gI.attributesToGet(Arrays.asList("LastPostDateTime", "Message", "Tag"));
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());

        GetItemRequest.Builder gI2 = GetItemRequest.builder().tableName(tableName).key(key);
        gI2.attributesToGet(Arrays.asList("Message"));
        GetItemResponse dynamoResult2 = dynamoDbClient.getItem(gI2.build());
        GetItemResponse phoenixResult2 = phoenixDBClientV2.getItem(gI2.build());
        Assert.assertEquals(dynamoResult2.item(), phoenixResult2.item());
    }

    @Test(timeout = 120000)
    public void testGetItemWithAttributesToGetAndValidationError() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "Subject", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        gI.attributesToGet(Arrays.asList("LastPostDateTime", "Message"));
        gI.projectionExpression("LastPostDateTime, Message, Tag");

        try {
            phoenixDBClientV2.getItem(gI.build());
            Assert.fail("Expected ValidationException for both "
                    + "AttributesToGet and ProjectionExpression");
        } catch (DynamoDbException e) {
            Assert.assertEquals("Expected 400 status code for Phoenix", 400, e.statusCode());
        }

        try {
            dynamoDbClient.getItem(gI.build());
            Assert.fail("Expected ValidationException for both "
                    + "AttributesToGet and ProjectionExpression");
        } catch (DynamoDbException e) {
            Assert.assertEquals("Expected 400 status code for DynamoDB", 400, e.statusCode());
        }
    }
}
