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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class BatchGetItemIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchGetItemIT.class);

    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    final static String tableName1 = "FoRum";
    final static String tableName2 = "dataBase";

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

        //create table
        CreateTableRequest createTableRequest1 =
                DDLTestUtils.getCreateTableRequest(tableName1, "ForumName", ScalarAttributeType.S,
                        null, null);
        CreateTableRequest createTableRequest2 =
                DDLTestUtils.getCreateTableRequest(tableName2, "DatabaseName",
                        ScalarAttributeType.S, "Id", ScalarAttributeType.N);

        dynamoDbClient =
                LocalDynamoDbTestBase.localDynamoDb().createV2Client();

        phoenixDBClientV2.createTable(createTableRequest1);
        dynamoDbClient.createTable(createTableRequest1);
        phoenixDBClientV2.createTable(createTableRequest2);
        dynamoDbClient.createTable(createTableRequest2);

        //put items in both tables
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName1).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName1).item(getItem2()).build();

        phoenixDBClientV2.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest2);

        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName1).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest4);

        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName2).item(getItem3()).build();
        phoenixDBClientV2.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest3);
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

    @Test
    public void testAllTablesWithResultsFound() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        Map<String, AttributeValue> key2 = new HashMap<>();
        key2.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());

        //putting keys for FORUM table in a list since KeyAndAttributes is List<Map<String, AttributeValue>>
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        forumKeys.add(key1);
        forumKeys.add(key2);

        //putting that list in KeysAndAttribute object
        //set projection expression for that item
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        KeysAndAttributes forForum = KeysAndAttributes.builder().keys(forumKeys)
                .projectionExpression("ForumName, Threads, Messages").build();
        requestItems.put(tableName1, forForum);

        //making keys for DATABASE table
        Map<String, AttributeValue> key3 = new HashMap<>();
        key3.put("DatabaseName", AttributeValue.builder().s("Amazon Redshift").build());
        key3.put("Id", AttributeValue.builder().n("25").build());

        //putting keys for DATABASE table in a list
        List<Map<String, AttributeValue>> databaseKeys = new ArrayList<>();
        databaseKeys.add(key3);

        //putting that list in KeysAndAttributes Object
        //set projection expression for that item
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        KeysAndAttributes forDatabase =
                KeysAndAttributes.builder().keys(databaseKeys).projectionExpression("Tags, Message")
                        .build();
        requestItems.put(tableName2, forDatabase);
        BatchGetItemRequest gI = BatchGetItemRequest.builder().requestItems(requestItems).build();
        BatchGetItemResponse dynamoResult = dynamoDbClient.batchGetItem(gI);
        BatchGetItemResponse phoenixResult = phoenixDBClientV2.batchGetItem(gI);
        Assert.assertEquals(dynamoResult.responses(), phoenixResult.responses());
    }

    @Test
    public void testOneTableHasNoResultFound() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table
        //key1 is not found in the table
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", AttributeValue.builder().s("Amazon API").build());
        Map<String, AttributeValue> key2 = new HashMap<>();
        key2.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());

        //putting keys for FORUM table in a list since KeyAndAttributes is List<Map<String, AttributeValue>>
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        forumKeys.add(key1);
        forumKeys.add(key2);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes.Builder forForum = KeysAndAttributes.builder();
        forForum.keys(forumKeys);
        //set projection expression for that item
        String projectionExprForForum = "ForumName, Threads, Messages";
        forForum.projectionExpression(projectionExprForForum);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put(tableName1, forForum.build());

        //making keys for DATABASE table
        Map<String, AttributeValue> key3 = new HashMap<>();
        key3.put("DatabaseName", AttributeValue.builder().s("Amazon Redshift").build());
        key3.put("Id", AttributeValue.builder().n("25").build());

        //putting keys for DATABASE table in a list
        List<Map<String, AttributeValue>> databaseKeys = new ArrayList<>();
        databaseKeys.add(key3);

        //putting that list in KeysAndAttributes Object
        KeysAndAttributes.Builder forDatabase = KeysAndAttributes.builder();
        forDatabase.keys(databaseKeys);
        //set projection expression for that item
        String projectionExprForDatabase = "Tags, Message";
        forDatabase.projectionExpression(projectionExprForDatabase);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put(tableName2, forDatabase.build());

        BatchGetItemRequest gI = BatchGetItemRequest.builder().requestItems(requestItems).build();
        BatchGetItemResponse dynamoResult = dynamoDbClient.batchGetItem(gI);
        BatchGetItemResponse phoenixResult = phoenixDBClientV2.batchGetItem(gI);
        Assert.assertEquals(dynamoResult.responses(), phoenixResult.responses());
    }

    @Test
    public void testBothTablesHaveNoResultFound() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table which doesnt exist
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", AttributeValue.builder().s("Amazon API").build());

        //putting key for FORUM table in a list since KeyAndAttributes is List<Map<String, AttributeValue>>
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        forumKeys.add(key1);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes.Builder forForum = KeysAndAttributes.builder();
        forForum.keys(forumKeys);
        //set projection expression for that item
        String projectionExprForForum = "ForumName, Threads, Messages";
        forForum.projectionExpression(projectionExprForForum);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put(tableName1, forForum.build());

        //making keys for DATABASE table which doesn't exist
        Map<String, AttributeValue> key3 = new HashMap<>();
        //Partition Key exists but not sort key
        key3.put("DatabaseName", AttributeValue.builder().s("Amazon RDS").build());
        key3.put("Id", AttributeValue.builder().n("39").build());

        //putting key for DATABASE table in a list
        List<Map<String, AttributeValue>> databaseKeys = new ArrayList<>();
        databaseKeys.add(key3);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes.Builder forDatabase = KeysAndAttributes.builder();
        forDatabase.keys(databaseKeys);
        //set projection expression for that item
        String projectionExprForDatabase = "Tags, Message";
        forDatabase.projectionExpression(projectionExprForDatabase);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put(tableName2, forDatabase.build());

        BatchGetItemRequest gI = BatchGetItemRequest.builder().requestItems(requestItems).build();
        BatchGetItemResponse dynamoResult = dynamoDbClient.batchGetItem(gI);
        BatchGetItemResponse phoenixResult = phoenixDBClientV2.batchGetItem(gI);
        Assert.assertEquals(dynamoResult.responses(), phoenixResult.responses());
    }

    @Test
    public void testWithNestedList() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        Map<String, AttributeValue> key2 = new HashMap<>();
        key2.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());

        //putting keys for FORUM table in a list since KeyAndAttributes is List<Map<String, AttributeValue>>
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        forumKeys.add(key1);
        forumKeys.add(key2);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes.Builder forForum = KeysAndAttributes.builder();
        forForum.keys(forumKeys);
        //set expression attribute name for that item
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "DatabaseName");
        exprAttrNames.put("#1", "Id");
        exprAttrNames.put("#2", "Feedback");
        forForum.expressionAttributeNames(exprAttrNames);
        //set projection expression for that item
        String projectionExpr = "#0, #1, #2.FeedbackDetails[0].Sender";
        forForum.projectionExpression(projectionExpr);

        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put(tableName1, forForum.build());

        BatchGetItemRequest gI = BatchGetItemRequest.builder().requestItems(requestItems).build();
        BatchGetItemResponse dynamoResult = dynamoDbClient.batchGetItem(gI);
        BatchGetItemResponse phoenixResult = phoenixDBClientV2.batchGetItem(gI);
        Assert.assertEquals(dynamoResult.responses(), phoenixResult.responses());
    }

    @Test
    public void testValidationException() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table
        //putting 60 keys
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        for (int i = 0; i < 60; i++) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("ForumName", AttributeValue.builder().s("str" + i).build());
            forumKeys.add(key);
        }


        //putting that list in KeysAndAttribute object
        KeysAndAttributes.Builder forForum = KeysAndAttributes.builder();
        forForum.keys(forumKeys);
        //set projection expression for that item
        String projectionExpr = "DatabaseName, Id";
        forForum.projectionExpression(projectionExpr);

        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put(tableName1, forForum.build());

        //making keys for DATABASE table, 50 keys
        //putting keys for DATABASE table in a list
        List<Map<String, AttributeValue>> databaseKeys = new ArrayList<>();
        for (int i = 0; i < 50; i++) {
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("DatabaseName", AttributeValue.builder().s("str" + i).build());
            key.put("Id", AttributeValue.builder().n(Integer.toString(i)).build());
            databaseKeys.add(key);
        }

        //putting that list in KeysAndAttributes Object
        KeysAndAttributes.Builder forDatabase = KeysAndAttributes.builder();
        forDatabase.keys(databaseKeys);
        //set projection expression for that item
        String projectionExprForDatabase = "Tags, Message";
        forDatabase.projectionExpression(projectionExprForDatabase);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put(tableName2, forDatabase.build());

        BatchGetItemRequest gI = BatchGetItemRequest.builder().requestItems(requestItems).build();
        try {
            dynamoDbClient.batchGetItem(gI);
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        try {
            phoenixDBClientV2.batchGetItem(gI);
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
    }

    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DatabaseName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Id", AttributeValue.builder().n("5").build());
        Map<String, AttributeValue> messageMap1 = new HashMap<>();
        messageMap1.put("Sender", AttributeValue.builder().s("Jake").build());
        messageMap1.put("Type", AttributeValue.builder().s("Positive Feedback").build());
        Map<String, AttributeValue> feedback = new HashMap<>();
        feedback.put("FeedbackDetails",
                AttributeValue.builder().l(AttributeValue.builder().m(messageMap1).build())
                        .build());
        item.put("Feedback", AttributeValue.builder().m(feedback).build());
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Subject", AttributeValue.builder().s("Concurrent Reads").build());
        item.put("Tags", AttributeValue.builder().s("Reads").build());
        item.put("Message", AttributeValue.builder()
                .s("How many users can read a single data item at a time? Are there any limits?")
                .build());
        item.put("Threads", AttributeValue.builder().n("12").build());
        item.put("Messages", AttributeValue.builder().n("55").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DatabaseName", AttributeValue.builder().s("Amazon RDS").build());
        item.put("Id", AttributeValue.builder().n("20").build());
        Map<String, AttributeValue> messageMap1 = new HashMap<>();
        messageMap1.put("Sender", AttributeValue.builder().s("Bob").build());
        messageMap1.put("Type", AttributeValue.builder().s("Neutral Feedback").build());
        Map<String, AttributeValue> feedback = new HashMap<>();
        feedback.put("FeedbackDetails",
                AttributeValue.builder().l(AttributeValue.builder().m(messageMap1).build())
                        .build());
        item.put("Feedback", AttributeValue.builder().m(feedback).build());
        item.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());
        item.put("Subject", AttributeValue.builder().s("Concurrent Reads").build());
        item.put("Tags", AttributeValue.builder().s("Writes").build());
        item.put("Message", AttributeValue.builder()
                .s("How many users can read multiple data item at a time? Are there any limits?")
                .build());
        item.put("Threads", AttributeValue.builder().n("8").build());
        item.put("Messages", AttributeValue.builder().n("32").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DatabaseName", AttributeValue.builder().s("Amazon DBS").build());
        item.put("Id", AttributeValue.builder().n("35").build());
        Map<String, AttributeValue> messageMap1 = new HashMap<>();
        messageMap1.put("Sender", AttributeValue.builder().s("Lynn").build());
        messageMap1.put("Type", AttributeValue.builder().s("Neutral Feedback").build());
        Map<String, AttributeValue> feedback = new HashMap<>();
        feedback.put("FeedbackDetails",
                AttributeValue.builder().l(AttributeValue.builder().m(messageMap1).build())
                        .build());
        item.put("Feedback", AttributeValue.builder().m(feedback).build());
        item.put("ForumName", AttributeValue.builder().s("Amazon DBS").build());
        item.put("Subject", AttributeValue.builder().s("Concurrent Reads").build());
        item.put("Tags", AttributeValue.builder().s("Writes").build());
        item.put("Message", AttributeValue.builder()
                .s("How many users can read multiple data item at a time? Are there any limits?")
                .build());
        item.put("Threads", AttributeValue.builder().n("8").build());
        item.put("Messages", AttributeValue.builder().n("32").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DatabaseName", AttributeValue.builder().s("Amazon Redshift").build());
        item.put("Id", AttributeValue.builder().n("25").build());
        Map<String, AttributeValue> messageMap1 = new HashMap<>();
        messageMap1.put("Sender", AttributeValue.builder().s("Fred").build());
        messageMap1.put("Type", AttributeValue.builder().s("Negative Feedback").build());
        Map<String, AttributeValue> feedback = new HashMap<>();
        feedback.put("FeedbackDetails",
                AttributeValue.builder().l(AttributeValue.builder().m(messageMap1).build())
                        .build());
        item.put("Feedback", AttributeValue.builder().m(feedback).build());
        item.put("ForumName", AttributeValue.builder().s("Amazon Redshift").build());
        item.put("Subject", AttributeValue.builder().s("Concurrent Writes").build());
        item.put("Tags", AttributeValue.builder().s("Writes").build());
        item.put("Message", AttributeValue.builder()
                .s("How many users can write multiple data item at a time? Are there any limits?")
                .build());
        item.put("Threads", AttributeValue.builder().n("12").build());
        item.put("Messages", AttributeValue.builder().n("55").build());
        return item;
    }
}
