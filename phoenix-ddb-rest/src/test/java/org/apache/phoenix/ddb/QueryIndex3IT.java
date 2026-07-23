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
import java.sql.SQLException;
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
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests for index functionality with other QueryRequest parameters using legacy KeyConditions -
 * lastEvaluatedKey, scanIndexForward, filterExpression, and limit.
 * This class mirrors tests from org.apache.phoenix.ddb.QueryIndex2IT but uses KeyConditions instead of KeyConditionExpression.
 */
public class QueryIndex3IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIndex3IT.class);

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
        DriverManager.registerDriver(new PhoenixTestDriver());

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
    public void testPaginationWithLastKeyWithKeyConditions() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName();
        final String indexName = "G_IdX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title", ScalarAttributeType.S, "num",
                        ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        TestUtils.waitForEventualConsistentIndex();

        //query request using index with KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // Attr_0 = "str_val_1" AND attr_1 < 453.23 (KeyConditions always use AND logic)
        keyConditions.put("Attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("str_val_1").build()).build());

        keyConditions.put("attr_1", Condition.builder().comparisonOperator("LT")
                .attributeValueList(AttributeValue.builder().n("453.23").build()).build());

        qr.keyConditions(keyConditions);
        qr.limit(1);

        // query result
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

        // explain plan
        //org.apache.phoenix.ddb.TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testFilterExpressionWithIndexWithKeyConditions() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName();
        final String indexName = "g_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title", ScalarAttributeType.S, "num",
                        ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        TestUtils.waitForEventualConsistentIndex();

        //query request using index and filter with KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // Attr_0 = "str_val_1"
        keyConditions.put("Attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("str_val_1").build()).build());

        // attr_1 < 453.23
        keyConditions.put("attr_1", Condition.builder().comparisonOperator("LT")
                .attributeValueList(AttributeValue.builder().n("453.23").build()).build());

        qr.keyConditions(keyConditions);

        // Use QueryFilter instead of FilterExpression (legacy parameter)
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put("Title", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("hello").build()).build());
        qr.queryFilter(queryFilter);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // explain plan
        //org.apache.phoenix.ddb.TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testQueryFilterWithIndexAndConditionalOperatorOR() throws SQLException {
        final String tableName = testName.getMethodName();
        final String indexName = "G_iDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title", ScalarAttributeType.S, "num",
                        ScalarAttributeType.N);
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Put test items
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);

        TestUtils.waitForEventualConsistentIndex();

        // Query with index using KeyConditions and QueryFilter with OR operator
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);

        // KeyConditions for index
        Map<String, Condition> keyConditions = new HashMap<>();
        keyConditions.put("Attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("str_val_1").build()).build());
        qr.keyConditions(keyConditions);

        // QueryFilter with OR conditions
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put("Title", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("hello").build()).build());
        queryFilter.put("num", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("100").build()).build());
        qr.queryFilter(queryFilter);
        qr.conditionalOperator("OR"); // Either condition can match

        // Execute query
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());
    }

    @Test(timeout = 120000)
    public void testScanIndexForwardWithKeyConditions() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName();
        final String indexName = "G_iDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title", ScalarAttributeType.S, "num",
                        ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        TestUtils.waitForEventualConsistentIndex();

        //query request using index and filter with KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // Attr_0 = "str_val_1"
        keyConditions.put("Attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("str_val_1").build()).build());

        qr.keyConditions(keyConditions);
        qr.scanIndexForward(false);

        // query result, should return 3 items in descending order of sort key
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (int i = 0; i < phoenixResult.items().size() - 1; i++) {
            double sortKeyVal1 = Double.parseDouble(phoenixResult.items().get(i).get("attr_1").n());
            double sortKeyVal2 =
                    Double.parseDouble(phoenixResult.items().get(i + 1).get("attr_1").n());
            Assert.assertTrue(sortKeyVal1 >= sortKeyVal2);
        }

        // explain plan
        //org.apache.phoenix.ddb.TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testIndexBeginsWithWithKeyConditions() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName();
        final String indexName = "G_IDx_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_1", ScalarAttributeType.N, null,
                        null);
        // create index on attr_0, attr_1
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                        ScalarAttributeType.S, "Title", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        TestUtils.waitForEventualConsistentIndex();

        //query request using index and filter with KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // Attr_0 = "str_val_1"
        keyConditions.put("Attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("str_val_1").build()).build());

        // Title BEGINS_WITH "he"
        keyConditions.put("Title", Condition.builder().comparisonOperator("BEGINS_WITH")
                .attributeValueList(AttributeValue.builder().s("he").build()).build());

        qr.keyConditions(keyConditions);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // explain plan
        //org.apache.phoenix.ddb.TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testIndexBetweenWithKeyConditions() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName();
        final String indexName = "G_iDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title", ScalarAttributeType.S, "num",
                        ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        TestUtils.waitForEventualConsistentIndex();

        //query request using index and filter with KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // Attr_0 = "str_val_1"
        keyConditions.put("Attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("str_val_1").build()).build());

        // attr_1 BETWEEN 0.0001 AND 30.121
        keyConditions.put("attr_1", Condition.builder().comparisonOperator("BETWEEN")
                .attributeValueList(AttributeValue.builder().n("0.0001").build(),
                        AttributeValue.builder().n("30.121").build()).build());

        qr.keyConditions(keyConditions);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // explain plan
        //org.apache.phoenix.ddb.TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testProjectionScanIndexForwardFilterLimitWithKeyConditions() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title", ScalarAttributeType.S, "num",
                        ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        TestUtils.waitForEventualConsistentIndex();

        //query request using index and filter with KeyConditions
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        Map<String, Condition> keyConditions = new HashMap<>();

        // Attr_0 = "str_val_1"
        keyConditions.put("Attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("str_val_1").build()).build());

        // attr_1 > 0.0
        keyConditions.put("attr_1", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("0.0").build()).build());

        qr.keyConditions(keyConditions);

        // Use QueryFilter instead of FilterExpression (legacy parameter)
        Map<String, Condition> queryFilter = new HashMap<>();
        queryFilter.put("num", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("1.01").build()).build());
        qr.queryFilter(queryFilter);

        //        qr.projectionExpression("Title");
        qr.scanIndexForward(false);
        qr.limit(1);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // check last Key
        Map<String, AttributeValue> lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals(getItem1(), lastKey);

        // explain plan
        //org.apache.phoenix.ddb.TestUtils.validateIndexUsed(qr.build(), url);
    }

    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("11295.03").build());
        item.put("Title", AttributeValue.builder().s("hi").build());
        item.put("num", AttributeValue.builder().n("1.1").build());
        return item;
    }

    private Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("30.12").build());
        item.put("Title", AttributeValue.builder().s("hello").build());
        item.put("num", AttributeValue.builder().n("1.2").build());
        return item;
    }

    private Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("-21.06").build());
        item.put("Title", AttributeValue.builder().s("hey").build());
        item.put("num", AttributeValue.builder().n("1.3").build());
        return item;
    }

    private Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("0.1").build());
        item.put("Title", AttributeValue.builder().s("hie").build());
        item.put("num", AttributeValue.builder().n("-0.8").build());
        return item;
    }

    /**
     * Helper method to consolidate repetitive assertions for comparing query results
     * between DynamoDB and Phoenix.
     *
     * @param dynamoResult  The query result from DynamoDB
     * @param phoenixResult The query result from Phoenix
     * @param expectedCount Optional expected count (can be null to skip count assertion)
     */
    private void assertQueryResultsEqual(QueryResponse dynamoResult, QueryResponse phoenixResult,
            Integer expectedCount) {
        if (expectedCount != null) {
            Assert.assertEquals(expectedCount.intValue(), dynamoResult.count().intValue());
        }
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
        Assert.assertTrue(
                ItemComparator.areItemsEqual(dynamoResult.items(), phoenixResult.items()));
    }

    /**
     * Helper method that executes queries on both DynamoDB and Phoenix clients and
     * then performs the standard assertions.
     *
     * @param queryRequest  The query request to execute
     * @param expectedCount Optional expected count (can be null to skip count assertion)
     */
    private void executeAndAssertQuery(QueryRequest queryRequest, Integer expectedCount) {
        QueryResponse phoenixResult = phoenixDBClientV2.query(queryRequest);
        QueryResponse dynamoResult = dynamoDbClient.query(queryRequest);
        assertQueryResultsEqual(dynamoResult, phoenixResult, expectedCount);
    }

}
