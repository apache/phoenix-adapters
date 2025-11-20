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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ScanIndex3IT {

    private static final String TABLE_NAME = "ScanIndex3IT_Table";
    private static final String INDEX_NAME = "gsi_ScanIndex3IT";
    private static final int NUM_RECORDS = 18000;

    private static DynamoDbClient dynamoDbClient;
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

        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();

        createTableAndInsertData();
    }

    private static void createTableAndInsertData() {
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(
                TABLE_NAME, "pk", ScalarAttributeType.S, "sk", ScalarAttributeType.N);
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, INDEX_NAME,
                "status", ScalarAttributeType.S, "timestamp", ScalarAttributeType.N);

        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        for (int i = 0; i < NUM_RECORDS; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk", AttributeValue.builder().s("pk_" + (i % 100)).build());
            item.put("sk", AttributeValue.builder().n(String.valueOf(i)).build());
            item.put("status", AttributeValue.builder().s("status_" + (i % 10)).build());
            item.put("timestamp", AttributeValue.builder().n(String.valueOf(i)).build());
            item.put("amount", AttributeValue.builder().n(String.valueOf((i % 1000) + 1)).build());
            item.put("priority", AttributeValue.builder().n(String.valueOf(i % 10)).build());
            item.put("active", AttributeValue.builder().bool(i % 2 == 0).build());
            item.put("description", AttributeValue.builder().s("description_" + i).build());
            item.put("labels", AttributeValue.builder().ss("label_" + (i % 6), "shared_label").build());
            item.put("scores", AttributeValue.builder().ns(String.valueOf(i % 15), String.valueOf((i % 25) + 100)).build());
            item.put("data", AttributeValue.builder().bs(
                    software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[]{(byte) (i % 64), 0x01}),
                    software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[]{0x0A, 0x0B})).build());
            
            if (i % 5 == 0) {
                item.put("optional_field", AttributeValue.builder().s("optional_" + i).build());
            }
            
            Map<String, AttributeValue> nestedMap = new HashMap<>();
            nestedMap.put("key1", AttributeValue.builder().s("value_" + (i % 4)).build());
            nestedMap.put("key2", AttributeValue.builder().n(String.valueOf(i % 80)).build());
            item.put("config", AttributeValue.builder().m(nestedMap).build());
            
            List<AttributeValue> nestedList = new ArrayList<>();
            nestedList.add(AttributeValue.builder().s("list_item_" + (i % 8)).build());
            nestedList.add(AttributeValue.builder().n(String.valueOf(i % 40)).build());
            nestedList.add(AttributeValue.builder().ss("set_in_list_" + (i % 3), "list_common").build());
            item.put("itms", AttributeValue.builder().l(nestedList).build());
            
            PutItemRequest putRequest = PutItemRequest.builder().tableName(TABLE_NAME).item(item).build();
            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);
        }

        for (int i = 0; i < NUM_RECORDS; i++) {
            if (i % 4 == 0) {
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("pk", AttributeValue.builder().s("pk_" + (i % 100)).build());
                key.put("sk", AttributeValue.builder().n(String.valueOf(i)).build());
                DeleteItemRequest deleteRequest =
                        DeleteItemRequest.builder().tableName(TABLE_NAME).key(key).build();
                phoenixDBClientV2.deleteItem(deleteRequest);
                dynamoDbClient.deleteItem(deleteRequest);
            } else if (i % 11 == 0) {
                Map<String, AttributeValue> key = new HashMap<>();
                key.put("pk", AttributeValue.builder().s("pk_" + (i % 100)).build());
                key.put("sk", AttributeValue.builder().n(String.valueOf(i)).build());
                final String updateExpression;
                if (i % 33 == 0) {
                    updateExpression = "REMOVE #st, #ts";
                } else if (i % 22 == 0) {
                    updateExpression = "REMOVE #ts";
                } else {
                    updateExpression = "REMOVE #st";
                }
                Map<String, String> exprAttrNames = new HashMap<>();
                if (updateExpression.contains("#st")) {
                    exprAttrNames.put("#st", "status");
                }
                if (updateExpression.contains("#ts")) {
                    exprAttrNames.put("#ts", "timestamp");
                }
                UpdateItemRequest updateRequest =
                        UpdateItemRequest.builder().tableName(TABLE_NAME).key(key)
                                .updateExpression(updateExpression)
                                .expressionAttributeNames(exprAttrNames).build();
                phoenixDBClientV2.updateItem(updateRequest);
                dynamoDbClient.updateItem(updateRequest);
            }
        }
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

    @Test(timeout = 300000)
    public void testFullTableScan() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testFilterExpression() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("amount > :minAmount AND active = :isActive");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":minAmount", AttributeValue.builder().n("500").build());
        exprAttrVal.put(":isActive", AttributeValue.builder().bool(true).build());
        sr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testLimit() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.limit(75);

        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());

        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertTrue(ItemComparator.areItemsEqual(
                TestUtils.sortItemsByPartitionAndSortKey(dynamoResult.items(), "pk", "sk"),
                TestUtils.sortItemsByPartitionAndSortKey(phoenixResult.items(), "pk", "sk")));
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testPagination() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.limit(150);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testProjection() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.projectionExpression("pk, #status, amount");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#status", "status");
        sr.expressionAttributeNames(exprAttrNames);
        sr.limit(50);

        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());

        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testComplexFilterExpression1() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("(amount BETWEEN :minAmt AND :maxAmt) OR (active = :isActive AND priority > :minPriority)");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":minAmt", AttributeValue.builder().n("200").build());
        exprAttrVal.put(":maxAmt", AttributeValue.builder().n("800").build());
        exprAttrVal.put(":isActive", AttributeValue.builder().bool(false).build());
        exprAttrVal.put(":minPriority", AttributeValue.builder().n("7").build());
        sr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testAttributeExists() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("attribute_exists(optional_field)");

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testAttributeNotExists() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("attribute_not_exists(optional_field)");

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testComplexFilterExpression2() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("amount > :minAmt AND priority >= :minPri AND active = :isActive");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":minAmt", AttributeValue.builder().n("600").build());
        exprAttrVal.put(":minPri", AttributeValue.builder().n("5").build());
        exprAttrVal.put(":isActive", AttributeValue.builder().bool(true).build());
        sr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testNotCondition() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("NOT (#status = :excludeStatus)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#status", "status");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":excludeStatus", AttributeValue.builder().s("status_0").build());
        sr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testInCondition() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("#status IN (:status1, :status2, :status3)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#status", "status");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":status1", AttributeValue.builder().s("status_1").build());
        exprAttrVal.put(":status2", AttributeValue.builder().s("status_3").build());
        exprAttrVal.put(":status3", AttributeValue.builder().s("status_7").build());
        sr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testFilterOnSetsAndNestedMap() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("contains(labels, :label) AND config.key1 = :configVal");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":label", AttributeValue.builder().s("label_3").build());
        exprAttrVal.put(":configVal", AttributeValue.builder().s("value_2").build());
        sr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 300000)
    public void testFilterOnNestedListAndMultipleSets() throws SQLException {
        ScanRequest.Builder sr = ScanRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        sr.filterExpression("contains(scores, :score) AND size(itms) = :listSize AND contains(labels, :label)");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":score", AttributeValue.builder().n("110").build());
        exprAttrVal.put(":listSize", AttributeValue.builder().n("3").build());
        exprAttrVal.put(":label", AttributeValue.builder().s("shared_label").build());
        sr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient, "pk", "sk",
                ScalarAttributeType.S, ScalarAttributeType.N);
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }
}

