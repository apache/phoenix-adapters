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
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class QueryIndex4IT {

    private static final String TABLE_NAME = "QueryIndex4IT_Table";
    private static final String INDEX_NAME = "gsi_QueryIndex4IT";
    private static final int NUM_RECORDS = 20000;

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
                "category", ScalarAttributeType.S, "score", ScalarAttributeType.N);
        
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        for (int i = 0; i < NUM_RECORDS; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk", AttributeValue.builder().s("pk_" + (i % 100)).build());
            item.put("sk", AttributeValue.builder().n(String.valueOf(i)).build());
            item.put("category", AttributeValue.builder().s("category_" + (i % 10)).build());
            item.put("score", AttributeValue.builder().n(String.valueOf(i)).build());
            item.put("price", AttributeValue.builder().n(String.valueOf((i % 1000) + 1)).build());
            item.put("active", AttributeValue.builder().bool(i % 2 == 0).build());
            item.put("name", AttributeValue.builder().s("item_" + i).build());
            item.put("tags", AttributeValue.builder().ss("tag_" + (i % 5), "common_tag").build());
            item.put("counts", AttributeValue.builder().ns(String.valueOf(i % 10), String.valueOf((i % 20) + 100)).build());
            item.put("bytes", AttributeValue.builder().bs(
                    software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[]{(byte) (i % 128), 0x01}),
                    software.amazon.awssdk.core.SdkBytes.fromByteArray(new byte[]{0x02, 0x03})).build());
            
            Map<String, AttributeValue> nestedMap = new HashMap<>();
            nestedMap.put("field1", AttributeValue.builder().s("nested_" + (i % 3)).build());
            nestedMap.put("field2", AttributeValue.builder().n(String.valueOf(i % 100)).build());
            item.put("metadata", AttributeValue.builder().m(nestedMap).build());
            
            List<AttributeValue> nestedList = new ArrayList<>();
            nestedList.add(AttributeValue.builder().s("item_" + (i % 7)).build());
            nestedList.add(AttributeValue.builder().n(String.valueOf(i % 50)).build());
            nestedList.add(AttributeValue.builder().ss("nested_tag_" + (i % 4), "nested_common").build());
            item.put("attributes", AttributeValue.builder().l(nestedList).build());
            
            PutItemRequest putRequest = PutItemRequest.builder().tableName(TABLE_NAME).item(item).build();
            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);
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
    public void testEqualityKeyCondition() throws Exception {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_5").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testRangeKeyCondition() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat AND score > :minScore");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_3").build());
        exprAttrVal.put(":minScore", AttributeValue.builder().n("5000").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testBetweenKeyCondition() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat AND score BETWEEN :low AND :high");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_7").build());
        exprAttrVal.put(":low", AttributeValue.builder().n("2000").build());
        exprAttrVal.put(":high", AttributeValue.builder().n("8000").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testLimit() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat");
        qr.limit(50);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_4").build());
        qr.expressionAttributeValues(exprAttrVal);

        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertTrue(ItemComparator.areItemsEqual(dynamoResult.items(), phoenixResult.items()));
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testPagination() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat AND score >= :minScore");
        qr.limit(100);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_1").build());
        exprAttrVal.put(":minScore", AttributeValue.builder().n("0").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testScanIndexForward() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat");
        qr.scanIndexForward(false);
        qr.limit(100);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_6").build());
        qr.expressionAttributeValues(exprAttrVal);

        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());

        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertTrue(ItemComparator.areItemsEqual(dynamoResult.items(), phoenixResult.items()));
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testProjection() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat");
        qr.projectionExpression("pk, score, price");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_8").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testComplexFilterExpression() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat AND score < :maxScore");
        qr.filterExpression("(price BETWEEN :minPrice AND :maxPrice) OR (active = :isActive AND #itemName <> :excludeName)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#itemName", "name");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_9").build());
        exprAttrVal.put(":maxScore", AttributeValue.builder().n("7000").build());
        exprAttrVal.put(":minPrice", AttributeValue.builder().n("100").build());
        exprAttrVal.put(":maxPrice", AttributeValue.builder().n("900").build());
        exprAttrVal.put(":isActive", AttributeValue.builder().bool(false).build());
        exprAttrVal.put(":excludeName", AttributeValue.builder().s("item_100").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testLessThanEqual() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat AND score <= :maxScore");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_0").build());
        exprAttrVal.put(":maxScore", AttributeValue.builder().n("5000").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testFilterExpression1() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat");
        qr.filterExpression("price > :minPrice AND active = :isActive");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_2").build());
        exprAttrVal.put(":minPrice", AttributeValue.builder().n("500").build());
        exprAttrVal.put(":isActive", AttributeValue.builder().bool(true).build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testFilterExpression2() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat AND score > :minScore");
        qr.filterExpression("price < :maxPrice AND active = :isActive");
        qr.limit(200);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_3").build());
        exprAttrVal.put(":minScore", AttributeValue.builder().n("3000").build());
        exprAttrVal.put(":maxPrice", AttributeValue.builder().n("600").build());
        exprAttrVal.put(":isActive", AttributeValue.builder().bool(true).build());
        qr.expressionAttributeValues(exprAttrVal);

        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());

        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertTrue(ItemComparator.areItemsEqual(dynamoResult.items(), phoenixResult.items()));
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testFilterOnSetsAndNestedMap() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat");
        qr.filterExpression("contains(tags, :tag) AND metadata.field1 = :nestedVal");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_2").build());
        exprAttrVal.put(":tag", AttributeValue.builder().s("tag_1").build());
        exprAttrVal.put(":nestedVal", AttributeValue.builder().s("nested_1").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 300000)
    public void testFilterOnNestedListAndSets() throws SQLException {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME).indexName(INDEX_NAME);
        qr.keyConditionExpression("category = :cat AND score < :maxScore");
        qr.filterExpression("contains(counts, :count) AND size(#attributes) >= :minSize");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attributes", "attributes");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":cat", AttributeValue.builder().s("category_7").build());
        exprAttrVal.put(":maxScore", AttributeValue.builder().n("15000").build());
        exprAttrVal.put(":count", AttributeValue.builder().n("105").build());
        exprAttrVal.put(":minSize", AttributeValue.builder().n("2").build());
        qr.expressionAttributeValues(exprAttrVal);

        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        TestUtils.validateIndexUsed(qr.build(), url);
    }
}

