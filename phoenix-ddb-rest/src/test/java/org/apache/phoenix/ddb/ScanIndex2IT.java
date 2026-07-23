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

import org.apache.phoenix.jdbc.PhoenixTestDriver;
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
import software.amazon.awssdk.services.dynamodb.model.Condition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

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
 * Test class for Scan API with indexes using legacy ScanFilter and ConditionalOperator parameters
 * instead of FilterExpression. This class mirrors the tests from org.apache.phoenix.ddb.ScanIndexIT but uses
 * legacy parameters for backward compatibility testing.
 */
public class ScanIndex2IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanIndex2IT.class);

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
    public void testScanIndexWithScanFilterAND() throws SQLException {
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

        // Scan with index using ScanFilter with AND operator
        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.indexName(indexName);

        // ScanFilter with multiple conditions using AND
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("Attr_0", Condition.builder().comparisonOperator("BETWEEN")
                .attributeValueList(AttributeValue.builder().s("str_val_0").build(),
                        AttributeValue.builder().s("str_val_2").build()).build());
        scanFilter.put("num", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("100").build()).build());
        sr.scanFilter(scanFilter);
        sr.conditionalOperator("AND"); // Both conditions must match

        ScanRequest scanRequest = sr.build();

        // Execute scan on both DynamoDB and Phoenix
        ScanResponse phoenixResponse = phoenixDBClientV2.scan(scanRequest);
        ScanResponse dynamoResponse = dynamoDbClient.scan(scanRequest);

        // Compare results
        Assert.assertNotNull("Phoenix response should not be null", phoenixResponse);
        Assert.assertNotNull("DynamoDB response should not be null", dynamoResponse);
        Assert.assertEquals("Count should match between DynamoDB and Phoenix",
                dynamoResponse.count(), phoenixResponse.count());
        Assert.assertEquals("Items should match between DynamoDB and Phoenix",
                dynamoResponse.items(), phoenixResponse.items());
    }

    @Test(timeout = 120000)
    public void testScanIndexWithScanFilterOR() throws SQLException {
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

        // Scan with index using ScanFilter with OR operator
        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.indexName(indexName);

        // ScanFilter with OR conditions
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("Title", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("hello").build()).build());
        scanFilter.put("num", Condition.builder().comparisonOperator("LT")
                .attributeValueList(AttributeValue.builder().n("50").build()).build());
        sr.scanFilter(scanFilter);
        sr.conditionalOperator("OR"); // Either condition can match

        ScanRequest scanRequest = sr.build();

        // Execute scan on both DynamoDB and Phoenix
        ScanResponse phoenixResponse = phoenixDBClientV2.scan(scanRequest);
        ScanResponse dynamoResponse = dynamoDbClient.scan(scanRequest);

        // Compare results
        Assert.assertNotNull("Phoenix response should not be null", phoenixResponse);
        Assert.assertNotNull("DynamoDB response should not be null", dynamoResponse);
        Assert.assertEquals("Count should match between DynamoDB and Phoenix",
                dynamoResponse.count(), phoenixResponse.count());
        Assert.assertEquals("Items should match between DynamoDB and Phoenix",
                dynamoResponse.items(), phoenixResponse.items());
    }

    @Test(timeout = 120000)
    public void testScanIndexFilterAndFilterExpressionValidationError() throws Exception {
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

        TestUtils.waitForEventualConsistentIndex();

        // Scan request with both ScanFilter and FilterExpression - should fail
        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.indexName(indexName);

        // ScanFilter (legacy)
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("Title", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("test").build()).build());
        sr.scanFilter(scanFilter);

        // FilterExpression (modern)
        sr.filterExpression("num > :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", AttributeValue.builder().n("100").build());
        sr.expressionAttributeValues(exprAttrVal);

        ScanRequest scanRequest = sr.build();

        // Test Phoenix error
        try {
            phoenixDBClientV2.scan(scanRequest);
            Assert.fail("Should have thrown an exception");
        } catch (DynamoDbException e) {
            Assert.assertEquals("Status code should be 400 for validation error", 400,
                    e.statusCode());
        }

        // Test DynamoDB error
        try {
            dynamoDbClient.scan(scanRequest);
            Assert.fail("Should have thrown an exception");
        } catch (DynamoDbException e) {
            Assert.assertEquals("Status code should be 400 for validation error", 400,
                    e.statusCode());
        }
    }

    // Utility methods for creating test items
    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Title", AttributeValue.builder().s("hello").build());
        item.put("num", AttributeValue.builder().n("42").build());
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("123.45").build());
        return item;
    }

    private Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Title", AttributeValue.builder().s("world").build());
        item.put("num", AttributeValue.builder().n("142").build());
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("453.23").build());
        return item;
    }

    private Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Title", AttributeValue.builder().s("test").build());
        item.put("num", AttributeValue.builder().n("242").build());
        item.put("Attr_0", AttributeValue.builder().s("str_val_2").build());
        item.put("attr_1", AttributeValue.builder().n("753.89").build());
        return item;
    }
}
