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
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Test class for Scan API using legacy ScanFilter and ConditionalOperator parameters
 * instead of FilterExpression. This class mirrors the tests from org.apache.phoenix.ddb.ScanTableIT but uses
 * legacy parameters for backward compatibility testing.
 */
public class ScanTable2IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanTable2IT.class);

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
    public void testScanWithScanFilterAND() throws Exception {
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

        // Scan with ScanFilter using ConditionalOperator AND
        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // ScanFilter with multiple conditions
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr1", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("value1").build()).build());
        scanFilter.put("attr2", Condition.builder().comparisonOperator("GE")
                .attributeValueList(AttributeValue.builder().n("10").build()).build());
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
        Assert.assertEquals("Should find 1 item (attr1=value1 AND attr2>=10)", 1,
                phoenixResponse.count().intValue());
        Assert.assertEquals("Items should match between DynamoDB and Phoenix",
                dynamoResponse.items(), phoenixResponse.items());
    }

    @Test(timeout = 120000)
    public void testScanWithScanFilterOR() throws Exception {
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

        // Scan with ScanFilter using ConditionalOperator OR
        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // ScanFilter with OR conditions
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr1", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("special").build()).build());
        scanFilter.put("attr2", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("20").build()).build());
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
        Assert.assertEquals("Should find 2 items (attr1=special OR attr2>20)", 2,
                phoenixResponse.count().intValue());
        Assert.assertEquals("Items should match between DynamoDB and Phoenix",
                dynamoResponse.items(), phoenixResponse.items());
    }

    @Test(timeout = 120000)
    public void testScanWithTopLevelAttributeFilterUsingScanFilter() {
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Use ScanFilter instead of filterExpression "#2 = :v2"
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("title", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("Title3").build()).build());
        sr.scanFilter(scanFilter);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithSimpleNumericFiltersUsingScanFilter() {
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test GT (Greater Than) operator
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr_1", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("2").build()).build());
        sr.scanFilter(scanFilter);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithBetweenOperatorUsingScanFilter() {
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test BETWEEN operator
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr_1", Condition.builder().comparisonOperator("BETWEEN")
                .attributeValueList(AttributeValue.builder().n("1").build(),
                        AttributeValue.builder().n("3").build()).build());
        sr.scanFilter(scanFilter);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithInOperatorUsingScanFilter() {
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test IN operator 
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr_0", Condition.builder().comparisonOperator("IN")
                .attributeValueList(AttributeValue.builder().s("A").build(),
                        AttributeValue.builder().s("C").build()).build());
        sr.scanFilter(scanFilter);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithMultipleConditionsORUsingScanFilter() {
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test multiple conditions with OR operator
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr_0", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("A").build()).build());
        scanFilter.put("attr_1", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("3").build()).build());
        sr.scanFilter(scanFilter);
        sr.conditionalOperator("OR"); // Either condition can match

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithStringComparisonUsingScanFilter() {
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test LT (Less Than) with strings - lexicographical comparison
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr_0", Condition.builder().comparisonOperator("LT")
                .attributeValueList(AttributeValue.builder().s("C").build()).build());
        sr.scanFilter(scanFilter);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithNEOperatorUsingScanFilter() {
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test NE (Not Equal) operator
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr_0", Condition.builder().comparisonOperator("NE")
                .attributeValueList(AttributeValue.builder().s("B").build()).build());
        sr.scanFilter(scanFilter);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithComplexScanFilterAndPagination() {
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Use ScanFilter with OR operator instead of filterExpression "(#4 > :v4 AND #5 < :v5) OR #1.#2[0].#3 = :v2"
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("Id1", Condition.builder().comparisonOperator("GT")
                .attributeValueList(AttributeValue.builder().n("-20").build()).build());
        scanFilter.put("Id2", Condition.builder().comparisonOperator("LT")
                .attributeValueList(AttributeValue.builder().n("200").build()).build());
        sr.scanFilter(scanFilter);
        sr.conditionalOperator("AND"); // Both conditions must match
        sr.limit(1);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithBeginsWithOperatorUsingScanFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Create test items with string attributes for begins_with testing
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("item1").build());
        item1.put("attr_1", AttributeValue.builder().n("1").build());
        item1.put("stringAttr", AttributeValue.builder().s("prefix_match").build());
        item1.put("category", AttributeValue.builder().s("test").build());
        PutItemRequest putRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putRequest1);
        dynamoDbClient.putItem(putRequest1);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("item2").build());
        item2.put("attr_1", AttributeValue.builder().n("2").build());
        item2.put("stringAttr", AttributeValue.builder().s("prefix_another").build());
        item2.put("category", AttributeValue.builder().s("production").build());
        PutItemRequest putRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(putRequest2);
        dynamoDbClient.putItem(putRequest2);

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("attr_0", AttributeValue.builder().s("item3").build());
        item3.put("attr_1", AttributeValue.builder().n("3").build());
        item3.put("stringAttr", AttributeValue.builder().s("different_value").build());
        item3.put("category", AttributeValue.builder().s("test").build());
        PutItemRequest putRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();
        phoenixDBClientV2.putItem(putRequest3);
        dynamoDbClient.putItem(putRequest3);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test BEGINS_WITH operator - equivalent to begins_with() function
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("stringAttr", Condition.builder().comparisonOperator("BEGINS_WITH")
                .attributeValueList(AttributeValue.builder().s("prefix_").build()).build());
        sr.scanFilter(scanFilter);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithBeginsWithAndConditionUsingScanFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Create test items
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("item1").build());
        item1.put("attr_1", AttributeValue.builder().n("1").build());
        item1.put("stringAttr", AttributeValue.builder().s("prefix_match").build());
        item1.put("category", AttributeValue.builder().s("test").build());
        PutItemRequest putRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putRequest1);
        dynamoDbClient.putItem(putRequest1);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("item2").build());
        item2.put("attr_1", AttributeValue.builder().n("2").build());
        item2.put("stringAttr", AttributeValue.builder().s("prefix_another").build());
        item2.put("category", AttributeValue.builder().s("production").build());
        PutItemRequest putRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(putRequest2);
        dynamoDbClient.putItem(putRequest2);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test BEGINS_WITH combined with AND condition
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("stringAttr", Condition.builder().comparisonOperator("BEGINS_WITH")
                .attributeValueList(AttributeValue.builder().s("prefix_").build()).build());
        scanFilter.put("category", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("test").build()).build());
        sr.scanFilter(scanFilter);
        sr.conditionalOperator("AND"); // Both conditions must match

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithContainsOperatorUsingScanFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Create test items with different data types for contains testing
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("item1").build());
        item1.put("attr_1", AttributeValue.builder().n("1").build());
        item1.put("stringAttr", AttributeValue.builder().s("hello world").build());
        item1.put("stringSet", AttributeValue.builder().ss("apple", "banana", "cherry").build());
        item1.put("numberSet", AttributeValue.builder().ns("1", "2", "3").build());
        PutItemRequest putRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putRequest1);
        dynamoDbClient.putItem(putRequest1);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("item2").build());
        item2.put("attr_1", AttributeValue.builder().n("2").build());
        item2.put("stringAttr", AttributeValue.builder().s("goodbye universe").build());
        item2.put("stringSet", AttributeValue.builder().ss("orange", "grape").build());
        item2.put("numberSet", AttributeValue.builder().ns("4", "5").build());
        PutItemRequest putRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(putRequest2);
        dynamoDbClient.putItem(putRequest2);

        // Test 1: CONTAINS with string attribute - substring search
        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        Map<String, Condition> scanFilter1 = new HashMap<>();
        scanFilter1.put("stringAttr", Condition.builder().comparisonOperator("CONTAINS")
                .attributeValueList(AttributeValue.builder().s("world").build()).build());
        sr1.scanFilter(scanFilter1);
        assertScanResults(sr1);

        // Test 2: CONTAINS with String Set - element search
        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        Map<String, Condition> scanFilter2 = new HashMap<>();
        scanFilter2.put("stringSet", Condition.builder().comparisonOperator("CONTAINS")
                .attributeValueList(AttributeValue.builder().s("apple").build()).build());
        sr2.scanFilter(scanFilter2);
        assertScanResults(sr2);

        // Test 3: CONTAINS with Number Set - element search
        ScanRequest.Builder sr3 = ScanRequest.builder().tableName(tableName);
        Map<String, Condition> scanFilter3 = new HashMap<>();
        scanFilter3.put("numberSet", Condition.builder().comparisonOperator("CONTAINS")
                .attributeValueList(AttributeValue.builder().n("1").build()).build());
        sr3.scanFilter(scanFilter3);
        assertScanResults(sr3);
    }

    @Test(timeout = 120000)
    public void testScanWithNotContainsOperatorUsingScanFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Create test items
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("item1").build());
        item1.put("attr_1", AttributeValue.builder().n("1").build());
        item1.put("stringAttr", AttributeValue.builder().s("hello world").build());
        item1.put("stringSet", AttributeValue.builder().ss("apple", "banana").build());
        PutItemRequest putRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putRequest1);
        dynamoDbClient.putItem(putRequest1);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("item2").build());
        item2.put("attr_1", AttributeValue.builder().n("2").build());
        item2.put("stringAttr", AttributeValue.builder().s("goodbye universe").build());
        item2.put("stringSet", AttributeValue.builder().ss("orange", "grape").build());
        PutItemRequest putRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(putRequest2);
        dynamoDbClient.putItem(putRequest2);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // Test NOT_CONTAINS operator - items that do NOT contain the specified value
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("stringAttr", Condition.builder().comparisonOperator("NOT_CONTAINS")
                .attributeValueList(AttributeValue.builder().s("world").build()).build());
        sr.scanFilter(scanFilter);

        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithNullAndNotNullOperatorsUsingScanFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S,
                        "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Create test items - some with optional attributes, some without
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", AttributeValue.builder().s("item1").build());
        item1.put("attr_1", AttributeValue.builder().n("1").build());
        item1.put("optionalAttr", AttributeValue.builder().s("exists").build());
        PutItemRequest putRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putRequest1);
        dynamoDbClient.putItem(putRequest1);

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", AttributeValue.builder().s("item2").build());
        item2.put("attr_1", AttributeValue.builder().n("2").build());
        // No optionalAttr - this item should match NULL condition
        PutItemRequest putRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(putRequest2);
        dynamoDbClient.putItem(putRequest2);

        // Test 1: NOT_NULL operator - equivalent to attribute_exists()
        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        Map<String, Condition> scanFilter1 = new HashMap<>();
        scanFilter1.put("optionalAttr", Condition.builder().comparisonOperator("NOT_NULL").build());
        sr1.scanFilter(scanFilter1);
        assertScanResults(sr1);

        // Test 2: NULL operator - equivalent to NOT attribute_exists()
        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        Map<String, Condition> scanFilter2 = new HashMap<>();
        scanFilter2.put("optionalAttr", Condition.builder().comparisonOperator("NULL").build());
        sr2.scanFilter(scanFilter2);
        assertScanResults(sr2);
    }

    @Test(timeout = 120000)
    public void testScanFilterAndFilterExpressionValidationError() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Scan request with both ScanFilter and FilterExpression - should fail
        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);

        // ScanFilter (legacy)
        Map<String, Condition> scanFilter = new HashMap<>();
        scanFilter.put("attr1", Condition.builder().comparisonOperator("EQ")
                .attributeValueList(AttributeValue.builder().s("value").build()).build());
        sr.scanFilter(scanFilter);

        // FilterExpression (modern)
        sr.filterExpression("attr2 = :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", AttributeValue.builder().s("test").build());
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

    private void assertScanResults(ScanRequest.Builder sr) {
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());

        // Compare counts
        Assert.assertEquals("Count should match between DynamoDB and Phoenix", dynamoResult.count(),
                phoenixResult.count());
        Assert.assertEquals("Scanned count should match between DynamoDB and Phoenix",
                dynamoResult.scannedCount(), phoenixResult.scannedCount());

        // Check that results are not empty when expected
        if (dynamoResult.count() > 0) {
            Assert.assertTrue("Phoenix should return items when DynamoDB does",
                    phoenixResult.hasItems() && phoenixResult.items().size() > 0);
        }
    }

    // Utility methods for creating test items - mirror those from org.apache.phoenix.ddb.ScanTableIT
    private static Map<String, AttributeValue> getItem1() {
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
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
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
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("C").build());
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
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("D").build());
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
        return item;
    }
}
