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

import java.sql.DriverManager;
import java.util.Arrays;
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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.Select;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ScanTableIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanTableIT.class);

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
    public void testScanSelectCount() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.filterExpression("#2 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "title");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().s("Title3").build());
        sr.expressionAttributeValues(exprAttrVal);
        sr.select("COUNT");
        
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(1, phoenixResult.count().intValue());
        Assert.assertTrue(phoenixResult.items().isEmpty());
        Assert.assertTrue(dynamoResult.items().isEmpty());
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void testScanSelectCountWithPagination() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.select("COUNT");
        sr.limit(1);

        int phoenixCount = 0;
        ScanResponse phoenixResult;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            Assert.assertTrue(phoenixResult.items().isEmpty());
            phoenixCount += phoenixResult.count();
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.hasLastEvaluatedKey());
        int ddbCount = 0;
        ScanResponse ddbResult;
        do {
            ddbResult = dynamoDbClient.scan(sr.build());
            Assert.assertTrue(ddbResult.items().isEmpty());
            ddbCount += ddbResult.count();
            sr.exclusiveStartKey(ddbResult.lastEvaluatedKey());
        } while (ddbResult.hasLastEvaluatedKey());

        Assert.assertEquals(ddbCount,phoenixCount);
    }

    @Test(timeout = 120000)
    public void testScanSelectAllAttributes() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.select("ALL_ATTRIBUTES");
        
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(1, phoenixResult.count().intValue());
        // Should return all attributes
        Assert.assertTrue(phoenixResult.items().get(0).size() > 1);
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
    }

    @Test(timeout = 120000)
    public void testScanSelectAllAttributesWithProjectionValidation() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.projectionExpression("attr_0"); // should cause validation error
        sr.select("ALL_ATTRIBUTES");
        try {
            dynamoDbClient.scan(sr.build());
            Assert.fail("Expected ValidationException");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        try {
            phoenixDBClientV2.scan(sr.build());
            Assert.fail("Expected ValidationException");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
    }

    @Test(timeout = 120000)
    public void testScanSelectSpecificAttributesValidation() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.select("SPECIFIC_ATTRIBUTES");
        // No projectionExpression set - should fail
        try {
            dynamoDbClient.scan(sr.build());
            Assert.fail("Expected ValidationException");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        try {
            phoenixDBClientV2.scan(sr.build());
            Assert.fail("Expected ValidationException");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
    }

    @Test(timeout = 120000)
    public void testScanAllRowsNoSortKey() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.projectionExpression("title, #0.#1[0].#2");
        Map<String,String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Reviews");
        exprAttrNames.put("#1", "FiveStar");
        exprAttrNames.put("#2", "reviewer");
        sr.expressionAttributeNames(exprAttrNames);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        // dynamo does not guarantee ordering of partition keys in Scan, so only check count
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (Map<String, AttributeValue> item : phoenixResult.items()) {
            Assert.assertNotNull(item.get("Reviews").m().get("FiveStar").l().get(0).m().get("reviewer"));
            Assert.assertNotNull(item.get("title"));
        }
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void testScanAllRowsWithProjection() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.projectionExpression("title, #0.#1[0].#2");
        Map<String,String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Reviews");
        exprAttrNames.put("#1", "FiveStar");
        exprAttrNames.put("#2", "reviewer");
        sr.expressionAttributeNames(exprAttrNames);
        sr.select(Select.SPECIFIC_ATTRIBUTES);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        // dynamo does not guarantee ordering of partition keys in Scan, so only check count
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (Map<String, AttributeValue> item : phoenixResult.items()) {
            Assert.assertNotNull(item.get("Reviews").m().get("FiveStar").l().get(0).m().get("reviewer"));
            Assert.assertNotNull(item.get("title"));
        }
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void testScanWithTopLevelAttributeFilter() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.filterExpression("#2 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "title");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().s("Title3").build());
        sr.expressionAttributeValues(exprAttrVal);
        assertScanResults(sr);
    }

    @Test(timeout = 120000)
    public void testScanWithNestedAttributeFilter() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.filterExpression("#1.#2[0].#3 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "reviewer");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().s("Carl").build());
        sr.expressionAttributeValues(exprAttrVal);
        assertScanResults(sr);
    }

    /**
     * Dynamo seems to return results in the order of conditions in the filter expressions.
     * Phoenix returns results in order of PKs.
     * To test pagination, use a filter expression where the results satisfying the conditions
     * in order are also ordered by the keys.
     */
    @Test(timeout = 120000)
    public void testScanWithFilterAndPagination() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.filterExpression("(#4 > :v4 AND #5 < :v5) OR #1.#2[0].#3 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "reviewer");
        exprAttrNames.put("#4", "attr_0");
        exprAttrNames.put("#5", "attr_1");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().s("Drake").build());
        exprAttrVal.put(":v4", AttributeValue.builder().s("A").build());
        exprAttrVal.put(":v5", AttributeValue.builder().n("3").build());
        sr.expressionAttributeValues(exprAttrVal);
        sr.limit(1);
        ScanResponse phoenixResult, dynamoResult;
        int paginationCount = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            dynamoResult = dynamoDbClient.scan(sr.build());
            Assert.assertEquals(dynamoResult.items(), phoenixResult.items());
            paginationCount++;
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        // 1 more than total number of results expected
        Assert.assertEquals(3, paginationCount);
    }


    @Test(timeout = 120000)
    public void testScanWithPaginationNoFilter() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.limit(1);
        ScanResponse phoenixResult;
        int count = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            count += phoenixResult.count();
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        Assert.assertEquals(4, count);
    }

    @Test(timeout = 120000)
    public void testScanWithPaginationNoSortKeyNoFilter() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.limit(1);
        ScanResponse phoenixResult;
        int count = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            count += phoenixResult.count();
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        Assert.assertEquals(4, count);
    }

    @Test(timeout = 120000)
    public void testScanFullTable() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        for (int i=0; i<10;i++) {
            String randomPk = "A" + i % 4;
            String randomValue = UUID.randomUUID().toString();
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("PK1", AttributeValue.builder().s(randomPk).build());
            item.put("PK2", AttributeValue.builder().n(String.valueOf(i)).build());
            item.put("VAL", AttributeValue.builder().s(randomValue).build());
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            phoenixDBClientV2.putItem(request);
        }
        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.limit(1);
        ScanResponse phoenixResult;
        int count = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            count += phoenixResult.count();
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        Assert.assertEquals(10, count);
    }

    @Test(timeout = 120000)
    public void testScanWithSegments() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName).segment(0).totalSegments(2);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        Assert.assertEquals(4, phoenixResult.items().size());

        sr = ScanRequest.builder().tableName(tableName).segment(1).totalSegments(2);
        phoenixResult = phoenixDBClientV2.scan(sr.build());
        Assert.assertEquals(0, phoenixResult.items().size());
    }

    @Test(timeout = 120000)
    public void testScanWithBeginsWithFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("item1").build());
        item1.put("stringAttr", AttributeValue.builder().s("prefix_match").build());
        item1.put("binaryAttr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3, 4, 5}))
                        .build());
        item1.put("numberAttr", AttributeValue.builder().n("123").build());
        item1.put("category", AttributeValue.builder().s("electronics").build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("item2").build());
        item2.put("stringAttr", AttributeValue.builder().s("prefix_nomatch").build());
        item2.put("binaryAttr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 9, 8, 7}))
                        .build());
        item2.put("numberAttr", AttributeValue.builder().n("456").build());
        item2.put("category", AttributeValue.builder().s("books").build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("item3").build());
        item3.put("stringAttr", AttributeValue.builder().s("different_prefix").build());
        item3.put("binaryAttr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {5, 4, 3, 2, 1}))
                        .build());
        item3.put("numberAttr", AttributeValue.builder().n("789").build());
        item3.put("category", AttributeValue.builder().s("electronics").build());

        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put("pk", AttributeValue.builder().s("item4").build());
        item4.put("stringAttr", AttributeValue.builder().s("prefix_another").build());
        item4.put("binaryAttr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3, 9, 9}))
                        .build());
        item4.put("numberAttr", AttributeValue.builder().n("101").build());
        item4.put("category", AttributeValue.builder().s("books").build());

        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(item4).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        // Test 1: begins_with with String attribute - positive case (using attribute name alias)
        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        sr1.filterExpression("begins_with(#strAttr, :prefix)");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#strAttr", "stringAttr");
        sr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":prefix", AttributeValue.builder().s("prefix_").build());
        sr1.expressionAttributeValues(exprAttrVal1);

        assertScanResults(sr1, 3);

        // Test 2: begins_with with Binary attribute - positive case
        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        sr2.filterExpression("begins_with(binaryAttr, :binPrefix)");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":binPrefix",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        sr2.expressionAttributeValues(exprAttrVal2);

        assertScanResults(sr2, 3);

        // Test 3: begins_with with String attribute - negative case (no matches)
        ScanRequest.Builder sr3 = ScanRequest.builder().tableName(tableName);
        sr3.filterExpression("begins_with(stringAttr, :noMatch)");
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":noMatch", AttributeValue.builder().s("nonexistent").build());
        sr3.expressionAttributeValues(exprAttrVal3);

        assertScanResults(sr3, 0);

        // Test 4: begins_with combined with AND condition
        ScanRequest.Builder sr4 = ScanRequest.builder().tableName(tableName);
        sr4.filterExpression("begins_with(stringAttr, :prefix) AND category = :cat");
        Map<String, AttributeValue> exprAttrVal4 = new HashMap<>();
        exprAttrVal4.put(":prefix", AttributeValue.builder().s("prefix_").build());
        exprAttrVal4.put(":cat", AttributeValue.builder().s("electronics").build());
        sr4.expressionAttributeValues(exprAttrVal4);

        assertScanResults(sr4, 1);

        // Test 5: begins_with combined with OR condition
        ScanRequest.Builder sr5 = ScanRequest.builder().tableName(tableName);
        sr5.filterExpression(
                "begins_with(stringAttr, :prefix1) OR begins_with(stringAttr, :prefix2)");
        Map<String, AttributeValue> exprAttrVal5 = new HashMap<>();
        exprAttrVal5.put(":prefix1", AttributeValue.builder().s("prefix_").build());
        exprAttrVal5.put(":prefix2", AttributeValue.builder().s("different_").build());
        sr5.expressionAttributeValues(exprAttrVal5);

        assertScanResults(sr5, 4);

        // Test 6: begins_with with attribute names using expression attribute names
        ScanRequest.Builder sr6 = ScanRequest.builder().tableName(tableName);
        sr6.filterExpression("begins_with(#attr, :prefix)");
        Map<String, String> exprAttrNames6 = new HashMap<>();
        exprAttrNames6.put("#attr", "stringAttr");
        sr6.expressionAttributeNames(exprAttrNames6);
        Map<String, AttributeValue> exprAttrVal6 = new HashMap<>();
        exprAttrVal6.put(":prefix", AttributeValue.builder().s("prefix_").build());
        sr6.expressionAttributeValues(exprAttrVal6);

        assertScanResults(sr6, 3);

        // Test 7: Test begins_with with unsupported data type (Number) - should fail
        ScanRequest.Builder sr7 = ScanRequest.builder().tableName(tableName);
        sr7.filterExpression("begins_with(numberAttr, :numPrefix)");
        Map<String, AttributeValue> exprAttrVal7 = new HashMap<>();
        exprAttrVal7.put(":numPrefix", AttributeValue.builder().n("12").build());
        sr7.expressionAttributeValues(exprAttrVal7);

        try {
            phoenixDBClientV2.scan(sr7.build());
            throw new RuntimeException("Should have thrown an exception for invalid data type");
        } catch (DynamoDbException e) {
            LOGGER.info("begins_with with Number data type failed as expected: {}", e.getMessage());
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            dynamoDbClient.scan(sr7.build());
            throw new RuntimeException("Should have thrown an exception for invalid data type");
        } catch (DynamoDbException e) {
            LOGGER.info("begins_with with Number data type failed as expected: {}", e.getMessage());
            Assert.assertEquals(400, e.statusCode());
        }

        // Test 8: begins_with with exact match (prefix equals entire string)
        ScanRequest.Builder sr8 = ScanRequest.builder().tableName(tableName);
        sr8.filterExpression("begins_with(stringAttr, :exactMatch)");
        Map<String, AttributeValue> exprAttrVal8 = new HashMap<>();
        exprAttrVal8.put(":exactMatch", AttributeValue.builder().s("prefix_match").build());
        sr8.expressionAttributeValues(exprAttrVal8);

        assertScanResults(sr8, 1);

        // Test 9: begins_with with empty prefix
        ScanRequest.Builder sr9 = ScanRequest.builder().tableName(tableName);
        sr9.filterExpression("begins_with(stringAttr, :emptyPrefix)");
        Map<String, AttributeValue> exprAttrVal9 = new HashMap<>();
        exprAttrVal9.put(":emptyPrefix", AttributeValue.builder().s("").build());
        sr9.expressionAttributeValues(exprAttrVal9);

        assertScanResults(sr9, 4);

        // Test 10: begins_with with binary data - more specific prefix
        ScanRequest.Builder sr10 = ScanRequest.builder().tableName(tableName);
        sr10.filterExpression("begins_with(binaryAttr, :specificBinPrefix)");
        Map<String, AttributeValue> exprAttrVal10 = new HashMap<>();
        exprAttrVal10.put(":specificBinPrefix",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build());
        sr10.expressionAttributeValues(exprAttrVal10);

        assertScanResults(sr10, 2);

        // Test 11: begins_with for non-existent attribute
        ScanRequest.Builder sr11 = ScanRequest.builder().tableName(tableName);
        sr11.filterExpression("NOT begins_with(nonExistentAttr, :specificBinPrefix)");
        Map<String, AttributeValue> exprAttrVal11 = new HashMap<>();
        exprAttrVal11.put(":specificBinPrefix",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build());
        sr11.expressionAttributeValues(exprAttrVal11);

        assertScanResults(sr11, 4);
    }

    @Test(timeout = 120000)
    public void testScanWithContainsFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        putItemsForScanWithContainsFilter(tableName);

        // Test 1: contains() with String attribute - substring search
        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        sr1.filterExpression("contains(#strAttr, :substring)");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#strAttr", "stringAttr");
        sr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":substring", AttributeValue.builder().s("world").build());
        sr1.expressionAttributeValues(exprAttrVal1);

        assertScanResults(sr1, 4);

        // Test 2: contains() with String Set - element search
        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        sr2.filterExpression("contains(stringSet, :element)");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":element", AttributeValue.builder().s("apple").build());
        sr2.expressionAttributeValues(exprAttrVal2);

        assertScanResults(sr2, 4);

        // Test 3: contains() with Number Set - element search
        ScanRequest.Builder sr3 = ScanRequest.builder().tableName(tableName);
        sr3.filterExpression("contains(numberSet, :number)");
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":number", AttributeValue.builder().n("1").build());
        sr3.expressionAttributeValues(exprAttrVal3);

        assertScanResults(sr3, 4);

        // Test 4: contains() with Binary Set - element search
        ScanRequest.Builder sr4 = ScanRequest.builder().tableName(tableName);
        sr4.filterExpression("contains(binarySet, :binaryElement)");
        Map<String, AttributeValue> exprAttrVal4 = new HashMap<>();
        exprAttrVal4.put(":binaryElement",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        sr4.expressionAttributeValues(exprAttrVal4);

        assertScanResults(sr4, 4);

        // Test 5: contains() with List attribute - element search (String)
        ScanRequest.Builder sr5 = ScanRequest.builder().tableName(tableName);
        sr5.filterExpression("contains(listAttr, :listElement)");
        Map<String, AttributeValue> exprAttrVal5 = new HashMap<>();
        exprAttrVal5.put(":listElement", AttributeValue.builder().s("red").build());
        sr5.expressionAttributeValues(exprAttrVal5);

        assertScanResults(sr5, 3);

        // Test 6: contains() with List attribute - element search (Number)
        ScanRequest.Builder sr6 = ScanRequest.builder().tableName(tableName);
        sr6.filterExpression("contains(listAttr, :listElement)");
        Map<String, AttributeValue> exprAttrVal6 = new HashMap<>();
        exprAttrVal6.put(":listElement", AttributeValue.builder().n("42").build());
        sr6.expressionAttributeValues(exprAttrVal6);

        assertScanResults(sr6, 2);

        // Test 7: contains() with String attribute - negative case (no matches)
        ScanRequest.Builder sr7 = ScanRequest.builder().tableName(tableName);
        sr7.filterExpression("contains(stringAttr, :noMatch)");
        Map<String, AttributeValue> exprAttrVal7 = new HashMap<>();
        exprAttrVal7.put(":noMatch", AttributeValue.builder().s("nonexistent").build());
        sr7.expressionAttributeValues(exprAttrVal7);

        assertScanResults(sr7, 0);

        // Test 8: contains() combined with AND condition
        ScanRequest.Builder sr8 = ScanRequest.builder().tableName(tableName);
        sr8.filterExpression("contains(stringAttr, :substring) AND category = :cat");
        Map<String, AttributeValue> exprAttrVal8 = new HashMap<>();
        exprAttrVal8.put(":substring", AttributeValue.builder().s("world").build());
        exprAttrVal8.put(":cat", AttributeValue.builder().s("electronics").build());
        sr8.expressionAttributeValues(exprAttrVal8);

        assertScanResults(sr8, 1);

        // Test 9: contains() combined with OR condition
        ScanRequest.Builder sr9 = ScanRequest.builder().tableName(tableName);
        sr9.filterExpression("contains(stringSet, :fruit1) OR contains(stringSet, :fruit2)");
        Map<String, AttributeValue> exprAttrVal9 = new HashMap<>();
        exprAttrVal9.put(":fruit1", AttributeValue.builder().s("apple").build());
        exprAttrVal9.put(":fruit2", AttributeValue.builder().s("orange").build());
        sr9.expressionAttributeValues(exprAttrVal9);

        assertScanResults(sr9, 6);

        // Test 10: contains() with expression attribute names
        ScanRequest.Builder sr10 = ScanRequest.builder().tableName(tableName);
        sr10.filterExpression("contains(#attr, :substring)");
        Map<String, String> exprAttrNames10 = new HashMap<>();
        exprAttrNames10.put("#attr", "stringAttr");
        sr10.expressionAttributeNames(exprAttrNames10);
        Map<String, AttributeValue> exprAttrVal10 = new HashMap<>();
        exprAttrVal10.put(":substring", AttributeValue.builder().s("test").build());
        sr10.expressionAttributeValues(exprAttrVal10);

        assertScanResults(sr10, 4);

        // Test 11: contains() with case-sensitive string matching
        ScanRequest.Builder sr11 = ScanRequest.builder().tableName(tableName);
        sr11.filterExpression("contains(stringAttr, :substring)");
        Map<String, AttributeValue> exprAttrVal11 = new HashMap<>();
        exprAttrVal11.put(":substring", AttributeValue.builder().s("WORLD").build()); // uppercase
        sr11.expressionAttributeValues(exprAttrVal11);

        assertScanResults(sr11, 0); // case-sensitive, no matches

        // Test 12: contains() with Set - element not found
        ScanRequest.Builder sr12 = ScanRequest.builder().tableName(tableName);
        sr12.filterExpression("contains(stringSet, :element)");
        Map<String, AttributeValue> exprAttrVal12 = new HashMap<>();
        exprAttrVal12.put(":element", AttributeValue.builder().s("nonexistent").build());
        sr12.expressionAttributeValues(exprAttrVal12);

        assertScanResults(sr12, 0);

        // Test 13: contains() with Number Set - partial number match (should fail)
        ScanRequest.Builder sr13 = ScanRequest.builder().tableName(tableName);
        sr13.filterExpression("contains(numberSet, :partialNumber)");
        Map<String, AttributeValue> exprAttrVal13 = new HashMap<>();
        exprAttrVal13.put(":partialNumber",
                AttributeValue.builder().n("10").build()); // Looking for exact match
        sr13.expressionAttributeValues(exprAttrVal13);

        assertScanResults(sr13, 2); // item4 and item6 have "10" in numberSet

        // Test 14: contains() with unsupported data type (Number) on String attribute - should fail
        ScanRequest.Builder sr14 = ScanRequest.builder().tableName(tableName);
        sr14.filterExpression("contains(numberAttr, :substring)");
        Map<String, AttributeValue> exprAttrVal14 = new HashMap<>();
        exprAttrVal14.put(":substring", AttributeValue.builder().s("10").build());
        sr14.expressionAttributeValues(exprAttrVal14);

        assertScanResults(sr14, 0);

        // Test 15: contains() with NOT operator
        ScanRequest.Builder sr15 = ScanRequest.builder().tableName(tableName);
        sr15.filterExpression("NOT contains(stringAttr, :substring)");
        Map<String, AttributeValue> exprAttrVal15 = new HashMap<>();
        exprAttrVal15.put(":substring", AttributeValue.builder().s("world").build());
        sr15.expressionAttributeValues(exprAttrVal15);

        assertScanResults(sr15, 4); // items that don't contain "world"

        // Test 16: contains() with empty string
        ScanRequest.Builder sr16 = ScanRequest.builder().tableName(tableName);
        sr16.filterExpression("contains(stringAttr, :emptyString)");
        Map<String, AttributeValue> exprAttrVal16 = new HashMap<>();
        exprAttrVal16.put(":emptyString", AttributeValue.builder().s("").build());
        sr16.expressionAttributeValues(exprAttrVal16);

        assertScanResults(sr16, 8); // empty string is contained in all strings

        // Test 17: contains() with multiple conditions using parentheses
        ScanRequest.Builder sr17 = ScanRequest.builder().tableName(tableName);
        sr17.filterExpression(
                "(contains(stringAttr, :sub1) OR contains(stringAttr, :sub2)) AND category = :cat");
        Map<String, AttributeValue> exprAttrVal17 = new HashMap<>();
        exprAttrVal17.put(":sub1", AttributeValue.builder().s("hello").build());
        exprAttrVal17.put(":sub2", AttributeValue.builder().s("testing").build());
        exprAttrVal17.put(":cat", AttributeValue.builder().s("electronics").build());
        sr17.expressionAttributeValues(exprAttrVal17);

        assertScanResults(sr17, 3);

        // Test 18: contains() for non-existent attribute
        ScanRequest.Builder sr18 = ScanRequest.builder().tableName(tableName);
        sr18.filterExpression("NOT contains(nonExistentAttr, :value)");
        Map<String, AttributeValue> exprAttrVal18 = new HashMap<>();
        exprAttrVal18.put(":value", AttributeValue.builder().s("test").build());
        sr18.expressionAttributeValues(exprAttrVal18);

        assertScanResults(sr18, 8); // non-existent attribute doesn't contain anything
    }

    @Test(timeout = 120000)
    public void testScanWithSizeFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        putItemsForScanWithSizeFilter(tableName);

        // Test 1: size() with String attribute - character count
        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        sr1.filterExpression("size(#strAttr) > :size");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#strAttr", "stringAttr");
        sr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":size", AttributeValue.builder().n("10").build());
        sr1.expressionAttributeValues(exprAttrVal1);

        assertScanResults(sr1, 2);

        // Test 2: size() with Binary attribute - byte count
        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        sr2.filterExpression("size(binaryAttr) = :byteSize");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":byteSize", AttributeValue.builder().n("3").build());
        sr2.expressionAttributeValues(exprAttrVal2);

        assertScanResults(sr2, 2);

        // Test 3: size() with String Set - element count
        ScanRequest.Builder sr3 = ScanRequest.builder().tableName(tableName);
        sr3.filterExpression("size(stringSet) >= :setSize");
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":setSize", AttributeValue.builder().n("3").build());
        sr3.expressionAttributeValues(exprAttrVal3);

        assertScanResults(sr3, 2);

        // Test 4: size() with Number Set - element count
        ScanRequest.Builder sr4 = ScanRequest.builder().tableName(tableName);
        sr4.filterExpression("size(numberSet) = :numSetSize");
        Map<String, AttributeValue> exprAttrVal4 = new HashMap<>();
        exprAttrVal4.put(":numSetSize", AttributeValue.builder().n("2").build());
        sr4.expressionAttributeValues(exprAttrVal4);

        assertScanResults(sr4, 2);

        // Test 5: size() with Binary Set - element count
        ScanRequest.Builder sr5 = ScanRequest.builder().tableName(tableName);
        sr5.filterExpression("size(binarySet) < :binSetSize");
        Map<String, AttributeValue> exprAttrVal5 = new HashMap<>();
        exprAttrVal5.put(":binSetSize", AttributeValue.builder().n("3").build());
        sr5.expressionAttributeValues(exprAttrVal5);

        assertScanResults(sr5, 3);

        // Test 6: size() with List attribute - element count
        ScanRequest.Builder sr6 = ScanRequest.builder().tableName(tableName);
        sr6.filterExpression("size(listAttr) = :listSize");
        Map<String, AttributeValue> exprAttrVal6 = new HashMap<>();
        exprAttrVal6.put(":listSize", AttributeValue.builder().n("3").build());
        sr6.expressionAttributeValues(exprAttrVal6);

        assertScanResults(sr6, 2);

        // Test 7: size() with Map attribute - key count
        ScanRequest.Builder sr7 = ScanRequest.builder().tableName(tableName);
        sr7.filterExpression("size(mapAttr) > :mapSize");
        Map<String, AttributeValue> exprAttrVal7 = new HashMap<>();
        exprAttrVal7.put(":mapSize", AttributeValue.builder().n("1").build());
        sr7.expressionAttributeValues(exprAttrVal7);

        assertScanResults(sr7, 2);

        // Test 8: size() combined with AND condition
        ScanRequest.Builder sr8 = ScanRequest.builder().tableName(tableName);
        sr8.filterExpression("size(stringAttr) > :strSize AND size(numberSet) = :numSize");
        Map<String, AttributeValue> exprAttrVal8 = new HashMap<>();
        exprAttrVal8.put(":strSize", AttributeValue.builder().n("8").build());
        exprAttrVal8.put(":numSize", AttributeValue.builder().n("3").build());
        sr8.expressionAttributeValues(exprAttrVal8);

        assertScanResults(sr8, 1);

        // Test 9: size() combined with OR condition
        ScanRequest.Builder sr9 = ScanRequest.builder().tableName(tableName);
        sr9.filterExpression("size(stringAttr) <= :smallStr OR size(listAttr) = :bigList");
        Map<String, AttributeValue> exprAttrVal9 = new HashMap<>();
        exprAttrVal9.put(":smallStr", AttributeValue.builder().n("5").build());
        exprAttrVal9.put(":bigList", AttributeValue.builder().n("4").build());
        sr9.expressionAttributeValues(exprAttrVal9);

        assertScanResults(sr9, 3);

        // Test 10: size() with NOT operator
        ScanRequest.Builder sr10 = ScanRequest.builder().tableName(tableName);
        sr10.filterExpression("NOT size(stringSet) = :setSize");
        Map<String, AttributeValue> exprAttrVal10 = new HashMap<>();
        exprAttrVal10.put(":setSize", AttributeValue.builder().n("2").build());
        sr10.expressionAttributeValues(exprAttrVal10);

        assertScanResults(sr10, 2);

        // Test 11: size() with empty string
        ScanRequest.Builder sr11 = ScanRequest.builder().tableName(tableName);
        sr11.filterExpression("size(stringAttr) > :zero");
        Map<String, AttributeValue> exprAttrVal11 = new HashMap<>();
        exprAttrVal11.put(":zero", AttributeValue.builder().n("0").build());
        sr11.expressionAttributeValues(exprAttrVal11);

        assertScanResults(sr11, 4); // All non-empty strings

        // Test 12: size() for non-existent attribute
        ScanRequest.Builder sr12 = ScanRequest.builder().tableName(tableName);
        sr12.filterExpression("size(nonExistentAttr) > :zero");
        Map<String, AttributeValue> exprAttrVal12 = new HashMap<>();
        exprAttrVal12.put(":zero", AttributeValue.builder().n("0").build());
        sr12.expressionAttributeValues(exprAttrVal12);

        assertScanResults(sr12, 0);
    }

    @Test(timeout = 120000)
    public void testScanWithAttributeTypeFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        putItemsForScanWithAttributeTypeFilter(tableName);

        // Test 1: attribute_type() for String attribute
        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        sr1.filterExpression("attribute_type(#strAttr, :stringType)");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#strAttr", "stringAttr");
        sr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":stringType", AttributeValue.builder().s("S").build());
        sr1.expressionAttributeValues(exprAttrVal1);

        assertScanResults(sr1, 6);

        // Test 2: attribute_type() for Number attribute
        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        sr2.filterExpression("attribute_type(numberAttr, :numberType)");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":numberType", AttributeValue.builder().s("N").build());
        sr2.expressionAttributeValues(exprAttrVal2);

        assertScanResults(sr2, 6);

        // Test 3: attribute_type() for Binary attribute
        ScanRequest.Builder sr3 = ScanRequest.builder().tableName(tableName);
        sr3.filterExpression("attribute_type(binaryAttr, :binaryType)");
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":binaryType", AttributeValue.builder().s("B").build());
        sr3.expressionAttributeValues(exprAttrVal3);

        assertScanResults(sr3, 4);

        // Test 4: attribute_type() for String Set
        ScanRequest.Builder sr4 = ScanRequest.builder().tableName(tableName);
        sr4.filterExpression("attribute_type(stringSet, :stringSetType)");
        Map<String, AttributeValue> exprAttrVal4 = new HashMap<>();
        exprAttrVal4.put(":stringSetType", AttributeValue.builder().s("SS").build());
        sr4.expressionAttributeValues(exprAttrVal4);

        assertScanResults(sr4, 5);

        // Test 5: attribute_type() for Number Set
        ScanRequest.Builder sr5 = ScanRequest.builder().tableName(tableName);
        sr5.filterExpression("attribute_type(numberSet, :numberSetType)");
        Map<String, AttributeValue> exprAttrVal5 = new HashMap<>();
        exprAttrVal5.put(":numberSetType", AttributeValue.builder().s("NS").build());
        sr5.expressionAttributeValues(exprAttrVal5);

        assertScanResults(sr5, 4);

        // Test 6: attribute_type() for Binary Set
        ScanRequest.Builder sr6 = ScanRequest.builder().tableName(tableName);
        sr6.filterExpression("attribute_type(binarySet, :binarySetType)");
        Map<String, AttributeValue> exprAttrVal6 = new HashMap<>();
        exprAttrVal6.put(":binarySetType", AttributeValue.builder().s("BS").build());
        sr6.expressionAttributeValues(exprAttrVal6);

        assertScanResults(sr6, 3);

        // Test 7: attribute_type() for List attribute
        ScanRequest.Builder sr7 = ScanRequest.builder().tableName(tableName);
        sr7.filterExpression("attribute_type(listAttr, :listType)");
        Map<String, AttributeValue> exprAttrVal7 = new HashMap<>();
        exprAttrVal7.put(":listType", AttributeValue.builder().s("L").build());
        sr7.expressionAttributeValues(exprAttrVal7);

        assertScanResults(sr7, 4);

        // Test 8: attribute_type() for Map attribute
        ScanRequest.Builder sr8 = ScanRequest.builder().tableName(tableName);
        sr8.filterExpression("attribute_type(mapAttr, :mapType)");
        Map<String, AttributeValue> exprAttrVal8 = new HashMap<>();
        exprAttrVal8.put(":mapType", AttributeValue.builder().s("M").build());
        sr8.expressionAttributeValues(exprAttrVal8);

        assertScanResults(sr8, 3);

        // Test 9: attribute_type() for Boolean attribute
        ScanRequest.Builder sr9 = ScanRequest.builder().tableName(tableName);
        sr9.filterExpression("attribute_type(boolAttr, :boolType)");
        Map<String, AttributeValue> exprAttrVal9 = new HashMap<>();
        exprAttrVal9.put(":boolType", AttributeValue.builder().s("BOOL").build());
        sr9.expressionAttributeValues(exprAttrVal9);

        assertScanResults(sr9, 2);

        // Test 10: attribute_type() for Null attribute
        ScanRequest.Builder sr10 = ScanRequest.builder().tableName(tableName);
        sr10.filterExpression("attribute_type(nullAttr, :nullType)");
        Map<String, AttributeValue> exprAttrVal10 = new HashMap<>();
        exprAttrVal10.put(":nullType", AttributeValue.builder().s("NULL").build());
        sr10.expressionAttributeValues(exprAttrVal10);

        assertScanResults(sr10, 1);

        // Test 11: attribute_type() combined with AND condition
        ScanRequest.Builder sr11 = ScanRequest.builder().tableName(tableName);
        sr11.filterExpression("attribute_type(stringAttr, :strType) AND attribute_type(numberAttr, :numType)");
        Map<String, AttributeValue> exprAttrVal11 = new HashMap<>();
        exprAttrVal11.put(":strType", AttributeValue.builder().s("S").build());
        exprAttrVal11.put(":numType", AttributeValue.builder().s("N").build());
        sr11.expressionAttributeValues(exprAttrVal11);

        assertScanResults(sr11, 6);

        // Test 12: attribute_type() combined with OR condition
        ScanRequest.Builder sr12 = ScanRequest.builder().tableName(tableName);
        sr12.filterExpression("attribute_type(listAttr, :listType) OR attribute_type(mapAttr, :mapType)");
        Map<String, AttributeValue> exprAttrVal12 = new HashMap<>();
        exprAttrVal12.put(":listType", AttributeValue.builder().s("L").build());
        exprAttrVal12.put(":mapType", AttributeValue.builder().s("M").build());
        sr12.expressionAttributeValues(exprAttrVal12);

        assertScanResults(sr12, 4); // Items with either List or Map

        // Test 13: attribute_type() with NOT operator
        ScanRequest.Builder sr13 = ScanRequest.builder().tableName(tableName);
        sr13.filterExpression("NOT attribute_type(stringAttr, :stringType)");
        Map<String, AttributeValue> exprAttrVal13 = new HashMap<>();
        exprAttrVal13.put(":stringType", AttributeValue.builder().s("S").build());
        sr13.expressionAttributeValues(exprAttrVal13);

        assertScanResults(sr13, 0); // All items have string attribute

        // Test 14: attribute_type() for non-existent attribute
        ScanRequest.Builder sr14 = ScanRequest.builder().tableName(tableName);
        sr14.filterExpression("attribute_type(nonExistentAttr, :anyType)");
        Map<String, AttributeValue> exprAttrVal14 = new HashMap<>();
        exprAttrVal14.put(":anyType", AttributeValue.builder().s("S").build());
        sr14.expressionAttributeValues(exprAttrVal14);

        assertScanResults(sr14, 0);

        // Test 15: attribute_type() with expression attribute names
        ScanRequest.Builder sr15 = ScanRequest.builder().tableName(tableName);
        sr15.filterExpression("attribute_type(#attr, :attrType)");
        Map<String, String> exprAttrNames15 = new HashMap<>();
        exprAttrNames15.put("#attr", "binaryAttr");
        sr15.expressionAttributeNames(exprAttrNames15);
        Map<String, AttributeValue> exprAttrVal15 = new HashMap<>();
        exprAttrVal15.put(":attrType", AttributeValue.builder().s("B").build());
        sr15.expressionAttributeValues(exprAttrVal15);

        assertScanResults(sr15, 4);

        // Test 16: attribute_type() with complex parenthetical expressions
        ScanRequest.Builder sr16 = ScanRequest.builder().tableName(tableName);
        sr16.filterExpression("(attribute_type(stringAttr, :strType) OR attribute_type(listAttr, :listType)) AND attribute_type(numberAttr, :numType)");
        Map<String, AttributeValue> exprAttrVal16 = new HashMap<>();
        exprAttrVal16.put(":strType", AttributeValue.builder().s("S").build());
        exprAttrVal16.put(":listType", AttributeValue.builder().s("L").build());
        exprAttrVal16.put(":numType", AttributeValue.builder().s("N").build());
        sr16.expressionAttributeValues(exprAttrVal16);

        assertScanResults(sr16, 6);
    }

    private void putItemsForScanWithSizeFilter(String tableName) {
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("item1").build());
        item1.put("stringAttr", AttributeValue.builder().s("test").build()); // 4 chars
        item1.put("binaryAttr", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2})).build()); // 2 bytes
        item1.put("stringSet", AttributeValue.builder().ss("apple", "banana").build()); // 2 elements
        item1.put("numberSet", AttributeValue.builder().ns("1", "2", "3").build()); // 3 elements
        item1.put("binarySet", AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {1, 2})).build()); // 1 element
        item1.put("listAttr", AttributeValue.builder().l(
                AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("green").build(),
                AttributeValue.builder().s("blue").build()).build()); // 3 elements
        Map<String, AttributeValue> map1 = new HashMap<>();
        map1.put("key1", AttributeValue.builder().s("value1").build());
        item1.put("mapAttr", AttributeValue.builder().m(map1).build()); // 1 key

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("item2").build());
        item2.put("stringAttr", AttributeValue.builder().s("hello world").build()); // 11 chars
        item2.put("binaryAttr", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build()); // 3 bytes
        item2.put("stringSet", AttributeValue.builder().ss("orange", "grape", "kiwi").build()); // 3 elements
        item2.put("numberSet", AttributeValue.builder().ns("4", "5").build()); // 2 elements
        item2.put("binarySet", AttributeValue.builder().bs(
                SdkBytes.fromByteArray(new byte[] {7, 8}),
                SdkBytes.fromByteArray(new byte[] {9, 10})).build()); // 2 elements
        item2.put("listAttr", AttributeValue.builder().l(
                AttributeValue.builder().s("yellow").build(),
                AttributeValue.builder().s("purple").build(),
                AttributeValue.builder().s("pink").build(),
                AttributeValue.builder().s("brown").build()).build()); // 4 elements
        Map<String, AttributeValue> map2 = new HashMap<>();
        map2.put("key1", AttributeValue.builder().s("value1").build());
        map2.put("key2", AttributeValue.builder().n("42").build());
        item2.put("mapAttr", AttributeValue.builder().m(map2).build()); // 2 keys

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("item3").build());
        item3.put("stringAttr", AttributeValue.builder().s("testing size function").build()); // 20 chars
        item3.put("binaryAttr", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build()); // 3 bytes
        item3.put("stringSet", AttributeValue.builder().ss("apple", "orange", "mango", "cherry").build()); // 4 elements
        item3.put("numberSet", AttributeValue.builder().ns("1", "7", "8").build()); // 3 elements
        item3.put("binarySet", AttributeValue.builder().bs(
                SdkBytes.fromByteArray(new byte[] {1, 2}),
                SdkBytes.fromByteArray(new byte[] {11, 12}),
                SdkBytes.fromByteArray(new byte[] {13, 14})).build()); // 3 elements
        item3.put("listAttr", AttributeValue.builder().l(
                AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("orange").build(),
                AttributeValue.builder().n("42").build()).build()); // 3 elements
        Map<String, AttributeValue> map3 = new HashMap<>();
        map3.put("key1", AttributeValue.builder().s("value1").build());
        map3.put("key2", AttributeValue.builder().n("42").build());
        map3.put("key3", AttributeValue.builder().bool(true).build());
        item3.put("mapAttr", AttributeValue.builder().m(map3).build()); // 3 keys

        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put("pk", AttributeValue.builder().s("item4").build());
        item4.put("stringAttr", AttributeValue.builder().s("short").build()); // 5 chars
        item4.put("binaryAttr", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3, 4})).build()); // 4 bytes
        item4.put("stringSet", AttributeValue.builder().ss("watermelon", "pineapple").build()); // 2 elements
        item4.put("numberSet", AttributeValue.builder().ns("9", "10").build()); // 2 elements
        item4.put("binarySet", AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {13, 14})).build()); // 1 element
        item4.put("listAttr", AttributeValue.builder().l(
                AttributeValue.builder().s("black").build(),
                AttributeValue.builder().s("white").build()).build()); // 2 elements
        Map<String, AttributeValue> map4 = new HashMap<>();
        map4.put("singleKey", AttributeValue.builder().s("singleValue").build());
        item4.put("mapAttr", AttributeValue.builder().m(map4).build()); // 1 key

        // Insert test data
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(item3).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(item4).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);
    }

    private void putItemsForScanWithAttributeTypeFilter(String tableName) {
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("item1").build());
        item1.put("stringAttr", AttributeValue.builder().s("test string").build());
        item1.put("numberAttr", AttributeValue.builder().n("100").build());
        item1.put("binaryAttr", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        item1.put("stringSet", AttributeValue.builder().ss("apple", "banana").build());
        item1.put("numberSet", AttributeValue.builder().ns("1", "2", "3").build());
        item1.put("binarySet", AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        item1.put("listAttr", AttributeValue.builder().l(
                AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("green").build()).build());
        Map<String, AttributeValue> map1 = new HashMap<>();
        map1.put("nested", AttributeValue.builder().s("value").build());
        item1.put("mapAttr", AttributeValue.builder().m(map1).build());
        item1.put("boolAttr", AttributeValue.builder().bool(true).build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("item2").build());
        item2.put("stringAttr", AttributeValue.builder().s("another string").build());
        item2.put("numberAttr", AttributeValue.builder().n("200").build());
        item2.put("binaryAttr", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {3, 4, 5})).build());
        item2.put("stringSet", AttributeValue.builder().ss("orange", "grape").build());
        item2.put("binarySet", AttributeValue.builder().bs(
                SdkBytes.fromByteArray(new byte[] {7, 8}),
                SdkBytes.fromByteArray(new byte[] {9, 10})).build());
        item2.put("listAttr", AttributeValue.builder().l(
                AttributeValue.builder().n("42").build(),
                AttributeValue.builder().s("mixed").build()).build());
        Map<String, AttributeValue> map2 = new HashMap<>();
        map2.put("key1", AttributeValue.builder().s("value1").build());
        map2.put("key2", AttributeValue.builder().n("42").build());
        item2.put("mapAttr", AttributeValue.builder().m(map2).build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("item3").build());
        item3.put("stringAttr", AttributeValue.builder().s("third string").build());
        item3.put("numberAttr", AttributeValue.builder().n("300").build());
        item3.put("binaryAttr", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {6, 7, 8})).build());
        item3.put("stringSet", AttributeValue.builder().ss("kiwi", "mango", "cherry").build());
        item3.put("numberSet", AttributeValue.builder().ns("10", "20").build());
        item3.put("binarySet", AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {11, 12})).build());
        item3.put("listAttr", AttributeValue.builder().l(
                AttributeValue.builder().s("blue").build(),
                AttributeValue.builder().bool(false).build()).build());

        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put("pk", AttributeValue.builder().s("item4").build());
        item4.put("stringAttr", AttributeValue.builder().s("fourth string").build());
        item4.put("numberAttr", AttributeValue.builder().n("400").build());
        item4.put("binaryAttr", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {9, 10, 11})).build());
        item4.put("stringSet", AttributeValue.builder().ss("watermelon").build());
        item4.put("numberSet", AttributeValue.builder().ns("5", "15").build());
        item4.put("listAttr", AttributeValue.builder().l(
                AttributeValue.builder().s("yellow").build(),
                AttributeValue.builder().n("100").build()).build());
        Map<String, AttributeValue> map4 = new HashMap<>();
        map4.put("nested", AttributeValue.builder().bool(true).build());
        item4.put("mapAttr", AttributeValue.builder().m(map4).build());
        item4.put("boolAttr", AttributeValue.builder().bool(false).build());

        Map<String, AttributeValue> item5 = new HashMap<>();
        item5.put("pk", AttributeValue.builder().s("item5").build());
        item5.put("stringAttr", AttributeValue.builder().s("fifth string").build());
        item5.put("numberAttr", AttributeValue.builder().n("500").build());
        item5.put("stringSet", AttributeValue.builder().ss("apple", "orange").build());
        item5.put("numberSet", AttributeValue.builder().ns("1").build());
        // No binary attributes for this item

        Map<String, AttributeValue> item6 = new HashMap<>();
        item6.put("pk", AttributeValue.builder().s("item6").build());
        item6.put("stringAttr", AttributeValue.builder().s("sixth string").build());
        item6.put("numberAttr", AttributeValue.builder().n("600").build());
        item6.put("nullAttr", AttributeValue.builder().nul(true).build());
        // Only basic attributes for this item

        // Insert test data
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(item3).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(item4).build();
        PutItemRequest putItemRequest5 = PutItemRequest.builder().tableName(tableName).item(item5).build();
        PutItemRequest putItemRequest6 = PutItemRequest.builder().tableName(tableName).item(item6).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        phoenixDBClientV2.putItem(putItemRequest5);
        phoenixDBClientV2.putItem(putItemRequest6);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest5);
        dynamoDbClient.putItem(putItemRequest6);
    }

    private void putItemsForScanWithContainsFilter(String tableName) {
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("item1").build());
        item1.put("stringAttr", AttributeValue.builder().s("hello world test").build());
        item1.put("stringSet", AttributeValue.builder().ss("apple", "banana", "cherry").build());
        item1.put("numberSet", AttributeValue.builder().ns("1", "2", "3").build());
        item1.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {1, 2}),
                        SdkBytes.fromByteArray(new byte[] {3, 4}),
                        SdkBytes.fromByteArray(new byte[] {5, 6})).build());
        item1.put("listAttr", AttributeValue.builder().l(AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("green").build(),
                AttributeValue.builder().s("blue").build()).build());
        item1.put("category", AttributeValue.builder().s("electronics").build());
        item1.put("numberAttr", AttributeValue.builder().n("100").build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("item2").build());
        item2.put("stringAttr", AttributeValue.builder().s("goodbye world").build());
        item2.put("stringSet", AttributeValue.builder().ss("orange", "grape", "kiwi").build());
        item2.put("numberSet", AttributeValue.builder().ns("4", "5", "6").build());
        item2.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {7, 8}),
                        SdkBytes.fromByteArray(new byte[] {9, 10})).build());
        item2.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("yellow").build(),
                        AttributeValue.builder().s("purple").build()).build());
        item2.put("category", AttributeValue.builder().s("books").build());
        item2.put("numberAttr", AttributeValue.builder().n("200").build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("item3").build());
        item3.put("stringAttr", AttributeValue.builder().s("testing contains function").build());
        item3.put("stringSet", AttributeValue.builder().ss("apple", "orange", "mango").build());
        item3.put("numberSet", AttributeValue.builder().ns("1", "7", "8").build());
        item3.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {1, 2}),
                        SdkBytes.fromByteArray(new byte[] {11, 12})).build());
        item3.put("listAttr", AttributeValue.builder().l(AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("orange").build(),
                AttributeValue.builder().n("42").build()).build());
        item3.put("category", AttributeValue.builder().s("electronics").build());
        item3.put("numberAttr", AttributeValue.builder().n("300").build());

        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put("pk", AttributeValue.builder().s("item4").build());
        item4.put("stringAttr", AttributeValue.builder().s("no match here").build());
        item4.put("stringSet", AttributeValue.builder().ss("watermelon", "pineapple").build());
        item4.put("numberSet", AttributeValue.builder().ns("9", "10", "11").build());
        item4.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {13, 14})).build());
        item4.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("black").build(),
                        AttributeValue.builder().s("white").build()).build());
        item4.put("category", AttributeValue.builder().s("books").build());
        item4.put("numberAttr", AttributeValue.builder().n("400").build());

        Map<String, AttributeValue> item5 = new HashMap<>();
        item5.put("pk", AttributeValue.builder().s("item5").build());
        item5.put("stringAttr", AttributeValue.builder().s("world of wonders").build());
        item5.put("stringSet", AttributeValue.builder().ss("apple", "banana").build());
        item5.put("numberSet", AttributeValue.builder().ns("1", "2").build());
        item5.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        item5.put("listAttr", AttributeValue.builder().l(AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("pink").build()).build());
        item5.put("category", AttributeValue.builder().s("toys").build());
        item5.put("numberAttr", AttributeValue.builder().n("500").build());

        Map<String, AttributeValue> item6 = new HashMap<>();
        item6.put("pk", AttributeValue.builder().s("item6").build());
        item6.put("stringAttr", AttributeValue.builder().s("hello universe").build());
        item6.put("stringSet", AttributeValue.builder().ss("orange", "mango").build());
        item6.put("numberSet", AttributeValue.builder().ns("10", "12").build());
        item6.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {15, 16})).build());
        item6.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("green").build(),
                        AttributeValue.builder().s("blue").build()).build());
        item6.put("category", AttributeValue.builder().s("electronics").build());
        item6.put("numberAttr", AttributeValue.builder().n("600").build());

        Map<String, AttributeValue> item7 = new HashMap<>();
        item7.put("pk", AttributeValue.builder().s("item7").build());
        item7.put("stringAttr", AttributeValue.builder().s("testing world").build());
        item7.put("stringSet", AttributeValue.builder().ss("grape", "apple").build());
        item7.put("numberSet", AttributeValue.builder().ns("1", "9").build());
        item7.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {1, 2}),
                        SdkBytes.fromByteArray(new byte[] {7, 8})).build());
        item7.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("black").build(),
                        AttributeValue.builder().n("42").build()).build());
        item7.put("category", AttributeValue.builder().s("books").build());
        item7.put("numberAttr", AttributeValue.builder().n("700").build());

        Map<String, AttributeValue> item8 = new HashMap<>();
        item8.put("pk", AttributeValue.builder().s("item8").build());
        item8.put("stringAttr", AttributeValue.builder().s("sample test").build());
        item8.put("stringSet", AttributeValue.builder().ss("cherry", "kiwi").build());
        item8.put("numberSet", AttributeValue.builder().ns("5", "6").build());
        item8.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {9, 10})).build());
        item8.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("white").build(),
                        AttributeValue.builder().s("gray").build()).build());
        item8.put("category", AttributeValue.builder().s("toys").build());
        item8.put("numberAttr", AttributeValue.builder().n("800").build());

        // Insert test data
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(item4).build();
        PutItemRequest putItemRequest5 =
                PutItemRequest.builder().tableName(tableName).item(item5).build();
        PutItemRequest putItemRequest6 =
                PutItemRequest.builder().tableName(tableName).item(item6).build();
        PutItemRequest putItemRequest7 =
                PutItemRequest.builder().tableName(tableName).item(item7).build();
        PutItemRequest putItemRequest8 =
                PutItemRequest.builder().tableName(tableName).item(item8).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        phoenixDBClientV2.putItem(putItemRequest5);
        phoenixDBClientV2.putItem(putItemRequest6);
        phoenixDBClientV2.putItem(putItemRequest7);
        phoenixDBClientV2.putItem(putItemRequest8);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest5);
        dynamoDbClient.putItem(putItemRequest6);
        dynamoDbClient.putItem(putItemRequest7);
        dynamoDbClient.putItem(putItemRequest8);
    }


    private void assertScanResults(ScanRequest.Builder scanRequestBuilder, Integer expectedCount) {
        assertScanResults(scanRequestBuilder, expectedCount, "pk");
    }

    /**
     * Helper method to execute scan requests on both Phoenix and DynamoDB clients
     * and assert that the results match.
     */
    private void assertScanResults(ScanRequest.Builder scanRequestBuilder, Integer expectedCount,
            String pkName) {
        ScanResponse phoenixResult = phoenixDBClientV2.scan(scanRequestBuilder.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(scanRequestBuilder.build());

        if (expectedCount != null) {
            Assert.assertEquals(expectedCount.intValue(), dynamoResult.count().intValue());
        }
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(TestUtils.sortItemsByPk(dynamoResult.items(), pkName),
                TestUtils.sortItemsByPk(phoenixResult.items(), pkName)));
    }

    /**
     * Overloaded helper method for cases where expected count is not specified.
     */
    private void assertScanResults(ScanRequest.Builder scanRequestBuilder) {
        assertScanResults(scanRequestBuilder, null);
    }

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
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
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
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(
                AttributeValue.builder().m(reviewMap1).build(),
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
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
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
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    @Test(timeout = 120000)
    public void testScanWithAttributesToGet() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

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

        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        sr1.attributesToGet(Arrays.asList("attr_0", "title", "Id2"));
        assertScanResults(sr1, 4, "attr_0");

        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        sr2.attributesToGet(Arrays.asList("title", "attr_0"));
        assertScanResults(sr2, 4, "attr_0");

        ScanRequest.Builder sr3 = ScanRequest.builder().tableName(tableName);
        sr3.attributesToGet(Arrays.asList("attr_0", "attr_1", "Id1", "Id2", "title", "Reviews"));
        assertScanResults(sr3, 4, "attr_0");
    }

    @Test(timeout = 120000)
    public void testScanWithAttributesToGetAndValidationError() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.attributesToGet(Arrays.asList("attr_0", "title"));
        sr.projectionExpression("attr_0, title, Reviews");

        try {
            phoenixDBClientV2.scan(sr.build());
            Assert.fail("Expected ValidationException for both "
                    + "AttributesToGet and ProjectionExpression");
        } catch (DynamoDbException e) {
            Assert.assertEquals("Expected 400 status code for Phoenix", 400, e.statusCode());
        }

        try {
            dynamoDbClient.scan(sr.build());
            Assert.fail("Expected ValidationException for both "
                    + "AttributesToGet and ProjectionExpression");
        } catch (DynamoDbException e) {
            Assert.assertEquals("Expected 400 status code for DynamoDB", 400, e.statusCode());
        }
    }
}
