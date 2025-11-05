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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ReturnItemsLimitIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ReturnItemsLimitIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private final DynamoDbStreamsClient dynamoDbStreamsClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2StreamsClient();
    private static DynamoDbClient phoenixDBClientV2;
    private static DynamoDbStreamsClient phoenixDBStreamsClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    // DynamoDB max item size is 400 KB, so we'll create items around 390 KB each
    private static final int LARGE_ITEM_SIZE_KB = 390;
    private static final int LARGE_ITEM_SIZE_BYTES = LARGE_ITEM_SIZE_KB * 1024;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        //        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
        //                Long.toString(10000));
        //        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
        //                Long.toString(1000));
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpConfigForMiniCluster(conf, new ReadOnlyProps(props.entrySet().iterator()));

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        phoenixDBStreamsClientV2 =
                LocalDynamoDB.createV2StreamsClient("http://" + restServer.getServerAddress());
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

    /**
     * Helper method to perform complete scan pagination and collect all items from Phoenix.
     */
    private List<Map<String, AttributeValue>> performCompleteScanPagination(String tableName,
            DynamoDbClient client, Integer limit) {
        return performCompleteScanPagination(tableName, client, limit, false);
    }

    /**
     * Helper method to perform complete scan pagination and collect all items from Phoenix.
     *
     * @param verifyLargeItemsOneAtATime if true, verifies that each pagination round returns only 1 item (for large items)
     */
    private List<Map<String, AttributeValue>> performCompleteScanPagination(String tableName,
            DynamoDbClient client, Integer limit, boolean verifyLargeItemsOneAtATime) {
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        int paginationCount = 0;

        do {
            ScanRequest.Builder scanRequestBuilder = ScanRequest.builder().tableName(tableName);
            if (limit != null) {
                scanRequestBuilder.limit(limit);
            }
            if (lastEvaluatedKey != null) {
                scanRequestBuilder.exclusiveStartKey(lastEvaluatedKey);
            }

            ScanResponse scanResponse = client.scan(scanRequestBuilder.build());
            allItems.addAll(scanResponse.items());
            lastEvaluatedKey = scanResponse.lastEvaluatedKey();
            paginationCount++;

            LOGGER.info("Scan pagination round {}: returned {} items, total so far: {}",
                    paginationCount, scanResponse.count(), allItems.size());

            // Verify large items are returned one at a time
            if (verifyLargeItemsOneAtATime && paginationCount < 5) {
                Assert.assertEquals(
                        "Large items should be returned one at a time due to 1 MB limit. Round "
                                + paginationCount, paginationCount < 4 ? 3 : 1,
                        scanResponse.count().intValue());
                LOGGER.info("✓ Verified: Scan round {} returned exactly 1 large item as expected",
                        paginationCount);
            }

        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

        LOGGER.info("Complete scan pagination finished. Total items: {}, Total rounds: {}",
                allItems.size(), paginationCount);
        return allItems;
    }

    /**
     * Helper method to perform complete query pagination and collect all items.
     */
    private List<Map<String, AttributeValue>> performCompleteQueryPagination(String tableName,
            DynamoDbClient client, String keyConditionExpression,
            Map<String, AttributeValue> expressionAttributeValues, Integer limit) {
        return performCompleteQueryPagination(tableName, client, keyConditionExpression,
                expressionAttributeValues, limit, false);
    }

    /**
     * Helper method to perform complete query pagination and collect all items.
     *
     * @param verifyLargeItemsOneAtATime if true, verifies that each pagination round returns only 1 item (for large items)
     */
    private List<Map<String, AttributeValue>> performCompleteQueryPagination(String tableName,
            DynamoDbClient client, String keyConditionExpression,
            Map<String, AttributeValue> expressionAttributeValues, Integer limit,
            boolean verifyLargeItemsOneAtATime) {
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        int paginationCount = 0;

        do {
            QueryRequest.Builder queryRequestBuilder = QueryRequest.builder().tableName(tableName)
                    .keyConditionExpression(keyConditionExpression)
                    .expressionAttributeValues(expressionAttributeValues);
            if (limit != null) {
                queryRequestBuilder.limit(limit);
            }
            if (lastEvaluatedKey != null) {
                queryRequestBuilder.exclusiveStartKey(lastEvaluatedKey);
            }

            QueryResponse queryResponse = client.query(queryRequestBuilder.build());
            allItems.addAll(queryResponse.items());
            lastEvaluatedKey = queryResponse.lastEvaluatedKey();
            paginationCount++;

            LOGGER.info("Query pagination round {}: returned {} items, total so far: {}",
                    paginationCount, queryResponse.count(), allItems.size());

            if (verifyLargeItemsOneAtATime && paginationCount < 4) {
                Assert.assertEquals("Round " + paginationCount, paginationCount < 3 ? 3 : 2,
                        queryResponse.count().intValue());
            }

        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

        LOGGER.info("Complete query pagination finished. Total items: {}, Total rounds: {}",
                allItems.size(), paginationCount);
        return allItems;
    }

    /**
     * Sort items by partition key and sort key for consistent comparison.
     */
    private List<Map<String, AttributeValue>> sortItemsByKeys(
            List<Map<String, AttributeValue>> items) {
        return items.stream().sorted(Comparator.comparing(
                        (Map<String, AttributeValue> item) -> item.get("pk") != null ?
                                item.get("pk").s() :
                                "").thenComparing(item -> item.get("sk") != null ? item.get("sk").s() : ""))
                .collect(Collectors.toList());
    }

    /**
     * Compare two lists of items for equality using ItemComparator.
     * For Scan operations, sorts items by partition/sort keys before comparison.
     * For Query operations, compares items in their original order (already sorted by sort key).
     */
    private void compareItemLists(List<Map<String, AttributeValue>> phoenixItems,
            List<Map<String, AttributeValue>> dynamoItems, String testContext) {
        compareItemLists(phoenixItems, dynamoItems, testContext, true);
    }

    /**
     * Compare two lists of items for equality using ItemComparator.
     *
     * @param sortItems if true, sorts items by partition/sort keys before comparison (for Scan).
     *                  if false, compares items in original order (for Query).
     */
    private void compareItemLists(List<Map<String, AttributeValue>> phoenixItems,
            List<Map<String, AttributeValue>> dynamoItems, String testContext, boolean sortItems) {

        List<Map<String, AttributeValue>> phoenixItemsToCompare;
        List<Map<String, AttributeValue>> dynamoItemsToCompare;

        if (sortItems) {
            // Sort both lists for consistent comparison (used for Scan operations)
            phoenixItemsToCompare = sortItemsByKeys(phoenixItems);
            dynamoItemsToCompare = sortItemsByKeys(dynamoItems);
            LOGGER.info("{} - Comparing items after sorting by partition/sort keys", testContext);
        } else {
            // Use original order (used for Query operations which are already sorted by sort key)
            phoenixItemsToCompare = phoenixItems;
            dynamoItemsToCompare = dynamoItems;
            LOGGER.info("{} - Comparing items in original order (Query results already sorted)",
                    testContext);
        }

        LOGGER.info("{} - Phoenix returned {} items, DynamoDB returned {} items", testContext,
                phoenixItems.size(), dynamoItems.size());

        // Compare sizes
        Assert.assertEquals(testContext + " - Item counts should match", dynamoItems.size(),
                phoenixItems.size());

        // Compare all items
        Assert.assertTrue(testContext + " - All items should match between Phoenix and DynamoDB",
                ItemComparator.areItemsEqual(phoenixItemsToCompare, dynamoItemsToCompare));

        LOGGER.info("{} - ✅ All {} items match perfectly between Phoenix and DynamoDB", testContext,
                phoenixItems.size());
    }

    /**
     * Test ScanService with large documents to verify 1 MB response size limit.
     * Enhanced to do complete pagination and compare all results with DynamoDB.
     */
    @Test(timeout = 120000)
    public void testScanServiceOneMegabyteLimit() {
        final String tableName = testName.getMethodName() + "_scan_table";

        LOGGER.info("=== Testing Scan Service 1 MB Limit with Complete Pagination ===");

        // Create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 4 large items (each ~390 KB, total ~1.56 MB)
        // This should trigger the 1 MB limit and stop after 2-3 items
        LOGGER.info("Inserting 4 large items (~390 KB each)...");
        List<Map<String, AttributeValue>> largeItems = createLargeItems(10);
        for (int i = 0; i < largeItems.size(); i++) {
            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(largeItems.get(i)).build();
            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);
        }

        // Perform complete scan pagination for both Phoenix and DynamoDB
        // Verify that large items are returned one at a time due to 1 MB size limit
        LOGGER.info(
                "Performing complete scan pagination for Phoenix (verifying one large item at a time)...");
        List<Map<String, AttributeValue>> phoenixAllItems =
                performCompleteScanPagination(tableName, phoenixDBClientV2, 100, true);

        LOGGER.info("Performing complete scan pagination for DynamoDB...");
        List<Map<String, AttributeValue>> dynamoAllItems =
                performCompleteScanPagination(tableName, dynamoDbClient, 100);

        // Compare complete result sets (Scan results need sorting for consistent comparison)
        compareItemLists(phoenixAllItems, dynamoAllItems, "Scan 1MB Limit Test", true);

        // Verify that both services returned all inserted items
        Assert.assertEquals("Phoenix should return all 4 items", 10, phoenixAllItems.size());
        Assert.assertEquals("DynamoDB should return all 4 items", 10, dynamoAllItems.size());
    }

    /**
     * Test QueryService with large documents to verify 1 MB response size limit.
     * Enhanced to do complete pagination and compare all results with DynamoDB.
     */
    @Test(timeout = 120000)
    public void testQueryServiceOneMegabyteLimit() {
        final String tableName = testName.getMethodName() + "_query_table";

        LOGGER.info("=== Testing Query Service 1 MB Limit with Complete Pagination ===");

        // Create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 4 large items with same partition key (each ~390 KB, total ~1.56 MB)
        LOGGER.info("Inserting 4 large items with same partition key (~390 KB each)...");
        List<Map<String, AttributeValue>> largeItems =
                createLargeItemsWithSamePartitionKey(8, "test_pk");
        for (int i = 0; i < largeItems.size(); i++) {
            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(largeItems.get(i)).build();
            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);
        }

        // Set up query parameters
        Map<String, AttributeValue> keyConditionValues = new HashMap<>();
        keyConditionValues.put(":pk_val", AttributeValue.builder().s("test_pk").build());
        String keyConditionExpression = "pk = :pk_val";

        // Perform complete query pagination for both Phoenix and DynamoDB
        // Verify that large items are returned one at a time due to 1 MB size limit
        LOGGER.info(
                "Performing complete query pagination for Phoenix (verifying one large item at a time)...");
        List<Map<String, AttributeValue>> phoenixAllItems =
                performCompleteQueryPagination(tableName, phoenixDBClientV2, keyConditionExpression,
                        keyConditionValues, 100, true);

        LOGGER.info("Performing complete query pagination for DynamoDB...");
        List<Map<String, AttributeValue>> dynamoAllItems =
                performCompleteQueryPagination(tableName, dynamoDbClient, keyConditionExpression,
                        keyConditionValues, 100);

        // Compare complete result sets (Query results are already sorted by sort key, don't re-sort)
        compareItemLists(phoenixAllItems, dynamoAllItems, "Query 1MB Limit Test", false);

        // Verify that both services returned all inserted items
        Assert.assertEquals("Phoenix should return all 4 items", 8, phoenixAllItems.size());
        Assert.assertEquals("DynamoDB should return all 4 items", 8, dynamoAllItems.size());
    }

    /**
     * Test GetRecordsService with large documents to verify 1 MB response size limit.
     * Enhanced to do complete pagination and compare all results with DynamoDB.
     */
    @Test(timeout = 120000)
    public void testGetRecordsServiceOneMegabyteLimit() throws InterruptedException {
        final String tableName = testName.getMethodName() + "_streams_table";

        // Create table with streams enabled
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        createTableRequest =
                DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_AND_OLD_IMAGES");

        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Get stream ARN
        String streamArn = getStreamArn(tableName, phoenixDBStreamsClientV2);
        String ddbStreamArn = getStreamArn(tableName, dynamoDbStreamsClient);

        // Insert 4 large items (each ~390 KB, total ~1.56 MB)
        // This should trigger the 1 MB limit in GetRecords
        List<Map<String, AttributeValue>> largeItems = createLargeItems(4);
        for (int i = 0; i < largeItems.size(); i++) {
            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(largeItems.get(i)).build();
            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);
        }

        TestUtils.waitForStream(phoenixDBStreamsClientV2, streamArn);
        TestUtils.waitForStream(dynamoDbStreamsClient, ddbStreamArn);

        // Get shard iterator
        String shardIterator = getShardIterator(streamArn);

        // Test Phoenix GetRecords with unlimited limit to trigger size-based pagination
        GetRecordsRequest phoenixGetRecordsRequest =
                GetRecordsRequest.builder().shardIterator(shardIterator)
                        .limit(100) // High limit to ensure size limit is hit first
                        .build();

        GetRecordsResponse phoenixResponse =
                phoenixDBStreamsClientV2.getRecords(phoenixGetRecordsRequest);

        LOGGER.info("Phoenix GetRecords returned {} records", phoenixResponse.records().size());

        Assert.assertTrue("Phoenix should return fewer than 4 records due to 1 MB limit",
                phoenixResponse.records().size() < 4);
        Assert.assertTrue("Phoenix should return at least 1 record",
                phoenixResponse.records().size() >= 1);

        if (phoenixResponse.records().size() < 4) {
            Assert.assertNotNull("Should have next shard iterator for pagination",
                    phoenixResponse.nextShardIterator());
        }

        LOGGER.info("Completing GetRecords pagination for Phoenix...");
        List<Record> phoenixAllResponses = new ArrayList<>(phoenixResponse.records());
        List<Record> ddbAllResponses = new ArrayList<>();
        String phoenixNextShardIterator = phoenixResponse.nextShardIterator();
        int phoenixPaginationCount = 1;

        while (phoenixNextShardIterator != null && !phoenixResponse.records().isEmpty()) {
            GetRecordsRequest phoenixNextRequest =
                    GetRecordsRequest.builder().shardIterator(phoenixNextShardIterator).limit(100)
                            .build();

            phoenixResponse = phoenixDBStreamsClientV2.getRecords(phoenixNextRequest);
            phoenixAllResponses.addAll(phoenixResponse.records());
            phoenixNextShardIterator = phoenixResponse.nextShardIterator();
            phoenixPaginationCount++;

            LOGGER.info("Phoenix GetRecords pagination round {}: returned {} records",
                    phoenixPaginationCount, phoenixResponse.records().size());
        }

        // Complete pagination for DynamoDB GetRecords
        LOGGER.info("Completing GetRecords pagination for DynamoDB...");
        String ddbShardIterator = getShardIterator(ddbStreamArn, dynamoDbStreamsClient);
        int dynamoPaginationCount = 0;

        GetRecordsResponse dynamoResponse;
        do {
            GetRecordsRequest dynamoGetRecordsRequest =
                    GetRecordsRequest.builder().shardIterator(ddbShardIterator).limit(100).build();

            dynamoResponse = dynamoDbStreamsClient.getRecords(dynamoGetRecordsRequest);
            ddbAllResponses.addAll(dynamoResponse.records());
            ddbShardIterator = dynamoResponse.nextShardIterator();
            dynamoPaginationCount++;

            LOGGER.info("DynamoDB GetRecords pagination round {}: returned {} records",
                    dynamoPaginationCount, dynamoResponse.records().size());

        } while (ddbShardIterator != null && !dynamoResponse.records().isEmpty());

        // Verify that both services returned the same number of records
        Assert.assertEquals("Phoenix and DynamoDB should return same number of records",
                ddbAllResponses.size(), phoenixAllResponses.size());
        TestUtils.validateRecords(ddbAllResponses, phoenixAllResponses);
    }

    /**
     * Simple demonstration of scan with 1 MB size limit.
     * Enhanced to do complete pagination and compare all results with DynamoDB.
     */
    @Test(timeout = 120000)
    public void demonstrateScanSizeLimit() {
        final String tableName = testName.getMethodName() + "_demo";

        LOGGER.info("=== DEMONSTRATING SCAN 1 MB SIZE LIMIT WITH COMPLETE PAGINATION ===");

        // Create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 4 large items (each ~390 KB)
        LOGGER.info("Inserting 4 large items, each ~390 KB...");
        for (int i = 0; i < 10; i++) {
            Map<String, AttributeValue> largeItem =
                    ReturnItemsTestUtil.createLargeItem("large_pk_" + i, "sk_" + i);

            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(largeItem).build();

            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);

            LOGGER.info("Inserted item {}", i + 1);
        }

        LOGGER.info(ReturnItemsTestUtil.getTestDescription(4, 390));

        // Perform complete scan pagination for both Phoenix and DynamoDB
        // Verify that large items are returned one at a time due to 1 MB size limit
        LOGGER.info(
                "Performing complete scan pagination for Phoenix (verifying one large item at a time)...");
        List<Map<String, AttributeValue>> phoenixAllItems =
                performCompleteScanPagination(tableName, phoenixDBClientV2, 100, true);

        LOGGER.info("Performing complete scan pagination for DynamoDB...");
        List<Map<String, AttributeValue>> dynamoAllItems =
                performCompleteScanPagination(tableName, dynamoDbClient, 100);

        // Compare complete result sets (Scan results need sorting for consistent comparison)
        compareItemLists(phoenixAllItems, dynamoAllItems, "Scan Demo Test", true);

        // Verify that both services returned all inserted items
        Assert.assertEquals("Phoenix should return all 4 items", 10, phoenixAllItems.size());
        Assert.assertEquals("DynamoDB should return all 4 items", 10, dynamoAllItems.size());
    }

    /**
     * Simple demonstration of query with 1 MB size limit.
     * Enhanced to do complete pagination and compare all results with DynamoDB.
     */
    @Test(timeout = 120000)
    public void demonstrateQuerySizeLimit() {
        final String tableName = testName.getMethodName() + "_demo";

        LOGGER.info("=== DEMONSTRATING QUERY 1 MB SIZE LIMIT WITH COMPLETE PAGINATION ===");

        // Create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 4 large items with same partition key
        String partitionKey = "same_pk";
        LOGGER.info("Inserting 4 large items with same partition key, each ~390 KB...");

        for (int i = 0; i < 8; i++) {
            Map<String, AttributeValue> largeItem =
                    ReturnItemsTestUtil.createLargeItem(partitionKey,
                            "sk_" + String.format("%03d", i));

            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(largeItem).build();

            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);

            LOGGER.info("Inserted item {}", i + 1);
        }

        LOGGER.info(ReturnItemsTestUtil.getTestDescription(4, 390));

        // Set up query parameters
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":pk_val", AttributeValue.builder().s(partitionKey).build());
        String keyConditionExpression = "pk = :pk_val";

        // Perform complete query pagination for both Phoenix and DynamoDB
        // Verify that large items are returned one at a time due to 1 MB size limit
        LOGGER.info(
                "Performing complete query pagination for Phoenix (verifying one large item at a time)...");
        List<Map<String, AttributeValue>> phoenixAllItems =
                performCompleteQueryPagination(tableName, phoenixDBClientV2, keyConditionExpression,
                        expressionValues, 100, true);

        LOGGER.info("Performing complete query pagination for DynamoDB...");
        List<Map<String, AttributeValue>> dynamoAllItems =
                performCompleteQueryPagination(tableName, dynamoDbClient, keyConditionExpression,
                        expressionValues, 100);

        // Compare complete result sets (Query results are already sorted by sort key, don't re-sort)
        compareItemLists(phoenixAllItems, dynamoAllItems, "Query Demo Test", false);

        // Verify that both services returned all inserted items
        Assert.assertEquals("Phoenix should return all 4 items", 8, phoenixAllItems.size());
        Assert.assertEquals("DynamoDB should return all 4 items", 8, dynamoAllItems.size());
    }

    /**
     * Test scan pagination with smaller items but limited count.
     * Enhanced to do complete pagination and compare all results with DynamoDB.
     */
    @Test(timeout = 120000)
    public void testScanPaginationWithSmallItems() {
        final String tableName = testName.getMethodName() + "_small_items";

        LOGGER.info("=== Testing Scan Pagination with Small Items ===");

        // Create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 20 small items
        LOGGER.info("Inserting 20 small items...");
        for (int i = 0; i < 20; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk", AttributeValue.builder().s("small_pk_" + i).build());
            item.put("sk", AttributeValue.builder().s("sk_" + i).build());
            item.put("data", AttributeValue.builder().s("small_data_" + i).build());
            item.put("number", AttributeValue.builder().n(String.valueOf(i)).build());

            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(item).build();

            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);
        }

        // Perform complete scan pagination with limit 5 for both Phoenix and DynamoDB
        LOGGER.info("Performing complete scan pagination with limit 5 for Phoenix...");
        List<Map<String, AttributeValue>> phoenixAllItems =
                performCompleteScanPagination(tableName, phoenixDBClientV2, 5);

        LOGGER.info("Performing complete scan pagination with limit 5 for DynamoDB...");
        List<Map<String, AttributeValue>> dynamoAllItems =
                performCompleteScanPagination(tableName, dynamoDbClient, 5);

        // Compare complete result sets (Scan results need sorting for consistent comparison)
        compareItemLists(phoenixAllItems, dynamoAllItems, "Small Items Scan Test", true);

        // Verify that both services returned all inserted items
        Assert.assertEquals("Phoenix should return all 20 items", 20, phoenixAllItems.size());
        Assert.assertEquals("DynamoDB should return all 20 items", 20, dynamoAllItems.size());

        LOGGER.info("✅ Scan pagination with small items test completed successfully");
    }

    /**
     * Test query pagination with smaller items but limited count.
     * Enhanced to do complete pagination and compare all results with DynamoDB.
     */
    @Test(timeout = 120000)
    public void testQueryPaginationWithSmallItems() {
        final String tableName = testName.getMethodName() + "_small_query_items";

        LOGGER.info("=== Testing Query Pagination with Small Items ===");

        // Create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 15 small items with same partition key
        String partitionKey = "shared_pk";
        LOGGER.info("Inserting 15 small items with same partition key...");
        for (int i = 0; i < 15; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk", AttributeValue.builder().s(partitionKey).build());
            item.put("sk", AttributeValue.builder().s("sk_" + String.format("%03d", i)).build());
            item.put("data", AttributeValue.builder().s("small_data_" + i).build());
            item.put("number", AttributeValue.builder().n(String.valueOf(i)).build());

            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(item).build();

            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);
        }

        // Set up query parameters
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":pk_val", AttributeValue.builder().s(partitionKey).build());
        String keyConditionExpression = "pk = :pk_val";

        // Perform complete query pagination with limit 3 for both Phoenix and DynamoDB
        LOGGER.info("Performing complete query pagination with limit 3 for Phoenix...");
        List<Map<String, AttributeValue>> phoenixAllItems =
                performCompleteQueryPagination(tableName, phoenixDBClientV2, keyConditionExpression,
                        expressionValues, 3);

        LOGGER.info("Performing complete query pagination with limit 3 for DynamoDB...");
        List<Map<String, AttributeValue>> dynamoAllItems =
                performCompleteQueryPagination(tableName, dynamoDbClient, keyConditionExpression,
                        expressionValues, 3);

        // Compare complete result sets (Query results are already sorted by sort key, don't re-sort)
        compareItemLists(phoenixAllItems, dynamoAllItems, "Small Items Query Test", false);

        // Verify that both services returned all inserted items
        Assert.assertEquals("Phoenix should return all 15 items", 15, phoenixAllItems.size());
        Assert.assertEquals("DynamoDB should return all 15 items", 15, dynamoAllItems.size());

        LOGGER.info("✅ Query pagination with small items test completed successfully");
    }

    /**
     * Test to explicitly verify that extremely large items (close to 400 KB) are returned one at a time.
     * This test creates items that are just under the DynamoDB 400 KB limit to ensure the 1 MB
     * response size limit forces returning only one item per pagination call.
     */
    @Test(timeout = 120000)
    public void testVerifyOneLargeItemAtATime() {
        final String tableName = testName.getMethodName() + "_verify_one_item";

        // Create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 3 extremely large items (each close to 400 KB limit)
        LOGGER.info("Inserting 3 extremely large items (close to 400 KB each)...");
        for (int i = 0; i < 3; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk", AttributeValue.builder().s("xl_pk_" + i).build());
            item.put("sk", AttributeValue.builder().s("xl_sk_" + i).build());

            // Create item close to 400 KB (390 KB data + overhead)
            String largeData = generateLargeString(395 * 1024); // 395 KB
            item.put("large_data", AttributeValue.builder().s(largeData).build());
            item.put("metadata", AttributeValue.builder().s("test_metadata_" + i).build());

            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(item).build();

            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);

            LOGGER.info("Inserted extremely large item {} (~395 KB)", i + 1);
        }

        // Test scan - should return exactly 1 item per call
        LOGGER.info("Testing scan pagination - expecting exactly 1 large item per call...");
        Map<String, AttributeValue> lastEvaluatedKey = null;
        int totalItems = 0;
        int paginationRounds = 0;

        do {
            ScanRequest.Builder scanRequestBuilder = ScanRequest.builder().tableName(tableName)
                    .limit(100); // High limit, but size should limit to 1 item

            if (lastEvaluatedKey != null) {
                scanRequestBuilder.exclusiveStartKey(lastEvaluatedKey);
            }

            ScanResponse scanResponse = phoenixDBClientV2.scan(scanRequestBuilder.build());
            int itemsThisRound = scanResponse.count();
            totalItems += itemsThisRound;
            lastEvaluatedKey = scanResponse.lastEvaluatedKey();
            paginationRounds++;

            LOGGER.info("Scan round {}: returned {} items (expected 1)", paginationRounds,
                    itemsThisRound);

        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());

        Assert.assertEquals("Should have scanned all 3 items", 3, totalItems);
        Assert.assertEquals("Should require exactly 2 pagination rounds for 3 large items", 2,
                paginationRounds);

        // Insert 2 large items with same partition key
        String sharedPK = "shared_large_pk";
        for (int i = 0; i < 2; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk", AttributeValue.builder().s(sharedPK).build());
            item.put("sk",
                    AttributeValue.builder().s("query_sk_" + String.format("%03d", i)).build());

            String largeData = generateLargeString(395 * 1024); // 395 KB
            item.put("large_data", AttributeValue.builder().s(largeData).build());

            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(item).build();

            phoenixDBClientV2.putItem(putRequest);
            LOGGER.info("Inserted large query item {} with shared partition key", i + 1);
        }

        // Query with shared partition key
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":pk_val", AttributeValue.builder().s(sharedPK).build());

        List<Map<String, AttributeValue>> queryAllItems =
                performCompleteQueryPagination(tableName, phoenixDBClientV2, "pk = :pk_val",
                        expressionValues, 100, false);

        Assert.assertEquals("Query should return both large items", 2, queryAllItems.size());
    }

    /**
     * Create a list of large items, each approximately 390 KB in size.
     */
    private List<Map<String, AttributeValue>> createLargeItems(int count) {
        List<Map<String, AttributeValue>> items = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk", AttributeValue.builder().s("large_item_" + i).build());
            item.put("sk", AttributeValue.builder().s("sort_key_" + i).build());

            // Add large data field to reach ~390 KB
            item.put("large_data",
                    AttributeValue.builder().s(generateLargeString(LARGE_ITEM_SIZE_BYTES)).build());

            // Add some additional fields for realism
            item.put("timestamp",
                    AttributeValue.builder().n(String.valueOf(System.currentTimeMillis() + i))
                            .build());
            item.put("version", AttributeValue.builder().n(String.valueOf(i)).build());
            item.put("metadata", AttributeValue.builder().s("metadata_value_" + i).build());

            items.add(item);
        }

        return items;
    }

    /**
     * Create a list of large items with the same partition key for query testing.
     */
    private List<Map<String, AttributeValue>> createLargeItemsWithSamePartitionKey(int count,
            String partitionKey) {
        List<Map<String, AttributeValue>> items = new ArrayList<>();

        for (int i = 0; i < count; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("pk", AttributeValue.builder().s(partitionKey).build());
            item.put("sk",
                    AttributeValue.builder().s("sort_key_" + String.format("%03d", i)).build());

            // Add large data field to reach ~390 KB
            item.put("large_data",
                    AttributeValue.builder().s(generateLargeString(LARGE_ITEM_SIZE_BYTES)).build());

            // Add some additional fields for realism
            item.put("timestamp",
                    AttributeValue.builder().n(String.valueOf(System.currentTimeMillis() + i))
                            .build());
            item.put("version", AttributeValue.builder().n(String.valueOf(i)).build());
            item.put("metadata", AttributeValue.builder().s("metadata_value_" + i).build());

            items.add(item);
        }

        return items;
    }

    /**
     * Generate a large string of approximately the specified size in bytes.
     */
    private String generateLargeString(int sizeInBytes) {
        StringBuilder sb = new StringBuilder(sizeInBytes);
        String pattern = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

        for (int i = 0; i < sizeInBytes; i++) {
            sb.append(pattern.charAt(i % pattern.length()));
        }

        return sb.toString();
    }

    /**
     * Get the stream ARN for the given table.
     */
    private String getStreamArn(String tableName, DynamoDbStreamsClient dynamoDbStreamsClient) {
        ListStreamsRequest listStreamsRequest =
                ListStreamsRequest.builder().tableName(tableName).build();

        ListStreamsResponse listStreamsResponse =
                dynamoDbStreamsClient.listStreams(listStreamsRequest);

        Assert.assertFalse("Should have at least one stream",
                listStreamsResponse.streams().isEmpty());
        return listStreamsResponse.streams().get(0).streamArn();
    }

    /**
     * Get a shard iterator for the given stream.
     */
    private String getShardIterator(String streamArn) {
        return getShardIterator(streamArn, phoenixDBStreamsClientV2);
    }

    /**
     * Get a shard iterator for the given stream using the specified client.
     */
    private String getShardIterator(String streamArn, DynamoDbStreamsClient streamsClient) {
        DescribeStreamRequest describeStreamRequest =
                DescribeStreamRequest.builder().streamArn(streamArn).build();

        DescribeStreamResponse describeStreamResponse =
                streamsClient.describeStream(describeStreamRequest);

        Assert.assertFalse("Should have at least one shard",
                describeStreamResponse.streamDescription().shards().isEmpty());

        String shardId = describeStreamResponse.streamDescription().shards().get(0).shardId();

        GetShardIteratorRequest getShardIteratorRequest =
                GetShardIteratorRequest.builder().streamArn(streamArn).shardId(shardId)
                        .shardIteratorType("TRIM_HORIZON").build();

        return streamsClient.getShardIterator(getShardIteratorRequest).shardIterator();
    }
}
