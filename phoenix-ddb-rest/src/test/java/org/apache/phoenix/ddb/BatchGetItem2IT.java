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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Comprehensive parameterized test for BatchGetItem 16MB size limit functionality.
 * Tests different combinations of table counts and item sizes (within DynamoDB's 400KB item limit
 * and 100 key total limit) to verify unprocessed key behavior when the 16MB response limit is exceeded.
 */
@RunWith(Parameterized.class)
public class BatchGetItem2IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchGetItem2IT.class);

    @Parameterized.Parameter(0)
    public int tableCount;

    @Parameterized.Parameter(1)
    public int itemsPerTable;

    @Parameterized.Parameter(2)
    public int itemSizeKB;


    @Parameterized.Parameters(name = "{0}tables_{1}items_{2}KB")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            // Single table scenarios - hit 16MB limit mid-table
            {1, 50, 350},  // 50 keys, ~17.5MB total, hit limit around item 47
            {1, 60, 300},  // 60 keys, ~18MB total, hit limit around item 55
            
            // Multi-table scenarios - hit limit across tables  
            {2, 25, 350},  // 50 keys, ~17.5MB total, likely hit in second table
            {3, 20, 300},  // 60 keys, ~18MB total, hit limit in second/third table
            {4, 15, 320},  // 60 keys, ~19.2MB total, hit limit early
            {5, 12, 280},  // 60 keys, ~16.8MB total, hit limit in later tables
            
            // Edge cases
            {2, 45, 390},  // 90 keys, close to 400KB limit, ~35MB total
            {4, 20, 200}   // 80 keys, many smaller items across tables
        });
    }

    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbClient phoenixDBClientV2;
    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;
    private static Random RANDOM = new Random();

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

        LOGGER.info("Started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();
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
    public void testBatchGetItemSizeLimits() {
        String testId = String.format("%dt_%di_%dKB", tableCount, itemsPerTable, itemSizeKB);
        
        int totalKeys = tableCount * itemsPerTable;
        Assert.assertTrue("Total keys must not exceed 100 per BatchGetItem request", totalKeys <= 100);
        Assert.assertTrue("Item size must not exceed 400KB per DynamoDB limit", itemSizeKB <= 400);

        List<String> tableNames = createTables(testId);
        populateTables(tableNames);

        // compare first response
        BatchGetItemRequest request = createBatchGetRequest(tableNames);
        BatchGetItemResponse phoenixResponse = phoenixDBClientV2.batchGetItem(request);
        BatchGetItemResponse dynamoResponse = dynamoDbClient.batchGetItem(request);
        validateFirstResponses(phoenixResponse, dynamoResponse);

        // collect all items and compare
        Map<String, List<Map<String, AttributeValue>>> phoenixAllItems = 
            performCompleteBatchGetItem(phoenixDBClientV2, request);
        Map<String, List<Map<String, AttributeValue>>> dynamoAllItems = 
            performCompleteBatchGetItem(dynamoDbClient, request);
        compareCompleteResults(phoenixAllItems, dynamoAllItems, testId);
    }

    private List<String> createTables(String testId) {
        List<String> tableNames = new ArrayList<>();
        
        for (int i = 0; i < tableCount; i++) {
            String tableName = String.format("batch_test_%s_t%d", testId, i);
            tableNames.add(tableName);
            
            ScalarAttributeType pkType = (i % 2 == 0) ? ScalarAttributeType.S : ScalarAttributeType.N;
            ScalarAttributeType skType = (i % 3 == 0) ? null : 
                (i % 2 == 0) ? ScalarAttributeType.N : ScalarAttributeType.S;
            
            CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(
                tableName, "pk", pkType, skType != null ? "sk" : null, skType);
                
            phoenixDBClientV2.createTable(createTableRequest);
            dynamoDbClient.createTable(createTableRequest);
        }
        
        return tableNames;
    }

    private void populateTables(List<String> tableNames) {
        for (int tableIdx = 0; tableIdx < tableNames.size(); tableIdx++) {
            String tableName = tableNames.get(tableIdx);
            
            for (int itemIdx = 0; itemIdx < itemsPerTable; itemIdx++) {
                Map<String, AttributeValue> item = createTestItem(tableIdx, itemIdx);
                
                PutItemRequest putRequest = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
                    
                phoenixDBClientV2.putItem(putRequest);
                dynamoDbClient.putItem(putRequest);
            }
        }
    }

    private Map<String, AttributeValue> createTestItem(int tableIdx, int itemIdx) {
        Map<String, AttributeValue> item = new HashMap<>();
        
        // Vary key types by table
        if (tableIdx % 2 == 0) {
            item.put("pk", AttributeValue.builder().s("pk_" + tableIdx + "_" + itemIdx).build());
        } else {
            item.put("pk", AttributeValue.builder().n(String.valueOf(tableIdx * 1000 + itemIdx)).build());
        }
        
        // Add sort key if table has one
        if (tableIdx % 3 != 0) {
            if (tableIdx % 2 == 0) {
                item.put("sk", AttributeValue.builder().n(String.valueOf(itemIdx)).build());
            } else {
                item.put("sk", AttributeValue.builder().s("sk_" + itemIdx).build());
            }
        }
        
        // Add large data to reach target size
        int targetBytes = itemSizeKB * 1024;
        int dataSize = Math.max(100, targetBytes - 300); // Account for metadata
        item.put("data", AttributeValue.builder().s(
            ReturnItemsTestUtil.generateLargeString(dataSize)).build());
        item.put("metadata", AttributeValue.builder().s("test_" + tableIdx + "_" + itemIdx).build());
        
        return item;
    }

    private BatchGetItemRequest createBatchGetRequest(List<String> tableNames) {
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        // Pick random projection type for this test run
        int projectionType = RANDOM.nextInt(4);

        for (int tableIdx = 0; tableIdx < tableNames.size(); tableIdx++) {
            String tableName = tableNames.get(tableIdx);
            List<Map<String, AttributeValue>> keys = new ArrayList<>();
            
            for (int itemIdx = 0; itemIdx < itemsPerTable; itemIdx++) {
                Map<String, AttributeValue> key = new HashMap<>();
                
                if (tableIdx % 2 == 0) {
                    key.put("pk", AttributeValue.builder().s("pk_" + tableIdx + "_" + itemIdx).build());
                } else {
                    key.put("pk", AttributeValue.builder().n(String.valueOf(tableIdx * 1000 + itemIdx)).build());
                }
                
                if (tableIdx % 3 != 0) {
                    if (tableIdx % 2 == 0) {
                        key.put("sk", AttributeValue.builder().n(String.valueOf(itemIdx)).build());
                    } else {
                        key.put("sk", AttributeValue.builder().s("sk_" + itemIdx).build());
                    }
                }
                
                keys.add(key);
            }
            KeysAndAttributes.Builder keysAndAttrsBuilder = KeysAndAttributes.builder().keys(keys);
            switch (projectionType) {
                case 0: // Use AttributesToGet without expression attribute names
                    keysAndAttrsBuilder.attributesToGet("pk", "sk", "metadata");
                    break;
                case 1: // Use ProjectionExpression with ExpressionAttributeNames
                    Map<String, String> exprAttrNames = new HashMap<>();
                    exprAttrNames.put("#pk", "pk");
                    exprAttrNames.put("#d", "data");
                    keysAndAttrsBuilder.projectionExpression("#pk, #d, metadata")
                        .expressionAttributeNames(exprAttrNames);
                    break;
                case 2: // Use ProjectionExpression without ExpressionAttributeNames
                    keysAndAttrsBuilder.projectionExpression("pk, sk, metadata");
                    break;
                case 3: // No projection (get all attributes)
                default:
                    // No projection attributes specified - returns all attributes
                    break;
            }
            
            requestItems.put(tableName, keysAndAttrsBuilder.build());
        }
        
        return BatchGetItemRequest.builder().requestItems(requestItems).build();
    }

    private void validateFirstResponses(BatchGetItemResponse phoenixResponse,
            BatchGetItemResponse dynamoResponse) {

        // Calculate total data size
        int totalExpectedItems = tableCount * itemsPerTable;
        int expectedTotalSizeMB = (totalExpectedItems * itemSizeKB) / 1024;

        // Count items in responses
        int phoenixItemCount = phoenixResponse.responses().values().stream()
                .mapToInt(List::size)
                .sum();
        int dynamoItemCount = dynamoResponse.responses().values().stream()
                .mapToInt(List::size)
                .sum();

        // Count unprocessed keys
        int phoenixUnprocessedCount = phoenixResponse.unprocessedKeys().values().stream()
                .mapToInt(ka -> ka.keys().size())
                .sum();
        int dynamoUnprocessedCount = dynamoResponse.unprocessedKeys().values().stream()
                .mapToInt(ka -> ka.keys().size())
                .sum();

        LOGGER.info("Phoenix: {} items returned, {} unprocessed", phoenixItemCount, phoenixUnprocessedCount);
        LOGGER.info("DynamoDB: {} items returned, {} unprocessed", dynamoItemCount, dynamoUnprocessedCount);

        // Verify total items consistency
        Assert.assertEquals("Total items should match",
                phoenixItemCount + phoenixUnprocessedCount,
                dynamoItemCount + dynamoUnprocessedCount);

        // If size limit was hit, verify both have unprocessed keys
        if (expectedTotalSizeMB > 16) {
            Assert.assertTrue("Phoenix should have unprocessed keys when size limit exceeded",
                    phoenixUnprocessedCount > 0);
            Assert.assertTrue("Dynamo should have unprocessed keys when size limit exceeded",
                    dynamoUnprocessedCount > 0);
        }

        // Verify response sizes are comparable (within 10% due to metadata differences)
        if (phoenixItemCount > 0 && dynamoItemCount > 0) {
            double sizeDifferencePercent = Math.abs(phoenixItemCount - dynamoItemCount) * 100.0 /
                    Math.max(phoenixItemCount, dynamoItemCount);
            Assert.assertTrue(String.format("Response sizes should be comparable: Phoenix=%d, DynamoDB=%d",
                    phoenixItemCount, dynamoItemCount), sizeDifferencePercent <= 10);
        }

        // Verify progress was made (at least some items returned)
        // Since all items are < 400KB, should always return at least some items
        Assert.assertTrue("Should return at least some items", phoenixItemCount > 0);
    }

    private Map<String, List<Map<String, AttributeValue>>> performCompleteBatchGetItem(
            DynamoDbClient client, BatchGetItemRequest initialRequest) {
        Map<String, List<Map<String, AttributeValue>>> allItems = new HashMap<>();
        BatchGetItemRequest currentRequest = initialRequest;
        do {
            BatchGetItemResponse response = client.batchGetItem(currentRequest);
            for (Map.Entry<String, List<Map<String, AttributeValue>>> entry : response.responses().entrySet()) {
                allItems.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
            }
            if (response.unprocessedKeys().isEmpty()) {
                break;
            }
            currentRequest = BatchGetItemRequest.builder()
                .requestItems(response.unprocessedKeys())
                .build();
        } while (true);
        return allItems;
    }

    private void compareCompleteResults(Map<String, List<Map<String, AttributeValue>>> phoenixItems,
            Map<String, List<Map<String, AttributeValue>>> dynamoItems, String testId) {
        Assert.assertEquals("Should have same number of tables", dynamoItems.size(), phoenixItems.size());
        for (String tableName : dynamoItems.keySet()) {
            List<Map<String, AttributeValue>> phoenixTableItems = phoenixItems.get(tableName);
            List<Map<String, AttributeValue>> dynamoTableItems = dynamoItems.get(tableName);
            Assert.assertNotNull("Phoenix should have results for table " + tableName, phoenixTableItems);
            Assert.assertEquals("Item count should match for table " + tableName, 
                dynamoTableItems.size(), phoenixTableItems.size());
            compareItemLists(phoenixTableItems, dynamoTableItems, testId + "_" + tableName);
        }
    }

    private void compareItemLists(List<Map<String, AttributeValue>> phoenixItems,
            List<Map<String, AttributeValue>> dynamoItems, String testContext) {
        
        List<Map<String, AttributeValue>> phoenixSorted
                = TestUtils.sortItemsByPartitionAndSortKey(phoenixItems, "pk", "sk");
        List<Map<String, AttributeValue>> dynamoSorted
                = TestUtils.sortItemsByPartitionAndSortKey(dynamoItems, "pk", "sk");
        
        Assert.assertTrue(testContext + " - All items should match exactly between Phoenix and DynamoDB",
            ItemComparator.areItemsEqual(phoenixSorted, dynamoSorted));
    }
}
