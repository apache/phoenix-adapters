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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.jdbc.PhoenixTestDriver;
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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.core.SdkBytes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests scan pagination on table with different limits and different combinations of hash and sort key data types.
 */
@RunWith(Parameterized.class)
public class ScanExclusiveStartKeyIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanExclusiveStartKeyIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();
    
    // Test parameters
    public int scanLimit;
    public KeyTypeConfig keyTypeConfig;
    public boolean withFilter;

    // Configuration for different key type combinations
    public static class KeyTypeConfig {
        public final String name;
        public final ScalarAttributeType hashKeyType;
        public final ScalarAttributeType sortKeyType;
        public final String hashKeyName;
        public final String sortKeyName;

        public KeyTypeConfig(String name, ScalarAttributeType hashKeyType, ScalarAttributeType sortKeyType) {
            this.name = name;
            this.hashKeyType = hashKeyType;
            this.sortKeyType = sortKeyType;
            this.hashKeyName = "partition_key";
            this.sortKeyName = "sort_key";
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Parameterized.Parameters(name = "limit_{0}_keyTypes_{1}_filter_{2}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        
        // Different scan limits to test
        int[] scanLimits = {1, 2, 3, 4, 5, 6, 7, 8};
        
        // Different key type combinations
        KeyTypeConfig[] keyConfigs = {
            new KeyTypeConfig("S_N", ScalarAttributeType.S, ScalarAttributeType.N), // String + Number
            new KeyTypeConfig("B_B", ScalarAttributeType.B, ScalarAttributeType.B), // Binary + Binary  
            new KeyTypeConfig("B_N", ScalarAttributeType.B, ScalarAttributeType.N), // Binary + Number
            new KeyTypeConfig("N_S", ScalarAttributeType.N, ScalarAttributeType.S), // Number + String
            new KeyTypeConfig("N_N", ScalarAttributeType.N, ScalarAttributeType.N), // Number + Number
            new KeyTypeConfig("S_S", ScalarAttributeType.S, ScalarAttributeType.S), // String + String
            new KeyTypeConfig("S_B", ScalarAttributeType.S, ScalarAttributeType.B)  // String + Binary
        };
        
        for (int limit : scanLimits) {
            for (KeyTypeConfig config : keyConfigs) {
                for (boolean filter : new boolean[]{false, true}) {
                    parameters.add(new Object[]{limit, config, filter});
                }
            }
        }
        
        return parameters;
    }

    public ScanExclusiveStartKeyIT(int scanLimit, KeyTypeConfig keyTypeConfig, boolean withFilter) {
        this.scanLimit = scanLimit;
        this.keyTypeConfig = keyTypeConfig;
        this.withFilter = withFilter;
    }

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

    /**
     * Test getExclusiveStartKeyConditionForScan() logic by creating a table with hash and sort keys,
     * inserting 56 items (7 hash keys with 8 sort keys each), and scanning with different limits.
     * This tests both cases:
     * 1. When last evaluated key has only hash key
     * 2. When last evaluated key has both hash key and sort key
     */
    @Test(timeout = 120000)
    public void testScanExclusiveStartKeyPagination() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_") + 
                "_limit_" + scanLimit + "_" + keyTypeConfig.name + "_filter_" + withFilter;
        
        final int totalItems = 56;

        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, keyTypeConfig.hashKeyName,
                        keyTypeConfig.hashKeyType, keyTypeConfig.sortKeyName, keyTypeConfig.sortKeyType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        for (int hashIndex = 0; hashIndex < 7; hashIndex++) {
            for (int sortIndex = 0; sortIndex < 8; sortIndex++) {
                Map<String, AttributeValue> item = createTestItem(hashIndex, sortIndex, keyTypeConfig);
                PutItemRequest putItemRequest = PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .build();
                phoenixDBClientV2.putItem(putItemRequest);
                dynamoDbClient.putItem(putItemRequest);
            }
        }

        List<Map<String, AttributeValue>> phoenixItems = new ArrayList<>();
        List<Map<String, AttributeValue>> dynamoItems = new ArrayList<>();
        
        Map<String, AttributeValue> phoenixLastKey = null;
        Map<String, AttributeValue> dynamoLastKey = null;
        
        int phoenixPaginationCount = 0;
        int dynamoPaginationCount = 0;

        do {
            ScanRequest.Builder dynamoScanRequest = ScanRequest.builder()
                    .tableName(tableName)
                    .limit(scanLimit);
            applyFilter(dynamoScanRequest);
            if (dynamoLastKey != null && !dynamoLastKey.isEmpty()) {
                dynamoScanRequest.exclusiveStartKey(dynamoLastKey);
            }
            ScanResponse dynamoResult = dynamoDbClient.scan(dynamoScanRequest.build());
            dynamoItems.addAll(dynamoResult.items());
            dynamoLastKey = dynamoResult.lastEvaluatedKey();
            dynamoPaginationCount++;
            LOGGER.info("DynamoDB scan iteration {}, returned {} items, last key: {}", 
                    dynamoPaginationCount, dynamoResult.count(), dynamoLastKey);
        } while (dynamoLastKey != null && !dynamoLastKey.isEmpty());

        do {
            ScanRequest.Builder phoenixScanRequest = ScanRequest.builder()
                    .tableName(tableName)
                    .limit(scanLimit);
            applyFilter(phoenixScanRequest);
            if (phoenixLastKey != null && !phoenixLastKey.isEmpty()) {
                phoenixScanRequest.exclusiveStartKey(phoenixLastKey);
            }
            ScanResponse phoenixResult = phoenixDBClientV2.scan(phoenixScanRequest.build());
            phoenixItems.addAll(phoenixResult.items());
            phoenixLastKey = phoenixResult.lastEvaluatedKey();
            phoenixPaginationCount++;
            LOGGER.info("Phoenix scan iteration {}, returned {} items, last key: {}",
                    phoenixPaginationCount, phoenixResult.count(), phoenixLastKey);
        } while (phoenixLastKey != null && !phoenixLastKey.isEmpty());

        Assert.assertEquals("Phoenix and DynamoDB should return same number of items",
                dynamoItems.size(), phoenixItems.size());
        Assert.assertFalse("Should return some items", phoenixItems.isEmpty());
        if (withFilter) {
            Assert.assertTrue("Filter should reduce the result set",
                    phoenixItems.size() < totalItems);
        } else {
            Assert.assertEquals("Should return all items without filter",
                    totalItems, phoenixItems.size());
        }

        Assert.assertTrue("Phoenix pagination should complete", phoenixPaginationCount > 0);
        Assert.assertTrue("DynamoDB pagination should complete", dynamoPaginationCount > 0);

        List<Map<String, AttributeValue>> sortedPhoenixItems =
                TestUtils.sortItemsByPartitionAndSortKey(phoenixItems, keyTypeConfig.hashKeyName, keyTypeConfig.sortKeyName);
        List<Map<String, AttributeValue>> sortedDynamoItems =
                TestUtils.sortItemsByPartitionAndSortKey(dynamoItems, keyTypeConfig.hashKeyName, keyTypeConfig.sortKeyName);
        Assert.assertTrue("Phoenix and DynamoDB should return identical items when sorted",
                ItemComparator.areItemsEqual(sortedPhoenixItems, sortedDynamoItems));
    }

    /**
     * Create a test item with the given partition key and sort key based on the key type configuration.
     */
    private void applyFilter(ScanRequest.Builder sr) {
        if (withFilter) {
            Map<String, String> names = new HashMap<>();
            names.put("#nf", "num_field");
            Map<String, AttributeValue> values = new HashMap<>();
            values.put(":nv", AttributeValue.builder().n("30").build());
            sr.filterExpression("#nf > :nv")
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values);
        }
    }

    private Map<String, AttributeValue> createTestItem(int hashIndex, int sortIndex, KeyTypeConfig config) {
        Map<String, AttributeValue> item = new HashMap<>();
        
        AttributeValue hashKeyValue = createAttributeValue(config.hashKeyType, hashIndex, "pk");
        item.put(config.hashKeyName, hashKeyValue);
        
        AttributeValue sortKeyValue = createAttributeValue(config.sortKeyType, sortIndex, "sk");
        item.put(config.sortKeyName, sortKeyValue);
        
        item.put("data_field", AttributeValue.builder().s("data_" + hashIndex + "_" + sortIndex).build());
        item.put("num_field", AttributeValue.builder().n(String.valueOf(hashIndex * 10 + sortIndex)).build());
        
        return item;
    }

    /**
     * Create an AttributeValue based on the specified type and index.
     */
    private AttributeValue createAttributeValue(ScalarAttributeType type, int index, String prefix) {
        switch (type) {
            case S:
                return AttributeValue.builder().s(prefix + index).build();
            case N:
                return AttributeValue.builder().n(String.valueOf(index)).build();
            case B:
                // Create binary data using the index as bytes
                byte[] bytes = (prefix + index).getBytes();
                return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
            default:
                throw new IllegalArgumentException("Unsupported attribute type: " + type);
        }
    }

} 