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
import org.junit.Test;
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
 * Tests scan pagination on indexes with different limits and key type combinations.
 * Items are inserted with deliberate ties on index keys to verify RVC cursor correctness
 * when multiple rows share the same (ihk, isk) pair.
 */
@RunWith(Parameterized.class)
public class ScanIndexExclusiveStartKeyIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanIndexExclusiveStartKeyIT.class);

    private static final String INDEX_NAME = "test_gsi";
    private static final String TABLE_HK = "partition_key";
    private static final String TABLE_SK = "sort_key";
    private static final String INDEX_HK = "idx_hk";
    private static final String INDEX_SK = "idx_sk";

    private static final int NUM_HASH_KEYS = 7;
    private static final int NUM_SORT_KEYS = 8;
    private static final int TOTAL_ITEMS = NUM_HASH_KEYS * NUM_SORT_KEYS;

    private static final KeyTypeConfig[] KEY_CONFIGS = {
        new KeyTypeConfig("tSN_iSN", ScalarAttributeType.S, ScalarAttributeType.N,
                ScalarAttributeType.S, ScalarAttributeType.N),
        new KeyTypeConfig("tSS_iNS", ScalarAttributeType.S, ScalarAttributeType.S,
                ScalarAttributeType.N, ScalarAttributeType.S),
        new KeyTypeConfig("tNN_iSS", ScalarAttributeType.N, ScalarAttributeType.N,
                ScalarAttributeType.S, ScalarAttributeType.S),
        new KeyTypeConfig("tSN_iNN", ScalarAttributeType.S, ScalarAttributeType.N,
                ScalarAttributeType.N, ScalarAttributeType.N),
        new KeyTypeConfig("tBN_iSN", ScalarAttributeType.B, ScalarAttributeType.N,
                ScalarAttributeType.S, ScalarAttributeType.N),
        new KeyTypeConfig("tNS_iSB", ScalarAttributeType.N, ScalarAttributeType.S,
                ScalarAttributeType.S, ScalarAttributeType.B),
    };

    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbClient phoenixDBClientV2;

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    public int scanLimit;
    public KeyTypeConfig keyTypeConfig;
    public boolean withFilter;

    public static class KeyTypeConfig {
        public final String name;
        public final ScalarAttributeType tableHKType;
        public final ScalarAttributeType tableSKType;
        public final ScalarAttributeType indexHKType;
        public final ScalarAttributeType indexSKType;

        public KeyTypeConfig(String name, ScalarAttributeType tableHKType,
                ScalarAttributeType tableSKType, ScalarAttributeType indexHKType,
                ScalarAttributeType indexSKType) {
            this.name = name;
            this.tableHKType = tableHKType;
            this.tableSKType = tableSKType;
            this.indexHKType = indexHKType;
            this.indexSKType = indexSKType;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Parameterized.Parameters(name = "limit_{0}_keyTypes_{1}_filter_{2}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        int[] scanLimits = {1, 2, 3, 5, 7, 10, 15};
        for (int limit : scanLimits) {
            for (KeyTypeConfig config : KEY_CONFIGS) {
                for (boolean filter : new boolean[]{false, true}) {
                    parameters.add(new Object[]{limit, config, filter});
                }
            }
        }
        return parameters;
    }

    public ScanIndexExclusiveStartKeyIT(int scanLimit, KeyTypeConfig keyTypeConfig,
            boolean withFilter) {
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
        String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR
                + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());
        TestUtils.awaitPhoenixReady(url);

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();

        createAllTablesAndInsertData();

        TestUtils.waitForEventualConsistentIndex();
        TestUtils.waitForEventualConsistentIndex();
    }

    private static void createAllTablesAndInsertData() {
        for (KeyTypeConfig config : KEY_CONFIGS) {
            String tableName = "SIESK_" + config.name;

            CreateTableRequest createTableRequest =
                    DDLTestUtils.getCreateTableRequest(tableName, TABLE_HK,
                            config.tableHKType, TABLE_SK, config.tableSKType);
            createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest,
                    INDEX_NAME, INDEX_HK, config.indexHKType,
                    INDEX_SK, config.indexSKType);

            phoenixDBClientV2.createTable(createTableRequest);
            dynamoDbClient.createTable(createTableRequest);

            for (int hashIndex = 0; hashIndex < NUM_HASH_KEYS; hashIndex++) {
                for (int sortIndex = 0; sortIndex < NUM_SORT_KEYS; sortIndex++) {
                    Map<String, AttributeValue> item =
                            createTestItem(config, hashIndex, sortIndex);
                    PutItemRequest putItemRequest = PutItemRequest.builder()
                            .tableName(tableName)
                            .item(item)
                            .build();
                    phoenixDBClientV2.putItem(putItemRequest);
                    dynamoDbClient.putItem(putItemRequest);
                }
            }
            LOGGER.info("Created table {} and inserted {} items", tableName, TOTAL_ITEMS);
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

    @Test(timeout = 150000)
    public void testScanIndexExclusiveStartKeyPagination() {
        final String tableName = "SIESK_" + keyTypeConfig.name;

        List<Map<String, AttributeValue>> phoenixItems = new ArrayList<>();
        List<Map<String, AttributeValue>> dynamoItems = new ArrayList<>();

        Map<String, AttributeValue> phoenixLastKey = null;
        Map<String, AttributeValue> dynamoLastKey = null;

        int phoenixPageCount = 0;
        int dynamoPageCount = 0;

        do {
            ScanRequest.Builder sr = ScanRequest.builder()
                    .tableName(tableName)
                    .indexName(INDEX_NAME)
                    .limit(scanLimit);
            applyFilter(sr);
            if (dynamoLastKey != null && !dynamoLastKey.isEmpty()) {
                sr.exclusiveStartKey(dynamoLastKey);
            }
            ScanResponse response = dynamoDbClient.scan(sr.build());
            dynamoItems.addAll(response.items());
            dynamoLastKey = response.lastEvaluatedKey();
            dynamoPageCount++;
            LOGGER.info("DynamoDB index scan page {}, returned {} items, lastKey: {}",
                    dynamoPageCount, response.count(), dynamoLastKey);
        } while (dynamoLastKey != null && !dynamoLastKey.isEmpty());

        do {
            ScanRequest.Builder sr = ScanRequest.builder()
                    .tableName(tableName)
                    .indexName(INDEX_NAME)
                    .limit(scanLimit);
            applyFilter(sr);
            if (phoenixLastKey != null && !phoenixLastKey.isEmpty()) {
                sr.exclusiveStartKey(phoenixLastKey);
            }
            ScanResponse response = phoenixDBClientV2.scan(sr.build());
            phoenixItems.addAll(response.items());
            phoenixLastKey = response.lastEvaluatedKey();
            phoenixPageCount++;
            LOGGER.info("Phoenix index scan page {}, returned {} items, lastKey: {}",
                    phoenixPageCount, response.count(), phoenixLastKey);
        } while (phoenixLastKey != null && !phoenixLastKey.isEmpty());

        Assert.assertEquals("Phoenix and DynamoDB should return same number of items",
                dynamoItems.size(), phoenixItems.size());
        Assert.assertFalse("Should return some items", phoenixItems.isEmpty());
        if (withFilter) {
            Assert.assertTrue("Filter should reduce the result set",
                    phoenixItems.size() < TOTAL_ITEMS);
        } else {
            Assert.assertEquals("Should return all items without filter",
                    TOTAL_ITEMS, phoenixItems.size());
        }

        Assert.assertTrue("Phoenix pagination should complete", phoenixPageCount > 0);
        Assert.assertTrue("DynamoDB pagination should complete", dynamoPageCount > 0);

        if (scanLimit < TOTAL_ITEMS) {
            Assert.assertTrue("Should require multiple pagination rounds for limit " + scanLimit,
                    phoenixPageCount > 1);
        }

        List<Map<String, AttributeValue>> sortedPhoenixItems =
                TestUtils.sortItemsByPartitionAndSortKey(phoenixItems, TABLE_HK, TABLE_SK);
        List<Map<String, AttributeValue>> sortedDynamoItems =
                TestUtils.sortItemsByPartitionAndSortKey(dynamoItems, TABLE_HK, TABLE_SK);
        Assert.assertTrue("Phoenix and DynamoDB should return identical items when sorted",
                ItemComparator.areItemsEqual(sortedPhoenixItems, sortedDynamoItems));
    }

    private void applyFilter(ScanRequest.Builder sr) {
        if (withFilter) {
            Map<String, String> names = new HashMap<>();
            names.put("#sc", "score");
            Map<String, AttributeValue> values = new HashMap<>();
            values.put(":sv", AttributeValue.builder().n("300").build());
            sr.filterExpression("#sc > :sv")
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values);
        }
    }

    private static Map<String, AttributeValue> createTestItem(KeyTypeConfig config,
            int hashIndex, int sortIndex) {
        Map<String, AttributeValue> item = new HashMap<>();

        item.put(TABLE_HK, createAttributeValue(config.tableHKType, hashIndex, "pk"));
        item.put(TABLE_SK, createAttributeValue(config.tableSKType, sortIndex, "sk"));

        int ihkVal = hashIndex % 3;
        int iskVal = sortIndex % 4;
        item.put(INDEX_HK, createAttributeValue(config.indexHKType, ihkVal, "ihk"));
        item.put(INDEX_SK, createAttributeValue(config.indexSKType, iskVal, "isk"));

        item.put("extra_data", AttributeValue.builder()
                .s("data_" + hashIndex + "_" + sortIndex).build());
        item.put("score", AttributeValue.builder()
                .n(String.valueOf(hashIndex * 100 + sortIndex)).build());

        return item;
    }

    private static AttributeValue createAttributeValue(ScalarAttributeType type, int index,
            String prefix) {
        switch (type) {
            case S:
                return AttributeValue.builder().s(prefix + index).build();
            case N:
                return AttributeValue.builder().n(String.valueOf(index)).build();
            case B:
                byte[] bytes = (prefix + index).getBytes();
                return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
            default:
                throw new IllegalArgumentException("Unsupported attribute type: " + type);
        }
    }
}
