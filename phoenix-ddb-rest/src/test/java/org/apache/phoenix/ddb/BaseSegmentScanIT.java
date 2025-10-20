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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.ddb.service.utils.SegmentScanUtil;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.ddb.utils.PhoenixUtils;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Base class for segment scan integration tests.
 */
public abstract class BaseSegmentScanIT {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseSegmentScanIT.class);

    protected static final int TOTAL_ITEMS = 1500;
    protected static final int SPLIT_FREQUENCY = 300;
    protected static final int SCAN_LIMIT = 49;

    protected final DynamoDbClient dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    protected static DynamoDbClient phoenixDBClientV2;

    protected static String url;
    protected static HBaseTestingUtility utility = null;
    protected static String tmpDir;
    protected static RESTServer restServer = null;
    protected static Connection testConnection;

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
        DriverManager.registerDriver(new PhoenixTestDriver());

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("Started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        
        testConnection = DriverManager.getConnection(url);
    }

    @After
    public void tearDown() throws Exception {
        // Clear segment metadata after each test
        testConnection.createStatement().execute("DELETE FROM " +
                SegmentScanUtil.SEGMENT_SCAN_METADATA_TABLE_NAME);
    }

    @AfterClass
    public static void stopLocalDynamoDb() throws Exception {
        if (testConnection != null) {
            testConnection.close();
        }
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
     * Create a test item based on the configured key types.
     */
    protected Map<String, AttributeValue> createTestItem(int index, String hashKeyName, ScalarAttributeType hashKeyType,
                                                       String sortKeyName, ScalarAttributeType sortKeyType) {
        Map<String, AttributeValue> item = new HashMap<>();
        
        // Add hash key
        switch (hashKeyType) {
            case S:
                item.put(hashKeyName, AttributeValue.builder().s("hash_" + index).build());
                break;
            case N:
                item.put(hashKeyName, AttributeValue.builder().n(String.valueOf(index)).build());
                break;
            case B:
                item.put(hashKeyName, AttributeValue.builder()
                        .b(SdkBytes.fromUtf8String("hash_" + index)).build());
                break;
            default:
                throw new IllegalArgumentException("Unsupported hash key type: " + hashKeyType);
        }
        
        // Add sort key if present
        if (sortKeyName != null && sortKeyType != null) {
            switch (sortKeyType) {
                case S:
                    item.put(sortKeyName, AttributeValue.builder().s("sort_" + index).build());
                    break;
                case N:
                    item.put(sortKeyName, AttributeValue.builder().n(String.valueOf(index)).build());
                    break;
                case B:
                    item.put(sortKeyName, AttributeValue.builder()
                            .b(SdkBytes.fromUtf8String("sort_" + index)).build());
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported sort key type: " + sortKeyType);
            }
        }
        
        item.put("data", AttributeValue.builder().s("test_data_" + index).build());
        return item;
    }

    /**
     * Split the HBase table at a calculated point.
     */
    protected void splitTable(String tableName, ScalarAttributeType hashKeyType, int currentIndex) {
        try {
            byte[] splitPoint;
            switch (hashKeyType) {
                case S:
                    splitPoint = Bytes.toBytes("hash_" + currentIndex / 2);
                    break;
                case N:
                    splitPoint = PDouble.INSTANCE.toBytes((double)(currentIndex / 2));
                    break;
                case B:
                    splitPoint = Bytes.toBytes("hash_" + currentIndex / 2);
                    break;
                default:
                    splitPoint = Bytes.toBytes(currentIndex / 2);
                    break;
            }
            
            String fullTableName = PhoenixUtils.getFullTableName(tableName, false);
            TestUtils.splitTable(testConnection, fullTableName, splitPoint);
            LOGGER.info("Split table {} at index {}", tableName, currentIndex);
        } catch (Exception e) {
            LOGGER.warn("Failed to split table at index {}: {}", currentIndex, e.getMessage());
        }
    }

    /**
     * Perform a full scan with pagination using the specified limit.
     */
    protected List<Map<String, AttributeValue>> performFullScanWithPagination(DynamoDbClient client,
            String tableName, boolean useFilter) {
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            ScanRequest.Builder scanBuilder = ScanRequest.builder()
                    .tableName(tableName)
                    .limit(SCAN_LIMIT);
            if (useFilter) {
                scanBuilder.filterExpression(generateFilterExpression())
                           .expressionAttributeNames(getFilterExpressionAttributeNames())
                           .expressionAttributeValues(getFilterExpressionAttributeValues());
            }
            if (lastEvaluatedKey != null) {
                scanBuilder.exclusiveStartKey(lastEvaluatedKey);
            }
            ScanResponse response = client.scan(scanBuilder.build());
            allItems.addAll(response.items());
            lastEvaluatedKey = response.lastEvaluatedKey();
        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
        return allItems;
    }

    /**
     * Insert items with periodic table splitting to test segment scan behavior
     * across different HBase regions.
     */
    protected void insertItemsWithSplitting(String tableName, String hashKeyName, ScalarAttributeType hashKeyType,
                                          String sortKeyName, ScalarAttributeType sortKeyType, 
                                          int totalItems, int splitFrequency, int sleepMillis) throws Exception {
        for (int i = 0; i < totalItems; i++) {
            Map<String, AttributeValue> item = createTestItem(i, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
            
            PutItemRequest putRequest = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            
            phoenixDBClientV2.putItem(putRequest);
            dynamoDbClient.putItem(putRequest);
            
            // Periodically split the table to create multiple regions
            if (i > 0 && i % splitFrequency == 0) {
                splitTable(tableName, hashKeyType, i);
                Thread.sleep(sleepMillis);
            }
        }
    }

    /**
     * Scan a single segment with pagination.
     */
    protected List<Map<String, AttributeValue>> scanSingleSegmentWithPagination(String tableName, 
                                                                               int segment, 
                                                                               int totalSegments, 
                                                                               int scanLimit,
                                                                               boolean addDelay,
                                                                               boolean useFilter) {
        List<Map<String, AttributeValue>> segmentItems = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        
        do {
            ScanRequest.Builder scanBuilder = ScanRequest.builder()
                    .tableName(tableName)
                    .segment(segment)
                    .totalSegments(totalSegments)
                    .limit(scanLimit);
            if (useFilter) {
                scanBuilder.filterExpression(generateFilterExpression())
                           .expressionAttributeNames(getFilterExpressionAttributeNames())
                           .expressionAttributeValues(getFilterExpressionAttributeValues());
            }
            if (lastEvaluatedKey != null) {
                scanBuilder.exclusiveStartKey(lastEvaluatedKey);
            }
            ScanResponse response = phoenixDBClientV2.scan(scanBuilder.build());
            segmentItems.addAll(response.items());
            lastEvaluatedKey = response.lastEvaluatedKey();
            
            // Add delay if requested (for concurrent testing)
            if (addDelay) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            
        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
        
        return segmentItems;
    }

    protected String generateFilterExpression() {
        return "begins_with(#data, :prefix)";
    }

    protected Map<String, String> getFilterExpressionAttributeNames() {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#data", "data");
        return expressionAttributeNames;
    }

    protected Map<String, AttributeValue> getFilterExpressionAttributeValues() {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":prefix", AttributeValue.builder().s("test_data_1").build());
        return expressionAttributeValues;
    }
}
