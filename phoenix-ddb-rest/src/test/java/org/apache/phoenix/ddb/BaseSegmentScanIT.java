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
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

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
     * Create a test item based on the configured key types with rich attributes for filter testing.
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
        item.put("status", AttributeValue.builder().s(getStatus(index)).build());
        item.put("priority", AttributeValue.builder().n(String.valueOf(index % 10)).build());
        item.put("active", AttributeValue.builder().bool(index % 2 == 0).build());
        item.put("amount", AttributeValue.builder().n(String.valueOf((index % 1000) + 1)).build());
        item.put("category", AttributeValue.builder().s(getCategory(index)).build());
        item.put("description", AttributeValue.builder().s("description_" + index).build());
        item.put("timestamp", AttributeValue.builder().n(String.valueOf(1000000 + index)).build());
        
        List<String> tags = new ArrayList<>();
        tags.add("tag_" + (index % 5));
        tags.add("common_tag");
        if (index % 3 == 0) {
            tags.add("special_tag");
        }
        item.put("tags", AttributeValue.builder().ss(tags).build());
        
        List<String> scores = new ArrayList<>();
        scores.add(String.valueOf(index % 100));
        scores.add(String.valueOf((index % 50) + 100));
        item.put("scores", AttributeValue.builder().ns(scores).build());
        
        Map<String, AttributeValue> metadata = new HashMap<>();
        metadata.put("version", AttributeValue.builder().s("v" + (index % 3)).build());
        metadata.put("count", AttributeValue.builder().n(String.valueOf(index % 20)).build());
        metadata.put("enabled", AttributeValue.builder().bool(index % 4 == 0).build());
        item.put("metadata", AttributeValue.builder().m(metadata).build());
        
        List<AttributeValue> items = new ArrayList<>();
        items.add(AttributeValue.builder().s("item_" + (index % 7)).build());
        items.add(AttributeValue.builder().n(String.valueOf(index % 30)).build());
        item.put("items", AttributeValue.builder().l(items).build());
        
        if (index % 5 == 0) {
            item.put("optional_field", AttributeValue.builder().s("optional_" + index).build());
        }
        
        return item;
    }
    
    private String getStatus(int index) {
        String[] statuses = {"pending", "active", "completed", "failed", "cancelled"};
        return statuses[index % statuses.length];
    }
    
    private String getCategory(int index) {
        String[] categories = {"electronics", "books", "clothing", "food", "toys", "sports"};
        return categories[index % categories.length];
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
            String tableName, boolean useFilter, int filterNum) {
        return performFullScanWithPagination(client, tableName, useFilter, filterNum, SCAN_LIMIT);
    }

    /**
     * Perform a full scan with pagination using a custom scan limit.
     */
    protected List<Map<String, AttributeValue>> performFullScanWithPagination(DynamoDbClient client,
            String tableName, boolean useFilter, int filterNum, int scanLimit) {
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        do {
            ScanRequest.Builder scanBuilder = ScanRequest.builder()
                    .tableName(tableName)
                    .limit(scanLimit);
            if (useFilter) {
                scanBuilder.filterExpression(getFilterExpression(filterNum));
                Map<String, String> attrNames = getFilterAttributeNames(filterNum);
                if (!attrNames.isEmpty()) {
                    scanBuilder.expressionAttributeNames(attrNames);
                }
                Map<String, AttributeValue> attrValues = getFilterAttributeValues(filterNum);
                if (!attrValues.isEmpty()) {
                    scanBuilder.expressionAttributeValues(attrValues);
                }
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
    protected void insertItemsWithSplitting(String tableName, String hashKeyName,
            ScalarAttributeType hashKeyType, String sortKeyName, ScalarAttributeType sortKeyType,
            int totalItems, int splitFrequency, int sleepMillis) throws Exception {
        List<WriteRequest> batch = new ArrayList<>();
        for (int i = 0; i < totalItems; i++) {
            Map<String, AttributeValue> item =
                    createTestItem(i, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
            WriteRequest writeRequest =
                    WriteRequest.builder().putRequest(PutRequest.builder().item(item).build())
                            .build();
            batch.add(writeRequest);
            boolean shouldFlush = batch.size() >= 25 || (i > 0 && (i + 1) % splitFrequency == 0)
                    || i == totalItems - 1;
            if (shouldFlush) {
                executeBatchWrite(tableName, batch);
                batch.clear();
            }
            // Periodically split the table to create multiple regions
            if (i > 0 && i % splitFrequency == 0) {
                splitTable(tableName, hashKeyType, i);
                Thread.sleep(sleepMillis);
            }
        }
    }

    /**
     * Execute batch write for both Phoenix and DynamoDB clients.
     */
    private void executeBatchWrite(String tableName, List<WriteRequest> batch) {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName, new ArrayList<>(batch));
        BatchWriteItemRequest batchRequest =
                BatchWriteItemRequest.builder().requestItems(requestItems).build();

        phoenixDBClientV2.batchWriteItem(batchRequest);
        dynamoDbClient.batchWriteItem(batchRequest);
    }

    /**
     * Scan a single segment with pagination.
     */
    protected List<Map<String, AttributeValue>> scanSingleSegmentWithPagination(String tableName, 
                                                                               int segment, 
                                                                               int totalSegments, 
                                                                               int scanLimit,
                                                                               boolean addDelay,
                                                                               boolean useFilter,
                                                                               int filterNum) {
        List<Map<String, AttributeValue>> segmentItems = new ArrayList<>();
        Map<String, AttributeValue> lastEvaluatedKey = null;
        
        do {
            ScanRequest.Builder scanBuilder = ScanRequest.builder()
                    .tableName(tableName)
                    .segment(segment)
                    .totalSegments(totalSegments)
                    .limit(scanLimit);
            if (useFilter) {
                scanBuilder.filterExpression(getFilterExpression(filterNum));
                Map<String, String> attrNames = getFilterAttributeNames(filterNum);
                if (!attrNames.isEmpty()) {
                    scanBuilder.expressionAttributeNames(attrNames);
                }
                Map<String, AttributeValue> attrValues = getFilterAttributeValues(filterNum);
                if (!attrValues.isEmpty()) {
                    scanBuilder.expressionAttributeValues(attrValues);
                }
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

    protected String getFilterExpression(int filterNum) {
        switch (filterNum) {
            case 1:
                return getFilterExpression1();
            case 2:
                return getFilterExpression2();
            case 3:
                return getFilterExpression3();
            case 4:
                return getFilterExpression4();
            case 5:
                return getFilterExpression5();
            case 6:
                return getFilterExpression6();
            case 7:
                return getFilterExpression7();
            case 8:
                return getFilterExpression8();
            case 9:
                return getFilterExpression9();
            case 10:
                return getFilterExpression10();
            case 11:
                return getFilterExpression11();
            case 12:
                return getFilterExpression12();
            case 13:
                return getFilterExpression13();
            case 14:
                return getFilterExpression14();
            case 15:
                return getFilterExpression15();
            default:
                return getFilterExpression1();
        }
    }

    protected Map<String, String> getFilterAttributeNames(int filterNum) {
        switch (filterNum) {
            case 1:
                return getFilterAttributeNames1();
            case 2:
                return getFilterAttributeNames2();
            case 3:
                return getFilterAttributeNames3();
            case 4:
                return getFilterAttributeNames4();
            case 5:
                return getFilterAttributeNames5();
            case 6:
                return getFilterAttributeNames6();
            case 7:
                return getFilterAttributeNames7();
            case 8:
                return getFilterAttributeNames8();
            case 9:
                return getFilterAttributeNames9();
            case 10:
                return getFilterAttributeNames10();
            case 11:
                return getFilterAttributeNames11();
            case 12:
                return getFilterAttributeNames12();
            case 13:
                return getFilterAttributeNames13();
            case 14:
                return getFilterAttributeNames14();
            case 15:
                return getFilterAttributeNames15();
            default:
                return getFilterAttributeNames1();
        }
    }

    protected Map<String, AttributeValue> getFilterAttributeValues(int filterNum) {
        switch (filterNum) {
            case 1:
                return getFilterAttributeValues1();
            case 2:
                return getFilterAttributeValues2();
            case 3:
                return getFilterAttributeValues3();
            case 4:
                return getFilterAttributeValues4();
            case 5:
                return getFilterAttributeValues5();
            case 6:
                return getFilterAttributeValues6();
            case 7:
                return getFilterAttributeValues7();
            case 8:
                return getFilterAttributeValues8();
            case 9:
                return getFilterAttributeValues9();
            case 10:
                return getFilterAttributeValues10();
            case 11:
                return getFilterAttributeValues11();
            case 12:
                return getFilterAttributeValues12();
            case 13:
                return getFilterAttributeValues13();
            case 14:
                return getFilterAttributeValues14();
            case 15:
                return getFilterAttributeValues15();
            default:
                return getFilterAttributeValues1();
        }
    }

    protected String generateFilterExpression() {
        return getFilterExpression1();
    }

    protected Map<String, String> getFilterExpressionAttributeNames() {
        return getFilterAttributeNames1();
    }

    protected Map<String, AttributeValue> getFilterExpressionAttributeValues() {
        return getFilterAttributeValues1();
    }

    protected String getFilterExpression1() {
        return "begins_with(#data, :prefix)";
    }

    protected Map<String, String> getFilterAttributeNames1() {
        Map<String, String> expressionAttributeNames = new HashMap<>();
        expressionAttributeNames.put("#data", "data");
        return expressionAttributeNames;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues1() {
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":prefix", AttributeValue.builder().s("test_data_1").build());
        return expressionAttributeValues;
    }

    protected String getFilterExpression2() {
        return "#amount BETWEEN :minAmount AND :maxAmount";
    }

    protected Map<String, String> getFilterAttributeNames2() {
        Map<String, String> names = new HashMap<>();
        names.put("#amount", "amount");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues2() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":minAmount", AttributeValue.builder().n("200").build());
        values.put(":maxAmount", AttributeValue.builder().n("800").build());
        return values;
    }

    protected String getFilterExpression3() {
        return "#active = :isActive AND #priority > :minPriority";
    }

    protected Map<String, String> getFilterAttributeNames3() {
        Map<String, String> names = new HashMap<>();
        names.put("#active", "active");
        names.put("#priority", "priority");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues3() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":isActive", AttributeValue.builder().bool(true).build());
        values.put(":minPriority", AttributeValue.builder().n("5").build());
        return values;
    }

    protected String getFilterExpression4() {
        return "#status IN (:status1, :status2, :status3)";
    }

    protected Map<String, String> getFilterAttributeNames4() {
        Map<String, String> names = new HashMap<>();
        names.put("#status", "status");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues4() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":status1", AttributeValue.builder().s("active").build());
        values.put(":status2", AttributeValue.builder().s("pending").build());
        values.put(":status3", AttributeValue.builder().s("completed").build());
        return values;
    }

    protected String getFilterExpression5() {
        return "contains(#tags, :tag)";
    }

    protected Map<String, String> getFilterAttributeNames5() {
        Map<String, String> names = new HashMap<>();
        names.put("#tags", "tags");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues5() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":tag", AttributeValue.builder().s("special_tag").build());
        return values;
    }

    protected String getFilterExpression6() {
        return "#metadata.#version = :version AND #metadata.#enabled = :enabled";
    }

    protected Map<String, String> getFilterAttributeNames6() {
        Map<String, String> names = new HashMap<>();
        names.put("#metadata", "metadata");
        names.put("#version", "version");
        names.put("#enabled", "enabled");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues6() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":version", AttributeValue.builder().s("v1").build());
        values.put(":enabled", AttributeValue.builder().bool(true).build());
        return values;
    }

    protected String getFilterExpression7() {
        return "attribute_exists(optional_field)";
    }

    protected Map<String, String> getFilterAttributeNames7() {
        return new HashMap<>();
    }

    protected Map<String, AttributeValue> getFilterAttributeValues7() {
        return new HashMap<>();
    }

    protected String getFilterExpression8() {
        return "attribute_not_exists(optional_field)";
    }

    protected Map<String, String> getFilterAttributeNames8() {
        return new HashMap<>();
    }

    protected Map<String, AttributeValue> getFilterAttributeValues8() {
        return new HashMap<>();
    }

    protected String getFilterExpression9() {
        return "(#category = :category AND #amount > :minAmount) OR "
                + "(#status = :status AND #active = :active)";
    }

    protected Map<String, String> getFilterAttributeNames9() {
        Map<String, String> names = new HashMap<>();
        names.put("#category", "category");
        names.put("#amount", "amount");
        names.put("#status", "status");
        names.put("#active", "active");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues9() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":category", AttributeValue.builder().s("electronics").build());
        values.put(":minAmount", AttributeValue.builder().n("500").build());
        values.put(":status", AttributeValue.builder().s("active").build());
        values.put(":active", AttributeValue.builder().bool(true).build());
        return values;
    }

    protected String getFilterExpression10() {
        return "NOT (#status = :status)";
    }

    protected Map<String, String> getFilterAttributeNames10() {
        Map<String, String> names = new HashMap<>();
        names.put("#status", "status");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues10() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":status", AttributeValue.builder().s("failed").build());
        return values;
    }

    protected String getFilterExpression11() {
        return "size(#items) = :size";
    }

    protected Map<String, String> getFilterAttributeNames11() {
        Map<String, String> names = new HashMap<>();
        names.put("#items", "items");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues11() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":size", AttributeValue.builder().n("2").build());
        return values;
    }

    protected String getFilterExpression12() {
        return "contains(#description, :substring)";
    }

    protected Map<String, String> getFilterAttributeNames12() {
        Map<String, String> names = new HashMap<>();
        names.put("#description", "description");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues12() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":substring", AttributeValue.builder().s("description_1").build());
        return values;
    }

    protected String getFilterExpression13() {
        return "contains(#scores, :score)";
    }

    protected Map<String, String> getFilterAttributeNames13() {
        Map<String, String> names = new HashMap<>();
        names.put("#scores", "scores");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues13() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":score", AttributeValue.builder().n("110").build());
        return values;
    }

    protected String getFilterExpression14() {
        return "#priority >= :minPri AND #priority <= :maxPri AND #category <> :excludeCategory";
    }

    protected Map<String, String> getFilterAttributeNames14() {
        Map<String, String> names = new HashMap<>();
        names.put("#priority", "priority");
        names.put("#category", "category");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues14() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":minPri", AttributeValue.builder().n("3").build());
        values.put(":maxPri", AttributeValue.builder().n("7").build());
        values.put(":excludeCategory", AttributeValue.builder().s("food").build());
        return values;
    }

    protected String getFilterExpression15() {
        return "#timestamp > :startTime AND #timestamp < :endTime";
    }

    protected Map<String, String> getFilterAttributeNames15() {
        Map<String, String> names = new HashMap<>();
        names.put("#timestamp", "timestamp");
        return names;
    }

    protected Map<String, AttributeValue> getFilterAttributeValues15() {
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":startTime", AttributeValue.builder().n("1010000").build());
        values.put(":endTime", AttributeValue.builder().n("1015000").build());
        return values;
    }
}
