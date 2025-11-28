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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Tests concurrent segment scans while table split is happening.
 * Verifies that segment scanning remains consistent under concurrent operations.
 */
public class ConcurrentSegmentScanIT extends BaseSegmentScanIT {

    private static final int TOTAL_SEGMENTS = 4;

    @Test(timeout = 300000)
    public void testConcurrentSegmentScansWithStringSortKey() throws Exception {
        runConcurrentSegmentScanTest("pk", ScalarAttributeType.S, "sk", ScalarAttributeType.S);
    }

    @Test(timeout = 300000)
    public void testConcurrentSegmentScansHashKeyOnly() throws Exception {
        runConcurrentSegmentScanTest("pk", ScalarAttributeType.N, null, null);
    }

    @Test(timeout = 300000)  
    public void testConcurrentSegmentScansWithBinaryKeys() throws Exception {
        runConcurrentSegmentScanTest("pk", ScalarAttributeType.B, "sk", ScalarAttributeType.B);
    }

    private void runConcurrentSegmentScanTest(String hashKeyName, ScalarAttributeType hashKeyType,
                                            String sortKeyName, ScalarAttributeType sortKeyType) throws Exception {
        
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_");

        // Create table
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(
                tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert items with periodic splitting
        insertItemsWithSplitting(tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType,
                                TOTAL_ITEMS, SPLIT_FREQUENCY, 50);

        // Perform full scan for comparison
        List<Map<String, AttributeValue>> fullScanItemsPhoenix =
                performFullScanWithPagination(phoenixDBClientV2, tableName, false, 0);
        List<Map<String, AttributeValue>> fullScanItemsDDB =
                performFullScanWithPagination(dynamoDbClient, tableName, false, 0);

        // Execute concurrent segment scans with concurrent splits
        List<Map<String, AttributeValue>> concurrentSegmentItems = 
            performConcurrentSegmentScansWithSplits(tableName);

        // Verify results
        TestUtils.verifyItemsEqual(fullScanItemsDDB, fullScanItemsPhoenix, hashKeyName, sortKeyName);
        TestUtils.verifyItemsEqual(fullScanItemsPhoenix, concurrentSegmentItems, hashKeyName, sortKeyName);
    }





    private List<Map<String, AttributeValue>> performConcurrentSegmentScansWithSplits(String tableName) 
            throws Exception {
        
        ExecutorService executor = Executors.newFixedThreadPool(TOTAL_SEGMENTS + 1); // segments + 1 split thread
        CountDownLatch startLatch = new CountDownLatch(1);
        List<Future<List<Map<String, AttributeValue>>>> segmentFutures = new ArrayList<>();

        // Submit segment scan tasks
        for (int segment = 0; segment < TOTAL_SEGMENTS; segment++) {
            final int segmentNum = segment;
            segmentFutures.add(executor.submit(() -> {
                try {
                    startLatch.await();
                    Thread.sleep(1000); // Let split start
                    return scanSegmentWithPagination(tableName, segmentNum);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));
        }

        // Submit concurrent split task
        Future<Void> splitTask1 = executor.submit(() -> {
            try {
                startLatch.await();
                splitTable(tableName, ScalarAttributeType.S, TOTAL_ITEMS / 3);
            } catch (Exception e) {
                LOGGER.warn("Concurrent split 1 failed: {}", e.getMessage());
            }
            return null;
        });

        // Start all tasks
        startLatch.countDown();

        // Collect segment results
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        for (Future<List<Map<String, AttributeValue>>> future : segmentFutures) {
            List<Map<String, AttributeValue>> segmentItems = future.get(60, TimeUnit.SECONDS);
            allItems.addAll(segmentItems);
        }

        // Wait for splits to complete
        splitTask1.get(10, TimeUnit.SECONDS);

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        return allItems;
    }

    private List<Map<String, AttributeValue>> scanSegmentWithPagination(String tableName, int segment) {
        List<Map<String, AttributeValue>> segmentItems =
                scanSingleSegmentWithPagination(tableName, segment, TOTAL_SEGMENTS, SCAN_LIMIT,
                        true, false, 0);
        LOGGER.info("Segment {} scan completed with {} items", segment, segmentItems.size());
        return segmentItems;
    }

}