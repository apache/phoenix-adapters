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
 * Tests concurrent segment scans with different totalSegments values.
 * Verifies that multiple segment scans with different segment counts can run 
 * concurrently and both return correct results.
 */
public class ConcurrentDifferentSegmentCountIT extends BaseSegmentScanIT {

    @Test(timeout = 300000)
    public void test1() throws Exception {
        runConcurrentDifferentSegmentCountTest("pk", ScalarAttributeType.S, "sk", ScalarAttributeType.S);
    }

    @Test(timeout = 300000)
    public void test2() throws Exception {
        runConcurrentDifferentSegmentCountTest("pk", ScalarAttributeType.N, null, null);
    }

    @Test(timeout = 300000)
    public void test3() throws Exception {
        runConcurrentDifferentSegmentCountTest("pk", ScalarAttributeType.B, "sk", ScalarAttributeType.B);
    }

    private void runConcurrentDifferentSegmentCountTest(String hashKeyName, ScalarAttributeType hashKeyType,
                                                       String sortKeyName, ScalarAttributeType sortKeyType) throws Exception {
        
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_");

        // Create table
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(
                tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert items with periodic splitting
        insertItemsWithSplitting(tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType,
                                TOTAL_ITEMS, SPLIT_FREQUENCY, 75);

        // Perform full scan for comparison baseline
        List<Map<String, AttributeValue>> fullScanItemsPhoenix =
                performFullScanWithPagination(phoenixDBClientV2, tableName, false, 0);
        List<Map<String, AttributeValue>> fullScanItemsDDB =
                performFullScanWithPagination(dynamoDbClient, tableName, false, 0);

        // Execute concurrent segment scans with different totalSegments
        List<Map<String, AttributeValue>> segmentScan3Items = performConcurrentSegmentScansWithDifferentCounts(tableName);

        // Verify both segment scans return same results as full scan
        TestUtils.verifyItemsEqual(fullScanItemsDDB, fullScanItemsPhoenix, hashKeyName, sortKeyName);
        TestUtils.verifyItemsEqual(fullScanItemsPhoenix, segmentScan3Items, hashKeyName, sortKeyName);

        LOGGER.info("Concurrent segment scans with different totalSegments completed successfully for table {}", tableName);
    }

    /**
     * Perform two concurrent segment scans with different totalSegments (3 and 5)
     * and verify both return the same complete dataset.
     */
    private List<Map<String, AttributeValue>> performConcurrentSegmentScansWithDifferentCounts(String tableName) 
            throws Exception {
        
        ExecutorService executor = Executors.newFixedThreadPool(2); // 2 concurrent segment scan operations
        CountDownLatch startLatch = new CountDownLatch(1);

        // Submit first segment scan task (totalSegments = 3)
        Future<List<Map<String, AttributeValue>>> segmentScan3Future = executor.submit(() -> {
            try {
                startLatch.await();
                return performAllSegmentScans(tableName, 3);
            } catch (Exception e) {
                throw new RuntimeException("Segment scan with 3 segments failed", e);
            }
        });

        // Submit second segment scan task (totalSegments = 5) 
        Future<List<Map<String, AttributeValue>>> segmentScan5Future = executor.submit(() -> {
            try {
                startLatch.await();
                return performAllSegmentScans(tableName, 5);
            } catch (Exception e) {
                throw new RuntimeException("Segment scan with 5 segments failed", e);
            }
        });

        // Start both tasks concurrently
        startLatch.countDown();

        // Wait for both to complete and collect results
        List<Map<String, AttributeValue>> segmentScan3Items = segmentScan3Future.get(120, TimeUnit.SECONDS);
        List<Map<String, AttributeValue>> segmentScan5Items = segmentScan5Future.get(120, TimeUnit.SECONDS);

        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        // Verify both scans returned the same complete dataset
        TestUtils.verifyItemsEqual(segmentScan3Items, segmentScan5Items, "concurrent_3vs5", "segments");

        LOGGER.info("Segment scan with 3 segments found {} items", segmentScan3Items.size());
        LOGGER.info("Segment scan with 5 segments found {} items", segmentScan5Items.size());

        // Return one of them (they should be identical)
        return segmentScan3Items;
    }

    /**
     * Perform segment scan across all segments for given totalSegments value.
     * Reuses base class scanSingleSegmentWithPagination method.
     */
    private List<Map<String, AttributeValue>> performAllSegmentScans(String tableName, int totalSegments) {
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        
        // Scan each segment sequentially within this thread
        for (int segment = 0; segment < totalSegments; segment++) {
            List<Map<String, AttributeValue>> segmentItems =
                    scanSingleSegmentWithPagination(tableName, segment, totalSegments, SCAN_LIMIT,
                            false, false, 0);
            allItems.addAll(segmentItems);
            LOGGER.info("Segment {}/{} scan found {} items", segment, totalSegments, segmentItems.size());
        }
        
        return allItems;
    }
}
