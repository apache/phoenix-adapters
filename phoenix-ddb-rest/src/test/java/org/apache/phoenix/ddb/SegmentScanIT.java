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

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Abstract base class for segment scan tests.
 */
public abstract class SegmentScanIT extends BaseSegmentScanIT {

    protected final String hashKeyName;
    protected final ScalarAttributeType hashKeyType;
    protected final String sortKeyName;
    protected final ScalarAttributeType sortKeyType;
    protected final int totalSegments;
    protected final boolean useFilter;
    protected final int filterNum;
    protected static final int TOTAL_ITEMS = 25000;
    protected static final int SPLIT_FREQUENCY = 4000;

    public SegmentScanIT(String hashKeyName, ScalarAttributeType hashKeyType, 
                         String sortKeyName, ScalarAttributeType sortKeyType, 
                         boolean useFilter) {
        this.hashKeyName = hashKeyName;
        this.hashKeyType = hashKeyType;
        this.sortKeyName = sortKeyName;
        this.sortKeyType = sortKeyType;
        Random random = new Random();
        this.totalSegments = random.nextInt(8) + 1;
        this.useFilter = useFilter;
        this.filterNum = useFilter ? random.nextInt(15) + 1 : 0;
    }

    @Test(timeout = 600000)
    public void testSegmentScan() throws Exception {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_")
                + "_" + generateRandomString(7);

        // Create table
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(
                tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert items with periodic table splitting
        insertItemsWithSplitting(tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType,
                TOTAL_ITEMS, SPLIT_FREQUENCY, 200);

        Random random = new Random();

        // Perform full scan with pagination
        List<Map<String, AttributeValue>> fullScanItemsPhoenix =
                performFullScanWithPagination(phoenixDBClientV2, tableName, useFilter, filterNum,
                        random.nextInt(150) + 1);
        List<Map<String, AttributeValue>> fullScanItemsDDB =
                performFullScanWithPagination(dynamoDbClient, tableName, useFilter, filterNum,
                        random.nextInt(200) + 1);

        // Perform segment scan serially on all segments with pagination
        List<Map<String, AttributeValue>> segmentScanItems =
                performSegmentScanWithPagination(tableName, useFilter, filterNum);

        // Verify results
        TestUtils.verifyItemsEqual(fullScanItemsDDB, fullScanItemsPhoenix, hashKeyName,
                sortKeyName);
        TestUtils.verifyItemsEqual(fullScanItemsPhoenix, segmentScanItems, hashKeyName,
                sortKeyName);
    }

    /**
     * Perform segment scan across all segments with pagination.
     */
    private List<Map<String, AttributeValue>> performSegmentScanWithPagination(String tableName,
            boolean useFilter, int filterNum) throws SQLException {
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        Random random = new Random();
        
        // Scan each segment sequentially
        int numRegions = TestUtils.getNumberOfTableRegions(testConnection, tableName);
        for (int segment = 0; segment < totalSegments; segment++) {
            int scanLimit = random.nextInt(200) + 1;
            List<Map<String, AttributeValue>> segmentItems =
                    scanSingleSegmentWithPagination(tableName, segment, totalSegments, scanLimit,
                            false, useFilter, filterNum);
            allItems.addAll(segmentItems);
            if (segment < numRegions && !useFilter) {
                Assert.assertFalse(segmentItems.isEmpty());
            }
        }
        return allItems;
    }

    private String generateRandomString(int length) {
        String chars = "abcdefghijklmnopqrstuvwxyz0123456789-_.";
        Random random = new Random();
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }
}
