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

import java.util.HashMap;
import java.util.Map;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Utility class for creating large test items to verify 1 MB response size limits.
 */
public class ReturnItemsTestUtil {

    // DynamoDB max item size is 400 KB
    public static final int MAX_ITEM_SIZE_KB = 400;
    public static final int MAX_ITEM_SIZE_BYTES = MAX_ITEM_SIZE_KB * 1024;

    // Create items just under the limit for testing
    public static final int LARGE_ITEM_SIZE_KB = 390;
    public static final int LARGE_ITEM_SIZE_BYTES = LARGE_ITEM_SIZE_KB * 1024;

    // 1 MB response limit
    public static final int ONE_MB_BYTES = 1024 * 1024;

    /**
     * Create a large item of approximately the specified size.
     *
     * @param partitionKey The partition key value
     * @param sortKey      The sort key value
     * @param sizeInKB     The approximate size in KB (will be adjusted for metadata overhead)
     * @return A large DynamoDB item
     */
    public static Map<String, AttributeValue> createLargeItem(String partitionKey, String sortKey,
            int sizeInKB) {
        Map<String, AttributeValue> item = new HashMap<>();

        // Add primary keys
        item.put("pk", AttributeValue.builder().s(partitionKey).build());
        item.put("sk", AttributeValue.builder().s(sortKey).build());

        // Add metadata fields (these add some overhead)
        item.put("timestamp",
                AttributeValue.builder().n(String.valueOf(System.currentTimeMillis())).build());
        item.put("version", AttributeValue.builder().n("1").build());
        item.put("metadata", AttributeValue.builder().s("test_metadata_value").build());
        item.put("type", AttributeValue.builder().s("large_test_item").build());

        // Calculate size for large data field (accounting for other fields overhead)
        int targetSizeBytes = sizeInKB * 1024;
        int estimatedOverheadBytes =
                500; // Approximate overhead for other fields and DynamoDB metadata
        int largeDataSizeBytes = Math.max(100, targetSizeBytes - estimatedOverheadBytes);

        // Add large data field
        item.put("large_data",
                AttributeValue.builder().s(generateLargeString(largeDataSizeBytes)).build());

        return item;
    }

    /**
     * Create a large item with default size (~390 KB).
     */
    public static Map<String, AttributeValue> createLargeItem(String partitionKey, String sortKey) {
        return createLargeItem(partitionKey, sortKey, LARGE_ITEM_SIZE_KB);
    }

    /**
     * Generate a large string of approximately the specified size in bytes.
     */
    public static String generateLargeString(int sizeInBytes) {
        StringBuilder sb = new StringBuilder(sizeInBytes);
        String pattern =
                "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!@#$%^&*()_+-=[]{}|;:,.<>?";

        for (int i = 0; i < sizeInBytes; i++) {
            sb.append(pattern.charAt(i % pattern.length()));
        }

        return sb.toString();
    }

    /**
     * Calculate the expected number of items that should fit in a 1 MB response
     * given the item size.
     */
    public static int calculateExpectedItemsIn1MB(int itemSizeKB) {
        int itemSizeBytes = itemSizeKB * 1024;
        return ONE_MB_BYTES / itemSizeBytes;
    }

    /**
     * Create a test description for logging.
     */
    public static String getTestDescription(int itemCount, int itemSizeKB) {
        int totalSizeMB = (itemCount * itemSizeKB) / 1024;
        int expectedItemsIn1MB = calculateExpectedItemsIn1MB(itemSizeKB);

        return String.format(
                "Test with %d items, each ~%d KB (total ~%d MB). Expected items in 1 MB response: %d",
                itemCount, itemSizeKB, totalSizeMB, expectedItemsIn1MB);
    }
}
