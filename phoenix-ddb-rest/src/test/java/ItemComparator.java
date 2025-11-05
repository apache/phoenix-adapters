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

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

/**
 * Utility class for comparing items.
 */
public class ItemComparator {

    /**
     * Compares two items for equality, including all nested elements.
     *
     * @param item1 First item to compare
     * @param item2 Second item to compare
     * @return true if items are equal, false otherwise
     */
    public static boolean areItemsEqual(Map<String, AttributeValue> item1,
            Map<String, AttributeValue> item2) {
        if (item1 == item2) {
            return true;
        }
        if (item1 == null || item2 == null) {
            return false;
        }
        if (item1.size() != item2.size()) {
            return false;
        }

        for (Map.Entry<String, AttributeValue> entry : item1.entrySet()) {
            String key = entry.getKey();
            AttributeValue value1 = entry.getValue();
            AttributeValue value2 = item2.get(key);

            if (!areAttributeValuesEqual(value1, value2)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two lists of items for equality.
     *
     * @param list1 First list of items to compare
     * @param list2 Second list of items to compare
     * @return true if lists are equal, false otherwise
     */
    public static boolean areItemsEqual(List<Map<String, AttributeValue>> list1,
            List<Map<String, AttributeValue>> list2) {
        if (list1 == list2) {
            return true;
        }
        if (list1 == null || list2 == null) {
            return false;
        }
        if (list1.size() != list2.size()) {
            return false;
        }
        return IntStream.range(0, list1.size())
                .allMatch(i -> areItemsEqual(list1.get(i), list2.get(i)));
    }

    /**
     * Compares two AttributeValues for equality.
     */
    private static boolean areAttributeValuesEqual(AttributeValue value1, AttributeValue value2) {
        if (value1 == value2) {
            return true;
        }
        if (value1 == null || value2 == null) {
            return false;
        }

        // Compare based on the type of value
        if (value1.s() != null) {
            return value1.s().equals(value2.s());
        }
        if (value1.n() != null) {
            return value1.n().equals(value2.n());
        }
        if (value1.b() != null) {
            byte[] bytes1 = value1.b().asByteArray();
            byte[] bytes2 = value2.b().asByteArray();
            if (bytes1.length != bytes2.length) {
                return false;
            }
            for (int i = 0; i < bytes1.length; i++) {
                if (bytes1[i] != bytes2[i]) {
                    return false;
                }
            }
            return true;
        }
        if (value1.ss() != null) {
            return areStringSetsEqual(value1.ss(), value2.ss());
        }
        if (value1.ns() != null) {
            return areStringSetsEqual(value1.ns(), value2.ns());
        }
        if (value1.bs() != null) {
            return areBinarySetsEqual(value1.bs(), value2.bs());
        }
        if (value1.l() != null) {
            return areListsEqual(value1.l(), value2.l());
        }
        if (value1.m() != null) {
            return areItemsEqual(value1.m(), value2.m());
        }
        if (value1.bool() != null) {
            return value1.bool().equals(value2.bool());
        }
        if (value1.nul() != null) {
            return value1.nul().equals(value2.nul());
        }
        return false;
    }

    /**
     * Compares two lists of AttributeValues for equality.
     */
    private static boolean areListsEqual(List<AttributeValue> list1, List<AttributeValue> list2) {
        if (list1 == list2) {
            return true;
        }
        if (list1 == null || list2 == null) {
            return false;
        }
        if (list1.size() != list2.size()) {
            return false;
        }

        for (int i = 0; i < list1.size(); i++) {
            if (!areAttributeValuesEqual(list1.get(i), list2.get(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * Compares two sets of strings for equality.
     */
    private static boolean areStringSetsEqual(List<String> set1, List<String> set2) {
        if (set1 == set2) {
            return true;
        }
        if (set1 == null || set2 == null) {
            return false;
        }
        if (set1.size() != set2.size()) {
            return false;
        }

        List<String> list1 = set1.stream().sorted().collect(Collectors.toList());
        List<String> list2 = set2.stream().sorted().collect(Collectors.toList());
        return list1.equals(list2);
    }

    /**
     * Compares two sets of binary values for equality.
     */
    private static boolean areBinarySetsEqual(List<software.amazon.awssdk.core.SdkBytes> set1,
            List<software.amazon.awssdk.core.SdkBytes> set2) {
        if (set1 == set2) {
            return true;
        }
        if (set1 == null || set2 == null) {
            return false;
        }
        if (set1.size() != set2.size()) {
            return false;
        }

        // Convert to sorted lists of byte arrays
        List<byte[]> list1 = set1.stream().map(software.amazon.awssdk.core.SdkBytes::asByteArray)
                .sorted((a, b) -> {
                    int minLength = Math.min(a.length, b.length);
                    for (int i = 0; i < minLength; i++) {
                        int cmp = Byte.compare(a[i], b[i]);
                        if (cmp != 0) {
                            return cmp;
                        }
                    }
                    return Integer.compare(a.length, b.length);
                }).collect(Collectors.toList());

        List<byte[]> list2 = set2.stream().map(software.amazon.awssdk.core.SdkBytes::asByteArray)
                .sorted((a, b) -> {
                    int minLength = Math.min(a.length, b.length);
                    for (int i = 0; i < minLength; i++) {
                        int cmp = Byte.compare(a[i], b[i]);
                        if (cmp != 0) {
                            return cmp;
                        }
                    }
                    return Integer.compare(a.length, b.length);
                }).collect(Collectors.toList());

        if (list1.size() != list2.size()) {
            return false;
        }

        for (int i = 0; i < list1.size(); i++) {
            byte[] arr1 = list1.get(i);
            byte[] arr2 = list2.get(i);
            if (arr1.length != arr2.length) {
                return false;
            }
            for (int j = 0; j < arr1.length; j++) {
                if (arr1[j] != arr2[j]) {
                    return false;
                }
            }
        }
        return true;
    }
}
