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

import org.apache.phoenix.ddb.utils.KeyConditionsHolder;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PName;
import org.apache.phoenix.schema.PNameFactory;
import org.junit.Test;
import org.junit.Assert;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class KeyConditionsHolderTest {

    private Map<String, String> exprAttrNames = new HashMap<>();

    private List<PColumn> createPkCols(String partitionKey) {
        List<PColumn> pkCols = new ArrayList<>();
        PColumn mockColumn = Mockito.mock(PColumn.class);
        PName mockName = PNameFactory.newName(partitionKey);
        Mockito.when(mockColumn.getName()).thenReturn(mockName);
        pkCols.add(mockColumn);
        return pkCols;
    }

    private List<PColumn> createPkColsWithSortKey(String partitionKey, String sortKey) {
        List<PColumn> pkCols = new ArrayList<>();
        PColumn mockPartitionColumn = Mockito.mock(PColumn.class);
        PName mockPartitionName = PNameFactory.newName(partitionKey);
        Mockito.when(mockPartitionColumn.getName()).thenReturn(mockPartitionName);
        pkCols.add(mockPartitionColumn);

        PColumn mockSortColumn = Mockito.mock(PColumn.class);
        PName mockSortName = PNameFactory.newName(sortKey);
        Mockito.when(mockSortColumn.getName()).thenReturn(mockSortName);
        pkCols.add(mockSortColumn);

        return pkCols;
    }


    @Test(timeout = 120000)
    public void test1() {
        String keyCondExpr = "#0 = :0";
        exprAttrNames.put("#0", "hashKey");
        List<PColumn> pkCols = createPkCols("hashKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertFalse(keyConditions.hasSortKey());
    }

    @Test(timeout = 120000)
    public void test2() {
        String keyCondExpr = "#0 = :0 AND #1 = :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals("=", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test(timeout = 120000)
    public void test3() {
        String keyCondExpr = "#0 = :0 AND #1 > :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test(timeout = 120000)
    public void test4() {
        String keyCondExpr = "#0 = :0 AND #1 < :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals("<", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test(timeout = 120000)
    public void test5() {
        String keyCondExpr = "#0 = :0 AND #1 >= :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">=", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test(timeout = 120000)
    public void test6() {
        String keyCondExpr = "#0 = :0 AND #1 <= :1";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals("<=", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
    }

    @Test(timeout = 120000)
    public void test7() {
        String keyCondExpr1 = "#0 = :0 AND #1 BETWEEN :1 AND :2";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        List<PColumn> pkCols1 = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr1, exprAttrNames, pkCols1, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertTrue(keyConditions.hasBetween());
        Assert.assertEquals("BETWEEN", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
        Assert.assertEquals(":2", keyConditions.getSortKeyValue2());

        String keyCondExpr2 = "#hashKey = :hashKey AND #sortKey BETWEEN :sortKeyFrom AND :sortKeyTo";
        exprAttrNames.put("#hashKey", "Id");
        exprAttrNames.put("#sortKey", "Title");
        List<PColumn> pkCols = createPkCols("Id");
        keyConditions = new KeyConditionsHolder(keyCondExpr2, exprAttrNames, pkCols, false);
        Assert.assertEquals("Id", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("Title", keyConditions.getSortKeyName());
        Assert.assertTrue(keyConditions.hasBetween());
        Assert.assertEquals("BETWEEN", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":sortKeyFrom", keyConditions.getSortKeyValue1());
        Assert.assertEquals(":sortKeyTo", keyConditions.getSortKeyValue2());
    }

    @Test(timeout = 120000)
    public void test8() {
        String keyCondExpr = "#0 = :0 AND begins_with(#1, :1)";
        exprAttrNames.put("#0", "hashKey");
        exprAttrNames.put("#1", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertTrue(keyConditions.hasBeginsWith());
        Assert.assertEquals(":1", keyConditions.getBeginsWithSortKeyVal());
    }

    @Test(timeout = 120000)
    public void test9() {
        exprAttrNames = null;
        String keyCondExpr1 = "hashKey=:0 AND sortKey>:1";
        List<PColumn> pkCols1 = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr1, exprAttrNames, pkCols1, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());

        String keyCondExpr2 = "hashKey= :0 AND sortKey >:1";
        List<PColumn> pkCols2 = createPkColsWithSortKey("hashKey", "sortKey");
        keyConditions = new KeyConditionsHolder(keyCondExpr2, exprAttrNames, pkCols2, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());
        String keyCondExpr3 = "hashKey =:0 AND sortKey> :1";

        List<PColumn> pkCols3 = createPkColsWithSortKey("hashKey", "sortKey");
        keyConditions = new KeyConditionsHolder(keyCondExpr3, exprAttrNames, pkCols3, false);
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":1", keyConditions.getSortKeyValue1());

        exprAttrNames = new HashMap<>();
    }

    @Test(timeout = 120000)
    public void testSortKeyBeforePartitionKey() {
        // Test that order of conditions doesn't matter - sort key can come before partition key
        String keyCondExpr = "#sk > :v1 AND #pk = :v0";
        exprAttrNames.put("#pk", "hashKey");
        exprAttrNames.put("#sk", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);

        // Verify partition key is still recognized
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertEquals(":v0", keyConditions.getPartitionValue());

        // Verify sort key is still recognized
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":v1", keyConditions.getSortKeyValue1());
    }

    @Test(timeout = 120000)
    public void testSortKeyWithEqualBeforePartitionKey() {
        // Test that sort key using = operator and coming first is not mistaken for partition key
        String keyCondExpr = "#sk = :v1 AND #pk = :v0";
        exprAttrNames.put("#pk", "hashKey");
        exprAttrNames.put("#sk", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);

        // Verify partition key is still recognized
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertEquals(":v0", keyConditions.getPartitionValue());

        // Verify sort key is still recognized
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertEquals("=", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":v1", keyConditions.getSortKeyValue1());
    }

    @Test(timeout = 120000)
    public void testSortKeyBetweenBeforePartitionKey() {
        // Test that BETWEEN condition for sort key can come before partition key
        String keyCondExpr = "#sk BETWEEN :v3 AND :v4 AND #pk = :v1";
        exprAttrNames.put("#pk", "hashKey");
        exprAttrNames.put("#sk", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);

        // Verify partition key is still recognized
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertEquals(":v1", keyConditions.getPartitionValue());

        // Verify sort key BETWEEN condition is correctly parsed
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertTrue(keyConditions.hasBetween());
        Assert.assertEquals("BETWEEN", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":v3", keyConditions.getSortKeyValue1());
        Assert.assertEquals(":v4", keyConditions.getSortKeyValue2());
    }

    @Test(timeout = 120000)
    public void testBeginsWithBeforePartitionKey() {
        // Test that begins_with condition for sort key can come before partition key
        String keyCondExpr = "begins_with(#sk, :v2) AND #pk = :v1";
        exprAttrNames.put("#pk", "hashKey");
        exprAttrNames.put("#sk", "sortKey");
        List<PColumn> pkCols = createPkColsWithSortKey("hashKey", "sortKey");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);

        // Verify partition key is still recognized
        Assert.assertEquals("hashKey", keyConditions.getPartitionKeyName());
        Assert.assertEquals(":v1", keyConditions.getPartitionValue());

        // Verify sort key begins_with condition is correctly parsed
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("sortKey", keyConditions.getSortKeyName());
        Assert.assertTrue(keyConditions.hasBeginsWith());
        Assert.assertEquals(":v2", keyConditions.getBeginsWithSortKeyVal());
    }


    @Test(timeout = 120000, expected = IllegalArgumentException.class)
    public void testNullPkCols() {
        // Test that null pkCols throws IllegalArgumentException
        String keyCondExpr = "#0 = :0";
        exprAttrNames.put("#0", "hashKey");
        new KeyConditionsHolder(keyCondExpr, exprAttrNames, null, false);
    }

    @Test(timeout = 120000, expected = IllegalArgumentException.class)
    public void testEmptyPkCols() {
        // Test that empty pkCols throws IllegalArgumentException
        String keyCondExpr = "#0 = :0";
        exprAttrNames.put("#0", "hashKey");
        List<PColumn> pkCols = new ArrayList<>();
        new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
    }

    @Test(timeout = 120000, expected = IllegalArgumentException.class)
    public void testMoreThanTwoPkCols() {
        // Test that more than 2 pkCols throws IllegalArgumentException
        String keyCondExpr = "#0 = :0";
        exprAttrNames.put("#0", "hashKey");
        List<PColumn> pkCols = new ArrayList<>();

        PColumn mockColumn1 = Mockito.mock(PColumn.class);
        PName mockName1 = PNameFactory.newName("col1");
        Mockito.when(mockColumn1.getName()).thenReturn(mockName1);
        pkCols.add(mockColumn1);

        PColumn mockColumn2 = Mockito.mock(PColumn.class);
        PName mockName2 = PNameFactory.newName("col2");
        Mockito.when(mockColumn2.getName()).thenReturn(mockName2);
        pkCols.add(mockColumn2);

        PColumn mockColumn3 = Mockito.mock(PColumn.class);
        PName mockName3 = PNameFactory.newName("col3");
        Mockito.when(mockColumn3.getName()).thenReturn(mockName3);
        pkCols.add(mockColumn3);

        new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
    }

    @Test(timeout = 120000)
    public void testSinglePkColWithOnlyPartitionKey() {
        // Test that single pkCol works correctly with partition key only
        String keyCondExpr = "#pk = :v0";
        exprAttrNames.put("#pk", "userId");
        List<PColumn> pkCols = createPkCols("userId");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);

        Assert.assertEquals("userId", keyConditions.getPartitionKeyName());
        Assert.assertEquals(":v0", keyConditions.getPartitionValue());
        Assert.assertFalse(keyConditions.hasSortKey());
        Assert.assertNull(keyConditions.getSortKeyName());
    }

    @Test(timeout = 120000)
    public void testTwoPkColsWithBothKeys() {
        // Test that two pkCols works correctly with both partition and sort keys
        String keyCondExpr = "#pk = :v0 AND #sk > :v1";
        exprAttrNames.put("#pk", "userId");
        exprAttrNames.put("#sk", "timestamp");
        List<PColumn> pkCols = createPkColsWithSortKey("userId", "timestamp");
        KeyConditionsHolder keyConditions = new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);

        Assert.assertEquals("userId", keyConditions.getPartitionKeyName());
        Assert.assertEquals(":v0", keyConditions.getPartitionValue());
        Assert.assertTrue(keyConditions.hasSortKey());
        Assert.assertEquals("timestamp", keyConditions.getSortKeyName());
        Assert.assertEquals(">", keyConditions.getSortKeyOperator());
        Assert.assertEquals(":v1", keyConditions.getSortKeyValue1());
    }

    @Test(timeout = 120000)
    public void testFailedKeyConditionExpressionWithContains() {
        String keyCondExpr = "#pk = :v0 AND contains(#sk, :v1)";
        exprAttrNames.put("#pk", "userId");
        exprAttrNames.put("#sk", "tags");
        List<PColumn> pkCols = createPkColsWithSortKey("userId", "tags");
        try {
            new KeyConditionsHolder(keyCondExpr, exprAttrNames, pkCols, false);
            Assert.fail("Invalid KeyConditionExpression expected");
        } catch (RuntimeException e) {
            Assert.assertEquals("Invalid KeyConditionExpression: DocumentFieldContainsParseNode",
                e.getMessage());

        }
    }
}