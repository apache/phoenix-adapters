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

import org.junit.AfterClass;
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
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.ddb.TestUtils.verifyItemsEqual;
import static org.junit.Assert.assertTrue;
import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

@RunWith(Parameterized.class)
public class QueryIndexExclusiveStartKeyIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIndexExclusiveStartKeyIT.class);
    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbClient phoenixDBClientV2;
    private static HBaseTestingUtility utility;
    private static RESTServer restServer;
    private static String tmpDir;

    private static final Map<String, String> TABLE_NAMES = new HashMap<>();
    private static final Map<String, String> INDEX_NAMES = new HashMap<>();

    private final boolean tableHasSK;
    private final boolean indexHasSK;
    private final int limit;

    @Parameterized.Parameters(name = "tableSK={0}_indexSK={1}_limit={2}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        boolean[] bools = {false, true};
        int[] limits = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 19, 23, 37, 43, 49};
        for (boolean tableSK : bools) {
            for (boolean indexSK : bools) {
                for (int limit : limits) {
                    params.add(new Object[]{tableSK, indexSK, limit});
                }
            }
        }
        return params;
    }

    public QueryIndexExclusiveStartKeyIT(boolean tableHasSK, boolean indexHasSK, int limit) {
        this.tableHasSK = tableHasSK;
        this.indexHasSK = indexHasSK;
        this.limit = limit;
    }

    @BeforeClass
    public static void setup() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        utility.startMiniCluster();
        String zkQuorum = "127.0.0.1:" + utility.getZkCluster().getClientPort();
        String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());
        TestUtils.awaitPhoenixReady(url);

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();
        dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());

        boolean[] vals = {false, true};
        for (boolean tableSK : vals) {
            for (boolean indexSK : vals) {
                String key = tableSK + "_" + indexSK;
                String tableName = generateUniqueName() + "_" + tableSK + "_" + indexSK;
                String indexName = "GSI_" + generateUniqueName();
                TABLE_NAMES.put(key, tableName);
                INDEX_NAMES.put(key, indexName);
                createTableAndIndex(tableName, indexName, tableSK, indexSK);
                insertItems(tableName, 200, tableSK, indexSK);
            }
        }
        TestUtils.waitForEventualConsistentIndex();
        TestUtils.waitForEventualConsistentIndex();
    }

    @AfterClass
    public static void teardown() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().stop();
        if (restServer != null) restServer.stop();
        ServerUtil.ConnectionFactory.shutdown();
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            if (utility != null) utility.shutdownMiniCluster();
            ServerMetadataCacheTestImpl.resetCache();
        }
        System.setProperty("java.io.tmpdir", tmpDir);
    }

    @Test(timeout = 120000)
    public void testIndexQueryPagination() {
        String key = tableHasSK + "_" + indexHasSK;
        String tableName = TABLE_NAMES.get(key);
        String indexName = INDEX_NAMES.get(key);

        // Forward scan
        List<Map<String, AttributeValue>> phoenixItems = queryAllPages(phoenixDBClientV2, tableName, indexName, true);
        List<Map<String, AttributeValue>> dynamoItems = queryAllPages(dynamoDbClient, tableName, indexName, true);
        verifyItemsEqual(dynamoItems, phoenixItems, "hk", tableHasSK ? "sk" : null);


        // Reverse scan
        List<Map<String, AttributeValue>> phoenixItemsRev = queryAllPages(phoenixDBClientV2, tableName, indexName, false);
        for (int i = 1; i < phoenixItemsRev.size(); i++) {
            Map<String, AttributeValue> prev = phoenixItemsRev.get(i - 1);
            Map<String, AttributeValue> curr = phoenixItemsRev.get(i);
            int cmp = 0;
            if (indexHasSK && cmp == 0) cmp = Double.compare(
                    Double.parseDouble(curr.get("isk").n()),
                    Double.parseDouble(prev.get("isk").n()));
            if (cmp == 0) cmp = curr.get("hk").s().compareTo(prev.get("hk").s());
            if (tableHasSK && cmp == 0) cmp = curr.get("sk").s().compareTo(prev.get("sk").s());
            assertTrue("Reverse ordering violated at index " + i + ": prev=" + prev + ", curr=" + curr, cmp <= 0);
        }
        List<Map<String, AttributeValue>> dynamoItemsRev = queryAllPages(dynamoDbClient, tableName, indexName, false);
        verifyItemsEqual(dynamoItemsRev, phoenixItemsRev, "hk", tableHasSK ? "sk" : null);
    }

    private static void createTableAndIndex(String tableName, String indexName,
            boolean tableHasSK, boolean indexHasSK) {
        CreateTableRequest req = DDLTestUtils.getCreateTableRequest(
                tableName, "hk", ScalarAttributeType.S,
                tableHasSK ? "sk" : null, tableHasSK ? ScalarAttributeType.S : null);

        req = DDLTestUtils.addIndexToRequest(true, req, indexName,
                "ihk", ScalarAttributeType.S,
                indexHasSK ? "isk" : null, indexHasSK ? ScalarAttributeType.N : null);

        phoenixDBClientV2.createTable(req);
        dynamoDbClient.createTable(req);
    }

    private static void insertItems(String tableName, int count,
            boolean tableHasSK, boolean indexHasSK) {
        for (int i = 1; i <= count; i++) {
            Map<String, AttributeValue> item = new HashMap<>();
            // Create duplicates: groups of 5 share same hk when table has sk
            String hkVal = tableHasSK ? "hk" + (i / 5) : "hk" + i;
            item.put("hk", AttributeValue.builder().s(hkVal).build());
            if (tableHasSK) {
                item.put("sk", AttributeValue.builder().s("sk" + String.format("%03d", i)).build());
            }
            // Create duplicates: 2 distinct ihk values
            item.put("ihk", AttributeValue.builder().s("ihk" + (i % 2)).build());
            if (indexHasSK) {
                // Create duplicates: groups of 3 share same isk
                item.put("isk", AttributeValue.builder().n(String.valueOf((i / 3) * 10)).build());
            }
            item.put("data", AttributeValue.builder().s("data" + i).build());

            PutItemRequest put = PutItemRequest.builder().tableName(tableName).item(item).build();
            phoenixDBClientV2.putItem(put);
            dynamoDbClient.putItem(put);
        }
    }

    private List<Map<String, AttributeValue>> queryAllPages(DynamoDbClient client,
                                                            String tableName, String indexName,
                                                            boolean scanIndexForward) {
        Map<String, String> names = new HashMap<>();
        names.put("#ihk", "ihk");

        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":ihk", AttributeValue.builder().s("ihk1").build());

        String keyCondExpr = "#ihk = :ihk";
        if (indexHasSK) {
            names.put("#isk", "isk");
            values.put(":isk", AttributeValue.builder().n("500").build());
            keyCondExpr = "#ihk = :ihk AND #isk <= :isk";
        }

        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        Map<String, AttributeValue> lastKey = null;

        int pageCount = 0;
        do {
            QueryRequest.Builder qb = QueryRequest.builder()
                    .tableName(tableName)
                    .indexName(indexName)
                    .keyConditionExpression(keyCondExpr)
                    .expressionAttributeNames(names)
                    .expressionAttributeValues(values)
                    .scanIndexForward(scanIndexForward)
                    .limit(limit);

            if (lastKey != null && !lastKey.isEmpty()) {
                qb.exclusiveStartKey(lastKey);
            }

            QueryResponse result = client.query(qb.build());
            allItems.addAll(result.items());
            lastKey = result.lastEvaluatedKey();
            pageCount++;
        } while (lastKey != null && !lastKey.isEmpty());
        LOGGER.info("Page Count : {}", pageCount);
        return allItems;
    }
}
