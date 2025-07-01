/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ConditionalPutItem2IT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConditionalPutItem2IT.class);

    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;
    private static final String TABLE_NAME = "Conditional.Put-Item_2.IT";
    private static Map<String, AttributeValue> exprAttrVals = new HashMap<>();
    private static Map<String, String> exprAttrNames = new HashMap<>();
    private static Map<String, AttributeValue> existingItem = getItem("foo", 1, false);
    private static Map<String, AttributeValue> newItem = getItem("foo", 1, true);

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

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(TABLE_NAME, "hk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.N);
        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);
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

    @Test
    public void test1() {
        String conditionExpression =
                "(attribute_not_exists(hk) AND attribute_not_exists(sk)) OR (attribute_not_exists(COL1))";
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        existingItem.remove("COL1");
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test2() {
        String conditionExpression =
                "(attribute_not_exists(hk) AND attribute_not_exists(sk)) OR (attribute_exists(NEWCOL))";
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        existingItem.put("NEWCOL", AttributeValue.builder().n("1").build());
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test3() {
        String conditionExpression =
                "(attribute_not_exists(hk) AND attribute_not_exists(sk)) OR (attribute_not_exists(#1)) OR (#2 = :2)";
        exprAttrNames.put("#1", "NOTEXISTCOL");
        exprAttrNames.put("#2", "COL3");
        exprAttrVals.put(":2", AttributeValue.builder().s("notCol3Value").build());
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrVals.put(":2", existingItem.get("COL3"));
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test4() {
        String conditionExpression =
                "(attribute_exists(hk) AND attribute_exists(sk)) AND attribute_exists(#1)";
        exprAttrNames.put("#1", "COL100");
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrNames.put("#1", "COL3");
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test5() {
        String conditionExpression =
                "(attribute_exists(hk) AND attribute_exists(sk)) AND (attribute_not_exists(#1))";
        exprAttrNames.put("#1", "COL3");
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrNames.put("#1", "COL100");
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test6() {
        String conditionExpression = "#0 = :0 AND attribute_not_exists(#5)";
        exprAttrNames.put("#0", "COL4");
        exprAttrNames.put("#5", "COL3");
        exprAttrVals.put(":0", AttributeValue.builder().n("34").build());
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrNames.put("#5", "COL100");
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test7() {
        String conditionExpression = "#0 = :0 OR attribute_not_exists(#5)";
        exprAttrNames.put("#0", "COL4");
        exprAttrNames.put("#5", "COL3");
        exprAttrVals.put(":0", AttributeValue.builder().n("31").build());
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrNames.put("#5", "COL100");
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test8() {
        String conditionExpression = "#0 = :0 AND attribute_exists(#5)";
        exprAttrNames.put("#0", "COL4");
        exprAttrNames.put("#5", "COL100");
        exprAttrVals.put(":0", AttributeValue.builder().n("31").build());
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrVals.put(":0", AttributeValue.builder().n("34").build());
        exprAttrNames.put("#5", "COL3");
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test9() {
        String conditionExpression = "#0 = :0 OR attribute_exists(#5)";
        exprAttrNames.put("#0", "COL4");
        exprAttrNames.put("#5", "COL100");
        exprAttrVals.put(":0", AttributeValue.builder().n("31").build());
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrVals.put(":0", AttributeValue.builder().n("34").build());
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test10() {
        String conditionExpression =
                "(attribute_not_exists(hk) AND attribute_not_exists(sk)) or (attribute_not_exists(#2) and #s = :s)";
        exprAttrNames.put("#2", "COL4");
        exprAttrNames.put("#s", "COL100");
        exprAttrVals.put(":s", AttributeValue.builder().n("34").build());
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrNames.put("#2", "COL100");
        exprAttrNames.put("#s", "COL4");
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @Test
    public void test11() {
        String conditionExpression =
                "(attribute_exists(hk) AND attribute_exists(sk)) AND attribute_not_exists(#2) AND ((attribute_not_exists(#3)) OR (#3 < :0))";
        exprAttrNames.put("#2", "COL100");
        exprAttrNames.put("#3", "COL4");
        exprAttrVals.put(":0", AttributeValue.builder().n("10").build());
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrVals.put(":0", AttributeValue.builder().n("100").build());
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    /*
        AND
        ├── OR
        │   ├── OR
        │   │   ├── AND
        │   │   │   ├── attribute_not_exists(hk)
        │   │   │   └── attribute_not_exists(sk)
        │   │   └── attribute_exists(#1)
        │   └── AND
        │       ├── #3 = :0
        │       └── OR
        │           ├── #4 = :1
        │           └── #5 = :2
        └── OR
            ├── OR
            │   ├── AND
            │   │   ├── attribute_not_exists(hk)
            │   │   └── attribute_not_exists(sk)
            │   └── attribute_exists(#2)
            └── #6 = :3
     */
    @Test
    public void test12() {
        String conditionExpression =
                "(((attribute_not_exists(hk) AND attribute_not_exists(sk)) OR attribute_exists(#1)) "
                        + "OR ((#3 = :0) AND ((#4 = :1) OR (#5 = :2)))) AND (((attribute_not_exists(hk) AND attribute_not_exists(sk)) "
                        + "OR attribute_exists(#2)) OR (#6 = :3))";
        exprAttrNames.put("#1", "COL1");
        exprAttrNames.put("#2", "COL2");
        exprAttrNames.put("#3", "COL3");
        exprAttrNames.put("#4", "COL4");
        exprAttrNames.put("#5", "COL5");
        exprAttrNames.put("#6", "COL6");
        exprAttrVals.put(":0", AttributeValue.builder().n("10").build());
        exprAttrVals.put(":1", AttributeValue.builder().n("10").build());
        exprAttrVals.put(":2", AttributeValue.builder().n("10").build());
        exprAttrVals.put(":3", AttributeValue.builder().n("10").build());
        // item does not exist
        testPutWithCondition(null, newItem, conditionExpression, exprAttrNames, exprAttrVals);
        // condition expression is false
        exprAttrNames.put("#1", "COL10000");
        exprAttrVals.put(":0", AttributeValue.builder().s("notCol3Value").build());
        exprAttrNames.put("#2", "COL20000");
        exprAttrVals.put(":3", AttributeValue.builder().n("10").build());
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
        // condition expression is true
        exprAttrNames.put("#1", "COL2");
        exprAttrVals.put(":3", AttributeValue.builder().n("-10").build());
        testPutWithCondition(existingItem, newItem, conditionExpression, exprAttrNames,
                exprAttrVals);
    }

    @After
    public void clearExprMaps() {
        exprAttrNames.clear();
        exprAttrVals.clear();
    }

    private void testPutWithCondition(Map<String, AttributeValue> existingItem,
            Map<String, AttributeValue> newItem, String conditionExpression,
            Map<String, String> exprAttrNames, Map<String, AttributeValue> exprAttrVals) {

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("hk", newItem.get("hk"));
        key.put("sk", newItem.get("sk"));

        // Clean up
        DeleteItemRequest dir = DeleteItemRequest.builder().tableName(TABLE_NAME).key(key).build();
        dynamoDbClient.deleteItem(dir);
        phoenixDBClientV2.deleteItem(dir);

        // Insert existing item if present
        PutItemRequest.Builder pir;
        if (existingItem != null) {
            pir = PutItemRequest.builder().tableName(TABLE_NAME).item(existingItem);
            dynamoDbClient.putItem(pir.build());
            phoenixDBClientV2.putItem(pir.build());
        }

        // Put new item
        pir = PutItemRequest.builder().tableName(TABLE_NAME).item(newItem)
                .conditionExpression(conditionExpression);
        if (exprAttrNames != null && !exprAttrNames.isEmpty()) {
            pir.expressionAttributeNames(exprAttrNames);
        }
        if (exprAttrVals != null && !exprAttrVals.isEmpty()) {
            pir.expressionAttributeValues(exprAttrVals);
        }

        boolean ddbThrewException = false, phoenixThrewException = false;
        try {
            dynamoDbClient.putItem(pir.build());
        } catch (ConditionalCheckFailedException e) {
            ddbThrewException = true;
        }
        try {
            phoenixDBClientV2.putItem(pir.build());
        } catch (ConditionalCheckFailedException e) {
            phoenixThrewException = true;
        }
        Assert.assertEquals("ConditionalCheckFailedException did not match.", ddbThrewException,
                phoenixThrewException);

        GetItemRequest getItemRequest =
                GetItemRequest.builder().tableName(TABLE_NAME).key(key).build();
        Assert.assertEquals("Item did not match after PutItem.",
                dynamoDbClient.getItem(getItemRequest).item(),
                phoenixDBClientV2.getItem(getItemRequest).item());
    }

    private static Map<String, AttributeValue> getItem(String hk, int sk, boolean isNew) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("hk", AttributeValue.builder().s(hk).build());
        item.put("sk", AttributeValue.builder().n(Integer.toString(sk)).build());
        item.put("COL1", AttributeValue.builder().n("1").build());
        item.put("COL2", AttributeValue.builder().s("Title1").build());
        item.put("COL3", AttributeValue.builder().s("Description1").build());
        item.put("COL4", AttributeValue.builder().n("34").build());
        item.put("COL6", AttributeValue.builder().n("-10").build());
        item.put("TopLevelSet", AttributeValue.builder().ss("setMember1").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Alice").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar",
                AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        if (isNew) {
            item.put("COL2", AttributeValue.builder().s("Title2").build());
            item.put("COL42", AttributeValue.builder().n("42").build());
        }
        return item;
    }
}
