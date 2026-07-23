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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ComparisonOperator;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ConditionalOperator;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_NEW;
import static software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_OLD;

/**
 * Tests for UpdateItem API without conditional updates.
 * Every test does 3 things:
 * 1. Puts a row into the phoenix,ddb tables
 * 2. Updates this row
 * 3. Gets row from both phoenix and ddb to validate the update
 * <p>
 * {@link UpdateItemIT} has more tests for UpdateItem API.
 */
@RunWith(Parameterized.class)
public class UpdateItemBaseTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemBaseTests.class);

    protected final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    protected static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    protected boolean isSortKeyPresent;

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());

        TestUtils.awaitPhoenixReady(url);

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
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

    @Parameters(name = "SortKey_{0}")
    public static synchronized Collection<Boolean> data() {
        return Arrays.asList(false, true);
    }

    public UpdateItemBaseTests(boolean isSortKeyPresent) {
        this.isSortKeyPresent = isSortKeyPresent;
    }

    /**
     * SET: Adds one or more attributes and values to an item. If any of these attributes already
     * exist, they are replaced by the new values. You can also use SET to add or subtract from an
     * attribute that is of type Number.
     */
    @Test(timeout = 120000)
    public void testSet1() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "SET #1 = :v1, #2 = #2 + :v2, #3 = #3 - :v3, #4 = if_not_exists (#4, :v4)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "NEWCOL");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("3.2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("89.34").build());
        exprAttrVal.put(":v4",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, -1})).build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    /**
     * SET: test arithmetic operations with if_not_exists function.
     */
    @Test(timeout = 120000)
    public void testSet2() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = if_not_exists(#1, :v100) + :v1, " +
                "#4 = :v100 - if_not_exists( #4, :v100), " +
                "#n1 = if_not_exists(#n1, :v0) + :v100,  " +
                "#n2 = if_not_exists(#n2, :v110)- :v10, " +
                "#n5 = if_not_exists(#n5, :v10)  + if_not_exists(#5,  :v0), " +
                "NEWCOL6 = if_not_exists(NEWCOL6, :v10)");

        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        exprAttrNames.put("#4", "COL4");
        exprAttrNames.put("#n1", "NEWCOL1");
        exprAttrNames.put("#n2", "NEWCOL2");
        exprAttrNames.put("#5", "COL5");
        exprAttrNames.put("#n5", "NEWCOL5");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().n("1").build());
        exprAttrVal.put(":v10", AttributeValue.builder().n("10").build());
        exprAttrVal.put(":v100", AttributeValue.builder().n("100").build());
        exprAttrVal.put(":v110", AttributeValue.builder().n("110").build());
        exprAttrVal.put(":v0", AttributeValue.builder().n("0").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    /**
     * SET a literal string value that contains " - " and " + ". Such literals must be stored
     * verbatim and must never be interpreted as arithmetic. Regression for a production UpdateItem
     * failure where a JSON "properties" string contained " - " and Phoenix threw
     * "Operand ... does not exist".
     */
    @Test(timeout = 120000)
    public void testSetLiteralStringWithArithmeticOperators() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #props = :props, #eq = :eq, #co = :co");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#props", "Properties");
        exprAttrNames.put("#eq", "Equation");
        exprAttrNames.put("#co", "Company");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        // Literal JSON string containing " - ".
        exprAttrVal.put(":props", AttributeValue.builder()
                .s("{\"teamName\":\"Foo - Bar Baz Service\",\"channelName\":\"#foo-notifications\"}")
                .build());
        // Literal string containing " + ".
        exprAttrVal.put(":eq", AttributeValue.builder().s("E = mc^2 + offset").build());
        // Plain literal that looks like a subtraction of two field names.
        exprAttrVal.put(":co", AttributeValue.builder().s("Acme - Widgets").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    /**
     * SET: list_append against an existing list attribute path, plus a parallel
     * "create-or-append" using if_not_exists for a previously-missing list, plus a
     * subsequent re-apply that exercises both the existing-path operand and a literal
     * operand.
     */
    @Test(timeout = 120000)
    public void testListAppend() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // First update: create NewList via if_not_exists, append literal items to it.
        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression(
                "SET #newList = list_append(if_not_exists(#newList, :empty), :items1)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#newList", "NewList");
        uir1.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":empty", AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        exprAttrVal1.put(":items1", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("b").build()).build());
        uir1.expressionAttributeValues(exprAttrVal1);
        dynamoDbClient.updateItem(uir1.build());
        phoenixDBClientV2.updateItem(uir1.build());

        validateItem(tableName, key);

        // Second update: append more items to the now-existing list (path operand + literal).
        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression("SET #newList = list_append(#newList, :items2)");
        uir2.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":items2", AttributeValue.builder().l(
                AttributeValue.builder().s("c").build(),
                AttributeValue.builder().s("d").build()).build());
        uir2.expressionAttributeValues(exprAttrVal2);
        dynamoDbClient.updateItem(uir2.build());
        phoenixDBClientV2.updateItem(uir2.build());

        validateItem(tableName, key);

        // Third update: prepend (literal first, path second).
        // After this, the final list order is [z, a, b, c, d] — the literal is prepended.
        UpdateItemRequest.Builder uir3 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir3.updateExpression("SET #newList = list_append(:items3, #newList)");
        uir3.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":items3", AttributeValue.builder().l(
                AttributeValue.builder().s("z").build()).build());
        uir3.expressionAttributeValues(exprAttrVal3);
        dynamoDbClient.updateItem(uir3.build());
        phoenixDBClientV2.updateItem(uir3.build());

        validateItem(tableName, key);
    }

    /**
     * Create-or-append where the same operation is invoked twice with an overlapping
     * payload, leaving duplicates in the resulting list (list_append preserves order
     * and does not de-duplicate).
     */
    @Test(timeout = 120000)
    public void testListAppendCreateOrAppendWithDuplicates() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "SET #newList = list_append(if_not_exists(#newList, :empty), :items)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#newList", "NewList");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":empty",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        // payload contains an internal duplicate ("a" twice) and is then re-applied to itself.
        exprAttrVal.put(":items", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("b").build(),
                AttributeValue.builder().s("a").build()).build());
        uir.expressionAttributeValues(exprAttrVal);

        // First apply: NewList does not exist; create it via if_not_exists fallback and
        // append the literal payload, yielding [a, b, a].
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());
        validateItem(tableName, key);

        // Second apply: NewList now exists; append the same literal payload again,
        // yielding [a, b, a, a, b, a]. Confirms list_append preserves duplicates and
        // is not idempotent.
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(
                uir.returnValues(ALL_NEW).build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(
                uir.returnValues(ALL_NEW).build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        AttributeValue finalList = phoenixResult.attributes().get("NewList");
        Assert.assertNotNull("NewList should be present in returned attributes", finalList);
        Assert.assertEquals("list_append preserves order and duplicates",
                java.util.Arrays.asList("a", "b", "a", "a", "b", "a"),
                finalList.l().stream().map(AttributeValue::s).collect(java.util.stream.Collectors.toList()));

        validateItem(tableName, key);
    }

    /**
     * Prepend variant: list_append with a literal list as the first operand and an
     * if_not_exists fallback as the second operand. New items end up before the
     * existing-or-fallback list.
     */
    @Test(timeout = 120000)
    public void testListAppendLiteralAndIfNotExists() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // First apply: NewList does not exist, so if_not_exists falls back to []. The
        // result is the literal :items prepended to that empty list -> [a, b].
        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression(
                "SET #newList = list_append(:items1, if_not_exists(#newList, :empty))");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#newList", "NewList");
        uir1.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":empty",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        exprAttrVal1.put(":items1", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("b").build()).build());
        uir1.expressionAttributeValues(exprAttrVal1);
        dynamoDbClient.updateItem(uir1.build());
        phoenixDBClientV2.updateItem(uir1.build());
        validateItem(tableName, key);

        // Second apply: NewList now exists; literal :items2 is prepended in front of it.
        // Result: [c, d, a, b].
        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir2.updateExpression(
                "SET #newList = list_append(:items2, if_not_exists(#newList, :empty))");
        uir2.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":empty",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        exprAttrVal2.put(":items2", AttributeValue.builder().l(
                AttributeValue.builder().s("c").build(),
                AttributeValue.builder().s("d").build()).build());
        uir2.expressionAttributeValues(exprAttrVal2);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir2.returnValues(ALL_NEW).build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir2.returnValues(ALL_NEW).build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        AttributeValue finalList = phoenixResult.attributes().get("NewList");
        Assert.assertNotNull("NewList should be present in returned attributes", finalList);
        Assert.assertEquals("literal-first list_append prepends to existing list",
                java.util.Arrays.asList("c", "d", "a", "b"),
                finalList.l().stream().map(AttributeValue::s)
                        .collect(java.util.stream.Collectors.toList()));

        validateItem(tableName, key);
    }

    /**
     * Create-or-append a list while atomically incrementing a counter in the same SET
     * clause, gated on an updateCounter condition expression.
     */
    @Test(timeout = 120000)
    public void testListAppendWithIfNotExistsAndCounterIncrement() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Iteration 1: events does not exist; updateCounter starts unset, so initialize it
        // first via a plain SET so the condition expression can reference it deterministically.
        UpdateItemRequest.Builder seed = UpdateItemRequest.builder().tableName(tableName).key(key);
        seed.updateExpression("SET updateCounter = :zero");
        Map<String, AttributeValue> seedVals = new HashMap<>();
        seedVals.put(":zero", AttributeValue.builder().n("0").build());
        seed.expressionAttributeValues(seedVals);
        dynamoDbClient.updateItem(seed.build());
        phoenixDBClientV2.updateItem(seed.build());

        // Iteration 2: list_append(if_not_exists(events, :empty), :evts1) + counter increment,
        // gated on updateCounter = 0.
        UpdateItemRequest.Builder uir1 = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir1.updateExpression(
                "SET events = list_append(if_not_exists(events, :empty_list), :new_evts), "
                        + "updateCounter = updateCounter + :one");
        uir1.conditionExpression("updateCounter = :expected");
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":empty_list",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        exprAttrVal1.put(":new_evts", AttributeValue.builder().l(
                AttributeValue.builder().s("ev1").build(),
                AttributeValue.builder().s("ev2").build()).build());
        exprAttrVal1.put(":one", AttributeValue.builder().n("1").build());
        exprAttrVal1.put(":expected", AttributeValue.builder().n("0").build());
        uir1.expressionAttributeValues(exprAttrVal1);
        dynamoDbClient.updateItem(uir1.build());
        phoenixDBClientV2.updateItem(uir1.build());

        validateItem(tableName, key);

        // Iteration 3: append more events; gated on the post-iter-2 counter value.
        UpdateItemRequest.Builder uir2 = UpdateItemRequest.builder().tableName(tableName).key(key);
        // Extra whitespace around closing paren and comma to verify parser tolerance.
        uir2.updateExpression(
                "SET events = list_append(if_not_exists(events, :empty_list), :new_evts  )   , "
                        + "updateCounter = updateCounter + :one");
        uir2.conditionExpression("updateCounter = :expected");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":empty_list",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        exprAttrVal2.put(":new_evts", AttributeValue.builder().l(
                AttributeValue.builder().s("ev3").build()).build());
        exprAttrVal2.put(":one", AttributeValue.builder().n("1").build());
        exprAttrVal2.put(":expected", AttributeValue.builder().n("1").build());
        uir2.expressionAttributeValues(exprAttrVal2);
        dynamoDbClient.updateItem(uir2.build());
        phoenixDBClientV2.updateItem(uir2.build());

        validateItem(tableName, key);
    }

    /**
     * REMOVE - Removes one or more attributes from an item.
     */
    @Test(timeout = 120000)
    public void testRemove() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("REMOVE #1.#2[0], #3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    /**
     * The ADD action only supports Number and set data types.
     * In addition, ADD can only be used on top-level attributes, not nested attributes.
     * Both sets must have the same primitive data type.
     * <p>
     * TODO: test with add fails if item had Double to begin with,
     * e.g. 34.15 + 89.21, ddb --> 123.36, phoenix--> 123.35999999999999
     * but 34 + 89.21, ddb=phoenix-->123.21
     */
    @Test(timeout = 120000)
    public void testAdd() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("ADD #2 :v2, #3 :v3, #4 :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "TopLevelSet");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().n("-3.2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("89.21").build());
        exprAttrVal.put(":v4", AttributeValue.builder().ss("setMember2").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    /**
     * The DELETE action only supports set data types.
     * In addition, DELETE can only be used on top-level attributes, not nested attributes.
     */
    @Test(timeout = 120000)
    public void testDelete() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("DELETE #4 :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#4", "TopLevelSet");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v4", AttributeValue.builder().ss("setMember1", "setMember2").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testSetDelete() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = :v1, #2 = #2 + :v2 DELETE #4 :v4 ");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#4", "TopLevelSet");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("3.2").build());
        exprAttrVal.put(":v4", AttributeValue.builder().ss("setMember1").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testRemoveAdd() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("REMOVE #3, #1.#2[0] ADD #4 :v4, #5 :v5");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "COL1");
        exprAttrNames.put("#4", "TopLevelSet");
        exprAttrNames.put("#5", "COL4");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v5", AttributeValue.builder().n("-3.2").build());
        exprAttrVal.put(":v4", AttributeValue.builder().ss("setMember2").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testAddSetRemove() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("ADD #1 :v1, #2 :v2 SET #3 = :v3, #4 = #4 - :v4 REMOVE #5");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        exprAttrNames.put("#2", "TopLevelSet");
        exprAttrNames.put("#3", "COL2");
        exprAttrNames.put("#4", "COL4");
        exprAttrNames.put("#5", "COL3");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().n("-3.2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().ss("setMember2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v4", AttributeValue.builder().n("-3").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testDeleteRemoveSetAdd() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "DELETE #1 :v1 REMOVE #2 ADD #3 :v3 SET #5 = if_not_exists(#6, :v5), #4 = :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "TopLevelSet");
        exprAttrNames.put("#2", "Reviews");
        exprAttrNames.put("#3", "COL1");
        exprAttrNames.put("#4", "COL3");
        exprAttrNames.put("#5", "NEWCOL");
        exprAttrNames.put("#6", "COL2");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().ss("setMember2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("1000000").build());
        exprAttrVal.put(":v4", AttributeValue.builder().s("dEsCrIpTiOn1").build());
        exprAttrVal.put(":v5", AttributeValue.builder().s("Bob").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testCounterIncrement1() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "SET #counter = if_not_exists(#counter, :start) + :increment");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#counter", "Counter");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":start", AttributeValue.builder().n("1").build());
        exprAttrVal.put(":increment", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testCounterIncrement2() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "SET #counter = if_not_exists(#counter, :start) + :increment");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#counter", "Counter");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":start", AttributeValue.builder().n("1").build());
        exprAttrVal.put(":increment", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testCounterIncrement3() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "SET #counter = if_not_exists(#counter, :start) + :increment");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#counter", "Counter");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":start", AttributeValue.builder().n("1").build());
        exprAttrVal.put(":increment", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testCounterIncrement4() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "SET #counter = if_not_exists(#counter, :start) + :increment");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#counter", "Counter");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":start", AttributeValue.builder().n("1").build());
        exprAttrVal.put(":increment", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testDeleteRemoveSetAddNonExistentItemSuccess() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "DELETE #1 :v1 REMOVE #2 ADD #3 :v3 SET #5 = if_not_exists(#5, :v5) + :v51, #4 = :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "TopLevelSet");
        exprAttrNames.put("#2", "Reviews");
        exprAttrNames.put("#3", "COL1");
        exprAttrNames.put("#4", "COL3");
        exprAttrNames.put("#5", "NEWCOL");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().ss("setMember2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("1000000").build());
        exprAttrVal.put(":v4", AttributeValue.builder().s("dEsCrIpTiOn1").build());
        exprAttrVal.put(":v5", AttributeValue.builder().n("1").build());
        exprAttrVal.put(":v51", AttributeValue.builder().n("10").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testDeleteRemoveSetAddNonExistentItemFail() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
                "DELETE #1 :v1 REMOVE #2 ADD #3 :v3 SET #5 = #5 + :v5, #4 = :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "TopLevelSet");
        exprAttrNames.put("#2", "Reviews");
        exprAttrNames.put("#3", "COL1");
        exprAttrNames.put("#4", "COL3");
        exprAttrNames.put("#5", "NEWCOL");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().ss("setMember2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("1000000").build());
        exprAttrVal.put(":v4", AttributeValue.builder().s("dEsCrIpTiOn1").build());
        exprAttrVal.put(":v5", AttributeValue.builder().n("1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        try {
            UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        try {
            UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        validateItem(tableName, key);
    }

    protected void createTableAndPutItem(String tableName, boolean putItem) {
        //create table
        CreateTableRequest createTableRequest;
        if (isSortKeyPresent) {
            createTableRequest =
                    DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                            "PK2", ScalarAttributeType.N);
        } else {
            createTableRequest =
                    DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S,
                            null, null);
        }

        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        if (putItem) {
            Map<String, AttributeValue> item = getItem1();
            PutItemRequest putItemRequest =
                    PutItemRequest.builder().tableName(tableName).item(item).build();
            phoenixDBClientV2.putItem(putItemRequest);
            dynamoDbClient.putItem(putItemRequest);
        }
    }

    protected void validateItem(String tableName, Map<String, AttributeValue> key) {
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gir);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gir);
        ItemComparator.areItemsEqual(dynamoResult.item(), phoenixResult.item());
    }

    protected Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("A").build());
        item.put("PK2", AttributeValue.builder().n("1").build());
        item.put("COL1", AttributeValue.builder().n("1").build());
        item.put("COL2", AttributeValue.builder().s("Title1").build());
        item.put("COL3", AttributeValue.builder().s("Description1").build());
        item.put("COL4", AttributeValue.builder().n("34").build());
        item.put("COL5", AttributeValue.builder().n("67").build());
        item.put("TopLevelSet", AttributeValue.builder().ss("setMember1").build());
        item.put("Counter", AttributeValue.builder().n("66").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Alice").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar",
                AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    /**
     * Test AttributeUpdates legacy parameter support with various data types.
     * This tests that the legacy AttributeUpdates parameter works equivalently to UpdateExpression
     * with comprehensive coverage of different data types and actions.
     */
    @Test(timeout = 120000)
    public void testAttributeUpdates() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        // Create update request using legacy AttributeUpdates parameter
        Map<String, AttributeValue> key = getKey();

        // Build AttributeUpdates map with various data types and actions
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();

        // PUT action with String (equivalent to SET)
        attributeUpdates.put("COL2", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s("UpdatedTitle").build()).build());

        // PUT action with Number
        attributeUpdates.put("NewNumberField",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().n("42.5").build()).build());

        // PUT action with Boolean
        attributeUpdates.put("NewBooleanField",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().bool(true).build()).build());

        // PUT action with Binary
        attributeUpdates.put("NewBinaryField",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder()
                                .b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build()).build());

        // PUT action with String Set
        attributeUpdates.put("NewStringSet",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().ss("value1", "value2", "value3").build())
                        .build());

        // PUT action with Number Set
        attributeUpdates.put("NewNumberSet",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().ns("1", "2", "3").build()).build());

        // PUT action with Map (nested object)
        Map<String, AttributeValue> nestedMap = new HashMap<>();
        nestedMap.put("nestedString", AttributeValue.builder().s("nested value").build());
        nestedMap.put("nestedNumber", AttributeValue.builder().n("100").build());
        attributeUpdates.put("NewMapField",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().m(nestedMap).build()).build());

        // PUT action with List
        attributeUpdates.put("NewListField",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder()
                                .l(AttributeValue.builder().s("item1").build(),
                                        AttributeValue.builder().n("42").build(),
                                        AttributeValue.builder().bool(false).build()).build())
                        .build());

        // ADD action with Number
        attributeUpdates.put("COL1", AttributeValueUpdate.builder().action(AttributeAction.ADD)
                .value(AttributeValue.builder().n("5.5").build()).build());

        // ADD action with String Set (should add to existing set)
        attributeUpdates.put("AddToStringSet",
                AttributeValueUpdate.builder().action(AttributeAction.ADD)
                        .value(AttributeValue.builder().ss("newValue1", "newValue2").build())
                        .build());

        // ADD action with Number Set
        attributeUpdates.put("AddToNumberSet",
                AttributeValueUpdate.builder().action(AttributeAction.ADD)
                        .value(AttributeValue.builder().ns("10", "20").build()).build());

        // DELETE action without value (remove entire attribute)
        attributeUpdates.put("COL4",
                AttributeValueUpdate.builder().action(AttributeAction.DELETE).build());

        // DELETE action with String Set value (remove specific values from set)
        attributeUpdates.put("DeleteFromStringSet",
                AttributeValueUpdate.builder().action(AttributeAction.DELETE)
                        .value(AttributeValue.builder().ss("removeMe").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).build();

        // Execute update with AttributeUpdates on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test AttributeUpdates with default PUT action (when Action is not specified).
     */
    @Test(timeout = 120000)
    public void testAttributeUpdatesDefaultAction() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build AttributeUpdates without specifying Action (should default to PUT)
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();

        attributeUpdates.put("DefaultActionField", AttributeValueUpdate.builder()
                // No action specified - should default to PUT
                .value(AttributeValue.builder().s("DefaultPutValue").build()).build());

        attributeUpdates.put("COL2", AttributeValueUpdate.builder()
                // No action specified - should default to PUT
                .value(AttributeValue.builder().s("OverwrittenTitle").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).build();

        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        validateItem(tableName, key);
    }

    /**
     * Legacy AttributeUpdates PUT of literal string values that contain " - " and " + ". Same
     * regression as {@link #testSetLiteralStringWithArithmeticOperators()}, exercised through the
     * legacy AttributeUpdates parameter path. Literals must be stored verbatim and never parsed as
     * arithmetic.
     */
    @Test(timeout = 120000)
    public void testAttributeUpdatesLiteralStringWithArithmeticOperators() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        // PUT a literal JSON string containing " - ".
        attributeUpdates.put("Properties", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder()
                        .s("{\"teamName\":\"Foo - Bar Baz Service\",\"channelName\":\"#foo-notifications\"}")
                        .build()).build());
        // PUT a literal string containing " + ".
        attributeUpdates.put("Language", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s("C++ - systems programming").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).build();

        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        validateItem(tableName, key);
    }

    /**
     * Test AttributeUpdates validation - should reject when both UpdateExpression and AttributeUpdates are provided.
     */
    @Test(timeout = 120000)
    public void testAttributeUpdatesValidation() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("test", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s("value").build()).build());

        // This should fail - cannot specify both UpdateExpression and AttributeUpdates
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":val", AttributeValue.builder().s("value").build());

        UpdateItemRequest invalidRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET testField = :val").expressionAttributeValues(exprAttrValues)
                .attributeUpdates(attributeUpdates).build();

        // Should throw ValidationException for both DynamoDB and Phoenix
        try {
            dynamoDbClient.updateItem(invalidRequest);
            Assert.fail("Expected ValidationException for DynamoDB");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.updateItem(invalidRequest);
            Assert.fail("Expected ValidationException for Phoenix");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
    }

    /**
     * Test legacy Expected parameter support with various comparison operators.
     * This tests that the legacy Expected parameter works equivalently to ConditionExpression.
     */
    @Test(timeout = 120000)
    public void testExpectedParameter() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with various comparison operators
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // EQ condition (default operator) - COL1 has value "1"
        expected.put("COL1",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("1").build())
                        .build());

        // EXISTS condition (using NOT_NULL comparison operator)
        expected.put("COL2",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NOT_NULL)
                        .build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("COL2", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s("UpdatedWithExpected").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected).build();

        // Execute update with Expected on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with ConditionalOperator OR.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterWithOrOperator() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with OR operator
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // Condition 1: COL1 equals a different value (should be false)
        expected.put("COL1",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("999").build())
                        .comparisonOperator(ComparisonOperator.EQ).build());

        // Condition 2: COL2 exists (should be true)
        expected.put("COL2",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NOT_NULL)
                        .build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("COL2", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s("UpdatedWithOrCondition").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected)
                .conditionalOperator(ConditionalOperator.OR) // OR operator
                .build();

        // Execute update with Expected OR on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with various comparison operators.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterComparisonOperators() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with different comparison operators
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // GT condition (COL4 > 30) - COL4 has value 34
        expected.put("COL4",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("30").build())
                        .comparisonOperator(ComparisonOperator.GT).build());

        // BEGINS_WITH condition (COL2 begins with "Ti") - COL2 has value "Title1"
        expected.put("COL2",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().s("Ti").build())
                        .comparisonOperator(ComparisonOperator.BEGINS_WITH).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("NewField", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s("AddedByComparison").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected)
                .conditionalOperator(ConditionalOperator.AND) // AND operator (default)
                .build();

        // Execute update with Expected comparison operators on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter validation - should reject when both ConditionExpression and Expected are provided.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterValidation() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        Map<String, ExpectedAttributeValue> expected = new HashMap<>();
        expected.put("COL1",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("1").build())
                        .build());

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("test", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s("value").build()).build());

        // This should fail - cannot specify both ConditionExpression and Expected
        UpdateItemRequest invalidRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .conditionExpression("attribute_exists(COL1)").expected(expected)
                .attributeUpdates(attributeUpdates).build();

        try {
            dynamoDbClient.updateItem(invalidRequest);
            Assert.fail("Expected ValidationException for DynamoDB");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.updateItem(invalidRequest);
            Assert.fail("Expected ValidationException for Phoenix");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }
    }

    /**
     * Test legacy Expected parameter with exists(true) and exists(false) conditions.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterExistsConditions1() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with exists conditions
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // exists(true) - attribute should exist
        expected.put("COL2", ExpectedAttributeValue.builder().exists(true)
                .value(AttributeValue.builder().s("1").build()).build());

        // exists(false) - attribute should not exist
        expected.put("NonExistentField", ExpectedAttributeValue.builder().exists(false).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("UpdatedField",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("UpdatedWithExists").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected)
                .conditionalOperator(ConditionalOperator.AND).build();

        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("DeleteItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("DeleteItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with exists(true) and exists(false) conditions.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterExistsConditions2() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with exists conditions
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // exists(true) - attribute should exist
        expected.put("COL2", ExpectedAttributeValue.builder().exists(true)
                .value(AttributeValue.builder().s("Title1").build()).build());

        // exists(false) - attribute should not exist
        expected.put("NonExistentField", ExpectedAttributeValue.builder().exists(false).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("UpdatedField",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("UpdatedWithExists").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected)
                .conditionalOperator(ConditionalOperator.AND).build();

        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with BETWEEN operator.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterBetweenOperator() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with BETWEEN operator
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // BETWEEN condition (COL4 BETWEEN 30 AND 40) - COL4 has value 34
        expected.put("COL4", ExpectedAttributeValue.builder().attributeValueList(
                        Arrays.asList(AttributeValue.builder().n("30").build(),
                                AttributeValue.builder().n("40").build()))
                .comparisonOperator(ComparisonOperator.BETWEEN).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("BetweenTest",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("UpdatedWithBetween").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected).build();

        // Execute update with BETWEEN condition on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with IN operator.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterInOperator() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with IN operator
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // IN condition (COL2 IN ["Title1", "Title2", "Title3"]) - COL2 has value "Title1"
        expected.put("COL2", ExpectedAttributeValue.builder().attributeValueList(
                        Arrays.asList(AttributeValue.builder().s("Title1").build(),
                                AttributeValue.builder().s("Title2").build(),
                                AttributeValue.builder().s("Title3").build()))
                .comparisonOperator(ComparisonOperator.IN).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("InTest", AttributeValueUpdate.builder().action(AttributeAction.PUT)
                .value(AttributeValue.builder().s("UpdatedWithIn").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected).build();

        // Execute update with IN condition on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with CONTAINS and NOT_CONTAINS operators.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterContainsOperators() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with CONTAINS operators
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // CONTAINS condition (COL2 CONTAINS "itle") - COL2 has value "Title1"
        expected.put("COL2",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().s("itle").build())
                        .comparisonOperator(ComparisonOperator.CONTAINS).build());

        // NOT_CONTAINS condition (COL3 NOT_CONTAINS "xyz") - COL3 has value "Description1"
        expected.put("COL3",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().s("xyz").build())
                        .comparisonOperator(ComparisonOperator.NOT_CONTAINS).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("ContainsTest",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("UpdatedWithContains").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected)
                .conditionalOperator(ConditionalOperator.AND).build();

        // Execute update with CONTAINS conditions on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with all numeric comparison operators.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterNumericComparisons() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with numeric comparison operators
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // LT condition (COL4 < 40) - COL4 has value 34
        expected.put("COL4",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("40").build())
                        .comparisonOperator(ComparisonOperator.LT).build());

        // GE condition (COL1 >= 1) - COL1 has value 1
        expected.put("COL1",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("1").build())
                        .comparisonOperator(ComparisonOperator.GE).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("NumericTest",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("UpdatedWithNumeric").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected)
                .conditionalOperator(ConditionalOperator.AND).build();

        // Execute update with numeric comparisons on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with NE (not equal) operator.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterNotEqualOperator() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with NE operator
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // NE condition (COL2 != "WrongTitle") - COL2 has value "Title1"
        expected.put("COL2", ExpectedAttributeValue.builder()
                .value(AttributeValue.builder().s("WrongTitle").build())
                .comparisonOperator(ComparisonOperator.NE).build());

        // NE condition (COL1 != 999) - COL1 has value 1
        expected.put("COL1",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("999").build())
                        .comparisonOperator(ComparisonOperator.NE).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("NotEqualTest",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("UpdatedWithNotEqual").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected)
                .conditionalOperator(ConditionalOperator.AND).build();

        // Execute update with NE conditions on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with complex OR conditions.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterComplexOrConditions() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with complex OR conditions
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // Condition 1: COL1 = 999 (should be false, COL1 = 1)
        expected.put("COL1",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("999").build())
                        .comparisonOperator(ComparisonOperator.EQ).build());

        // Condition 2: COL2 BEGINS_WITH "Wrong" (should be false, COL2 = "Title1")
        expected.put("COL2",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().s("Wrong").build())
                        .comparisonOperator(ComparisonOperator.BEGINS_WITH).build());

        // Condition 3: COL4 > 30 (should be true, COL4 = 34)
        expected.put("COL4",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("30").build())
                        .comparisonOperator(ComparisonOperator.GT).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("ComplexOrTest",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("UpdatedWithComplexOr").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected).conditionalOperator(
                        ConditionalOperator.OR) // OR - only one condition needs to be true
                .build();

        // Execute update with complex OR conditions on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter failure case - condition should fail.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterConditionFailure() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with conditions that should fail
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // Condition that will fail: COL1 = 999 (but COL1 = 1)
        expected.put("COL1",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("999").build())
                        .comparisonOperator(ComparisonOperator.EQ).build());

        // Build AttributeUpdates for the update
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("ShouldNotUpdate",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("ShouldNotBeSet").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected).build();

        // Both DynamoDB and Phoenix should throw ConditionalCheckFailedException
        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("Expected ConditionalCheckFailedException for DynamoDB");
        } catch (Exception e) {
            Assert.assertTrue("Expected ConditionalCheckFailedException",
                    e.getMessage().contains("conditional") || e.getMessage()
                            .contains("Conditional"));
        }

        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Expected ConditionalCheckFailedException for Phoenix");
        } catch (Exception e) {
            Assert.assertTrue("Expected ConditionalCheckFailedException",
                    e.getMessage().contains("conditional") || e.getMessage()
                            .contains("Conditional"));
        }
    }

    /**
     * Test legacy Expected parameter with mixed data types and complex conditions.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterMixedDataTypes() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Build Expected map with mixed data types
        Map<String, ExpectedAttributeValue> expected = new HashMap<>();

        // Set existence check
        expected.put("TopLevelSet",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NOT_NULL)
                        .build());

        // String comparison
        expected.put("COL2",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().s("Title1").build())
                        .comparisonOperator(ComparisonOperator.EQ).build());

        // Number comparison
        expected.put("COL4",
                ExpectedAttributeValue.builder().value(AttributeValue.builder().n("35").build())
                        .comparisonOperator(ComparisonOperator.LE) // 34 <= 35
                        .build());

        // Build AttributeUpdates with mixed operations
        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();

        // PUT operation
        attributeUpdates.put("MixedTest1",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("MixedDataTest").build()).build());

        // ADD operation to existing number
        attributeUpdates.put("COL1", AttributeValueUpdate.builder().action(AttributeAction.ADD)
                .value(AttributeValue.builder().n("10").build()).build());

        // ADD to existing set
        attributeUpdates.put("TopLevelSet",
                AttributeValueUpdate.builder().action(AttributeAction.ADD)
                        .value(AttributeValue.builder().ss("newSetMember").build()).build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates).expected(expected)
                .conditionalOperator(ConditionalOperator.AND).build();

        // Execute update with mixed data types on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both produce the same result
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with NOT_NULL and NULL comparison operators.
     * Tests both positive (should succeed) and negative (should fail) cases.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterNotNullAndNullOperators() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Test 1: NOT_NULL on existing attribute (should succeed)
        Map<String, ExpectedAttributeValue> expected1 = new HashMap<>();
        expected1.put("COL2",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NOT_NULL)
                        .build());

        Map<String, AttributeValueUpdate> attributeUpdates1 = new HashMap<>();
        attributeUpdates1.put("TestField1",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("NotNullTest").build()).build());

        UpdateItemRequest updateRequest1 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates1).expected(expected1).build();

        // Both DynamoDB and Phoenix should succeed
        dynamoDbClient.updateItem(updateRequest1);
        phoenixDBClientV2.updateItem(updateRequest1);

        // Test 2: NULL on non-existing attribute (should succeed)
        Map<String, ExpectedAttributeValue> expected2 = new HashMap<>();
        expected2.put("NonExistentField",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NULL)
                        .build());

        Map<String, AttributeValueUpdate> attributeUpdates2 = new HashMap<>();
        attributeUpdates2.put("TestField2",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("NullTest").build()).build());

        UpdateItemRequest updateRequest2 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates2).expected(expected2).build();

        // Both DynamoDB and Phoenix should succeed
        dynamoDbClient.updateItem(updateRequest2);
        phoenixDBClientV2.updateItem(updateRequest2);

        // Test 3: NOT_NULL on non-existing attribute (should fail)
        Map<String, ExpectedAttributeValue> expected3 = new HashMap<>();
        expected3.put("AnotherNonExistentField",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NOT_NULL)
                        .build());

        Map<String, AttributeValueUpdate> attributeUpdates3 = new HashMap<>();
        attributeUpdates3.put("ShouldNotUpdate1",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("ShouldNotBeSet").build()).build());

        UpdateItemRequest updateRequest3 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates3).expected(expected3).build();

        // Both DynamoDB and Phoenix should throw ConditionalCheckFailedException
        try {
            dynamoDbClient.updateItem(updateRequest3);
            Assert.fail(
                    "Expected ConditionalCheckFailedException for DynamoDB (NOT_NULL on non-existing)");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.updateItem(updateRequest3);
            Assert.fail(
                    "Expected ConditionalCheckFailedException for Phoenix (NOT_NULL on non-existing)");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        // Test 4: NULL on existing attribute (should fail)
        Map<String, ExpectedAttributeValue> expected4 = new HashMap<>();
        expected4.put("COL1",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NULL)
                        .build());

        Map<String, AttributeValueUpdate> attributeUpdates4 = new HashMap<>();
        attributeUpdates4.put("ShouldNotUpdate2",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("ShouldNotBeSet").build()).build());

        UpdateItemRequest updateRequest4 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates4).expected(expected4).build();

        // Both DynamoDB and Phoenix should throw ConditionalCheckFailedException
        try {
            dynamoDbClient.updateItem(updateRequest4);
            Assert.fail("Expected ConditionalCheckFailedException for DynamoDB (NULL on existing)");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.updateItem(updateRequest4);
            Assert.fail("Expected ConditionalCheckFailedException for Phoenix (NULL on existing)");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        // Test 5: Complex condition with NOT_NULL and NULL using OR operator
        Map<String, ExpectedAttributeValue> expected5 = new HashMap<>();
        expected5.put("COL2",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NOT_NULL)
                        .build());
        expected5.put("NonExistentField2",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NULL)
                        .build());

        Map<String, AttributeValueUpdate> attributeUpdates5 = new HashMap<>();
        attributeUpdates5.put("TestField3",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("ComplexTest").build()).build());

        UpdateItemRequest updateRequest5 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates5).expected(expected5)
                .conditionalOperator(ConditionalOperator.OR).build();

        // Both DynamoDB and Phoenix should succeed (COL2 exists OR NonExistentField2 doesn't exist)
        dynamoDbClient.updateItem(updateRequest5);
        phoenixDBClientV2.updateItem(updateRequest5);

        // Validate final state
        validateItem(tableName, key);
    }

    /**
     * Test legacy Expected parameter with comparison operators using AttributeValueList.
     * Tests EQ, LT, GE, etc. with AttributeValueList instead of single Value field.
     */
    @Test(timeout = 120000)
    public void testExpectedParameterWithAttributeValueList() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();

        // Test 1: EQ with AttributeValueList (should succeed)
        Map<String, ExpectedAttributeValue> expected1 = new HashMap<>();
        expected1.put("COL1",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.builder().n("1").build()).build());

        Map<String, AttributeValueUpdate> attributeUpdates1 = new HashMap<>();
        attributeUpdates1.put("TestField1",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("EQTest").build()).build());

        UpdateItemRequest updateRequest1 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates1).expected(expected1).build();

        // Both DynamoDB and Phoenix should succeed (COL1 = 1)
        dynamoDbClient.updateItem(updateRequest1);
        phoenixDBClientV2.updateItem(updateRequest1);

        // Test 2: LT with AttributeValueList (should succeed)
        Map<String, ExpectedAttributeValue> expected2 = new HashMap<>();
        expected2.put("COL4",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.LT)
                        .attributeValueList(AttributeValue.builder().n("50").build()).build());

        Map<String, AttributeValueUpdate> attributeUpdates2 = new HashMap<>();
        attributeUpdates2.put("TestField2",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("LTTest").build()).build());

        UpdateItemRequest updateRequest2 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates2).expected(expected2).build();

        // Both DynamoDB and Phoenix should succeed (COL4 = 42 < 50)
        dynamoDbClient.updateItem(updateRequest2);
        phoenixDBClientV2.updateItem(updateRequest2);

        // Test 3: GE with AttributeValueList (should succeed)
        Map<String, ExpectedAttributeValue> expected3 = new HashMap<>();
        expected3.put("COL4",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.GE)
                        .attributeValueList(AttributeValue.builder().n("40").build()).build());

        Map<String, AttributeValueUpdate> attributeUpdates3 = new HashMap<>();
        attributeUpdates3.put("TestField3",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("GETest").build()).build());

        UpdateItemRequest updateRequest3 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates3).expected(expected3).build();

        try {
            dynamoDbClient.updateItem(updateRequest3);
            Assert.fail("Should have thrown an exception");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        try {
            phoenixDBClientV2.updateItem(updateRequest3);
            Assert.fail("Should have thrown an exception");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        // Test 4: NE with AttributeValueList (should succeed)
        Map<String, ExpectedAttributeValue> expected4 = new HashMap<>();
        expected4.put("COL1",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.NE)
                        .attributeValueList(AttributeValue.builder().n("999").build()).build());

        Map<String, AttributeValueUpdate> attributeUpdates4 = new HashMap<>();
        attributeUpdates4.put("TestField4",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("NETest").build()).build());

        UpdateItemRequest updateRequest4 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates4).expected(expected4).build();

        // Both DynamoDB and Phoenix should succeed (COL1 = 1 != 999)
        dynamoDbClient.updateItem(updateRequest4);
        phoenixDBClientV2.updateItem(updateRequest4);

        // Test 5: BEGINS_WITH with AttributeValueList (should succeed)
        Map<String, ExpectedAttributeValue> expected5 = new HashMap<>();
        expected5.put("COL2",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.BEGINS_WITH)
                        .attributeValueList(AttributeValue.builder().s("1").build()).build());

        Map<String, AttributeValueUpdate> attributeUpdates5 = new HashMap<>();
        attributeUpdates5.put("TestField5",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("BeginsWithTest").build()).build());

        UpdateItemRequest updateRequest5 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates5).expected(expected5).build();

        try {
            dynamoDbClient.updateItem(updateRequest5);
            Assert.fail("Should have thrown an exception");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        try {
            phoenixDBClientV2.updateItem(updateRequest5);
            Assert.fail("Should have thrown an exception");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        // Test 6: CONTAINS with AttributeValueList (should succeed)
        Map<String, ExpectedAttributeValue> expected6 = new HashMap<>();
        expected6.put("COL2",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.CONTAINS)
                        .attributeValueList(AttributeValue.builder().s("1").build()).build());

        Map<String, AttributeValueUpdate> attributeUpdates6 = new HashMap<>();
        attributeUpdates6.put("TestField6",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("ContainsTest").build()).build());

        UpdateItemRequest updateRequest6 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates6).expected(expected6).build();

        // Both DynamoDB and Phoenix should succeed (COL2 = "1" contains "1")
        dynamoDbClient.updateItem(updateRequest6);
        phoenixDBClientV2.updateItem(updateRequest6);

        // Test 7: Failure case - EQ with AttributeValueList (should fail)
        Map<String, ExpectedAttributeValue> expected7 = new HashMap<>();
        expected7.put("COL1",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.EQ)
                        .attributeValueList(AttributeValue.builder().n("999").build()).build());

        Map<String, AttributeValueUpdate> attributeUpdates7 = new HashMap<>();
        attributeUpdates7.put("ShouldNotUpdate",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("ShouldNotBeSet").build()).build());

        UpdateItemRequest updateRequest7 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates7).expected(expected7).build();

        // Both DynamoDB and Phoenix should throw ConditionalCheckFailedException
        try {
            dynamoDbClient.updateItem(updateRequest7);
            Assert.fail(
                    "Expected ConditionalCheckFailedException for DynamoDB (EQ with wrong value)");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.updateItem(updateRequest7);
            Assert.fail(
                    "Expected ConditionalCheckFailedException for Phoenix (EQ with wrong value)");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        // Test 8: Complex condition with multiple AttributeValueList operators using AND
        Map<String, ExpectedAttributeValue> expected8 = new HashMap<>();
        expected8.put("COL1",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.GE)
                        .attributeValueList(AttributeValue.builder().n("1").build()).build());
        expected8.put("COL4",
                ExpectedAttributeValue.builder().comparisonOperator(ComparisonOperator.LE)
                        .attributeValueList(AttributeValue.builder().n("50").build()).build());

        Map<String, AttributeValueUpdate> attributeUpdates8 = new HashMap<>();
        attributeUpdates8.put("TestField8",
                AttributeValueUpdate.builder().action(AttributeAction.PUT)
                        .value(AttributeValue.builder().s("ComplexTest").build()).build());

        UpdateItemRequest updateRequest8 = UpdateItemRequest.builder().tableName(tableName).key(key)
                .attributeUpdates(attributeUpdates8).expected(expected8)
                .conditionalOperator(ConditionalOperator.AND).build();

        // Both DynamoDB and Phoenix should succeed (COL1 >= 1 AND COL4 <= 50)
        dynamoDbClient.updateItem(updateRequest8);
        phoenixDBClientV2.updateItem(updateRequest8);

        // Validate final state
        validateItem(tableName, key);
    }

    protected Map<String, AttributeValue> getKey() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK1", AttributeValue.builder().s("A").build());
        if (isSortKeyPresent)
            key.put("PK2", AttributeValue.builder().n("1").build());
        return key;
    }

    @Test(timeout = 120000)
    public void testSetAddRemoveWithOverlappingPlaceholders1() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
            "SET #my_col = :1, #my_col1 = :2, #my_col10 = :3, #c = #c + :4, #c_1 = :5 " +
            "ADD #top_set :6 " +
            "REMOVE #rm_attr, #rm_attr1");

        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#my_col", "COL2");
        exprAttrNames.put("#my_col1", "COL3");
        exprAttrNames.put("#my_col10", "NewField10");
        exprAttrNames.put("#c", "COL1");
        exprAttrNames.put("#c_1", "COL4");
        exprAttrNames.put("#top_set", "TopLevelSet");
        exprAttrNames.put("#rm_attr", "Counter");
        exprAttrNames.put("#rm_attr1", "Reviews");
        uir.expressionAttributeNames(exprAttrNames);

        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":1", AttributeValue.builder().s("UpdatedTitle").build());
        exprAttrVal.put(":2", AttributeValue.builder().s("UpdatedDesc").build());
        exprAttrVal.put(":3", AttributeValue.builder().n("999").build());
        exprAttrVal.put(":4", AttributeValue.builder().n("5").build());
        exprAttrVal.put(":5", AttributeValue.builder().n("100").build());
        exprAttrVal.put(":6", AttributeValue.builder().ss("newMember").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);

        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        ItemComparator.areItemsEqual(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testSetAddRemoveWithOverlappingPlaceholders2() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression(
            "SET #1 = #1 + :1, #11 = if_not_exists(#11, :11), #111 = :111 " +
            "ADD #3 :3 " +
            "REMOVE #4, #5");

        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        exprAttrNames.put("#11", "NewCol11");
        exprAttrNames.put("#111", "NewCol111");
        exprAttrNames.put("#3", "TopLevelSet");
        exprAttrNames.put("#4", "COL3");
        exprAttrNames.put("#5", "Reviews");
        uir.expressionAttributeNames(exprAttrNames);

        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":1", AttributeValue.builder().n("10").build());
        exprAttrVal.put(":11", AttributeValue.builder().s("default11").build());
        exprAttrVal.put(":111", AttributeValue.builder().s("value111").build());
        exprAttrVal.put(":3", AttributeValue.builder().ss("addedMember").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);

        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        ItemComparator.areItemsEqual(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }
}
