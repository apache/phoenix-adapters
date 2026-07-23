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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests for UpdateExpression validation behaviors.
 */
public class UpdateExpressionValidationIT {
    private static final Logger LOGGER =
            LoggerFactory.getLogger(UpdateExpressionValidationIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    private static final String tableName = "Validation_Test_Table";

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();

        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
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

    @Before
    public void setUp() {
        CreateTableRequest createTableRequest = CreateTableRequest.builder().tableName(tableName)
                .keySchema(KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH)
                        .build()).attributeDefinitions(
                        AttributeDefinition.builder().attributeName("pk")
                                .attributeType(ScalarAttributeType.S).build())
                .provisionedThroughput(
                        ProvisionedThroughput.builder().readCapacityUnits(5L).writeCapacityUnits(5L)
                                .build()).build();

        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);
    }

    @After
    public void tearDown() {
        dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
        phoenixDBClientV2.deleteTable(DeleteTableRequest.builder().tableName(tableName).build());
    }

    // REMOVE Operation Failure Tests

    @Test(timeout = 120000)
    public void testRemoveParentMissing() {
        // Create item without 'a' attribute
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        testUpdateExpressionFailure(tableName, "REMOVE a.b", null, null,
                "UNSET with parent missing");
    }

    @Test(timeout = 120000)
    public void testRemoveParentNotMap() {
        // Create item with 'a' as a number, not a map
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().n("123").build());
        putTestItem(tableName, item);

        testUpdateExpressionFailure(tableName, "REMOVE a.b", null, null,
                "UNSET with parent not a map");
    }

    @Test(timeout = 120000)
    public void testRemoveArrayMissing() {
        // Create item without 'a' attribute
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        testUpdateExpressionFailure(tableName, "REMOVE a[0]", null, null,
                "UNSET with array missing");
    }

    @Test(timeout = 120000)
    public void testRemoveParentNotList() {
        // Create item with 'a' as a number, not a list
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().n("123").build());
        putTestItem(tableName, item);

        testUpdateExpressionFailure(tableName, "REMOVE a[0]", null, null,
                "UNSET with parent not a list");
    }

    // REMOVE Operation Success Tests

    @Test(timeout = 120000)
    public void testRemoveArrayIndexOutOfRange() {
        // Create item with 'a' as a list with 2 elements
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder()
                .l(AttributeValue.builder().n("1").build(), AttributeValue.builder().n("2").build())
                .build());
        putTestItem(tableName, item);

        testUpdateExpressionSuccess(tableName, "REMOVE a[5]", null, null,
                "UNSET with array index out of range");
    }

    @Test(timeout = 120000)
    public void testRemoveFieldMissingNoOp() {
        // Create item without 'a' attribute - should be no-op
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        testUpdateExpressionSuccess(tableName, "REMOVE a", null, null,
                "REMOVE field missing (no-op)");
    }

    @Test(timeout = 120000)
    public void testRemoveFieldPresent() {
        // Create item with 'a' attribute
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().s("value").build());
        putTestItem(tableName, item);

        testUpdateExpressionSuccess(tableName, "REMOVE a", null, null, "REMOVE field present");
    }

    @Test(timeout = 120000)
    public void testRemoveNestedFieldMissingNoOp() {
        // Create item with 'a' as map but 'b' missing - should be no-op
        Map<String, AttributeValue> item = getKey();
        Map<String, AttributeValue> mapValue = new HashMap<>();
        mapValue.put("c", AttributeValue.builder().s("value").build());
        item.put("a", AttributeValue.builder().m(mapValue).build());
        putTestItem(tableName, item);

        testUpdateExpressionSuccess(tableName, "REMOVE a.b", null, null,
                "REMOVE nested field missing (no-op)");
    }

    @Test(timeout = 120000)
    public void testRemoveNestedFieldPresent() {
        // Create item with 'a' as map and 'b' present
        Map<String, AttributeValue> item = getKey();
        Map<String, AttributeValue> mapValue = new HashMap<>();
        mapValue.put("b", AttributeValue.builder().s("value").build());
        item.put("a", AttributeValue.builder().m(mapValue).build());
        putTestItem(tableName, item);

        testUpdateExpressionSuccess(tableName, "REMOVE a.b", null, null,
                "REMOVE nested field present");
    }

    @Test(timeout = 120000)
    public void testRemoveListElementValid() {
        // Create item with 'a' as list with valid index
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder()
                .l(AttributeValue.builder().n("1").build(), AttributeValue.builder().n("2").build(),
                        AttributeValue.builder().n("3").build()).build());
        putTestItem(tableName, item);

        testUpdateExpressionSuccess(tableName, "REMOVE a[1]", null, null,
                "REMOVE list element valid index");
    }

    // SET Operation Failure Tests

    @Test(timeout = 120000)
    public void testSetParentNotMap() {
        // Create item with 'a' as a number, not a map
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().n("123").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("value").build());

        testUpdateExpressionFailure(tableName, "SET a.b = :val", null, expressionAttributeValues,
                "SET with parent not a map");
    }

    @Test(timeout = 120000)
    public void testSetArrayMissing() {
        // Create item without 'a' attribute
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("value").build());

        testUpdateExpressionFailure(tableName, "SET a[0] = :val", null, expressionAttributeValues,
                "SET with array missing");
    }

    @Test(timeout = 120000)
    public void testSetParentNotList() {
        // Create item with 'a' as a number, not a list
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().n("123").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("value").build());

        testUpdateExpressionFailure(tableName, "SET a[0] = :val", null, expressionAttributeValues,
                "SET with parent not a list");
    }

    @Test(timeout = 120000)
    public void testSetNestedFieldAutoCreateMap() {
        // Create item without 'a' attribute
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("value").build());

        testUpdateExpressionFailure(tableName, "SET a.b = :val", null, expressionAttributeValues,
                "SET nested field parent missing");
    }

    // SET Operation Success Tests

    @Test(timeout = 120000)
    public void testSetArrayIndexBeyondSize() {
        // Create item with 'a' as a list with 2 elements
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder()
                .l(AttributeValue.builder().n("1").build(), AttributeValue.builder().n("2").build())
                .build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("value").build());

        testUpdateExpressionSuccess(tableName, "SET a[5] = :val", null, expressionAttributeValues,
                "SET with index beyond array size");
    }

    @Test(timeout = 120000)
    public void testSetFieldMissing() {
        // Create item without 'a' attribute - should create it
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("value").build());

        testUpdateExpressionSuccess(tableName, "SET a = :val", null, expressionAttributeValues,
                "SET field missing (creates)");
    }

    @Test(timeout = 120000)
    public void testSetNestedFieldInExistingMap() {
        // Create item with 'a' as existing map
        Map<String, AttributeValue> item = getKey();
        Map<String, AttributeValue> mapValue = new HashMap<>();
        mapValue.put("c", AttributeValue.builder().s("existing").build());
        item.put("a", AttributeValue.builder().m(mapValue).build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("value").build());

        testUpdateExpressionSuccess(tableName, "SET a.b = :val", null, expressionAttributeValues,
                "SET nested field in existing map");
    }

    @Test(timeout = 120000)
    public void testSetListElementValidIndex() {
        // Create item with 'a' as list with valid index
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder()
                .l(AttributeValue.builder().n("1").build(), AttributeValue.builder().n("2").build(),
                        AttributeValue.builder().n("3").build()).build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("newvalue").build());

        testUpdateExpressionSuccess(tableName, "SET a[1] = :val", null, expressionAttributeValues,
                "SET list element valid index");
    }

    // ADD Operation Success Tests

    @Test(timeout = 120000)
    public void testAddNotNumberOrSet() {
        // Create item with 'a' as a string, not a number or set
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().s("string").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().n("10").build());

        testUpdateExpressionFailure(tableName, "ADD a :val", null, expressionAttributeValues,
                "ADD with target not number or set");
    }

    @Test(timeout = 120000)
    public void testAddSetDifferentType1() {
        // Create item with 'a' as a string set
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().ss("string1", "string2").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().ns("123", "456").build());

        testUpdateExpressionFailure(tableName, "ADD a :val", null, expressionAttributeValues,
                "ADD with set of different type");
    }

    @Test(timeout = 120000)
    public void testAddSetDifferentType2() {
        // Create item with 'a' as a number set
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().ns("1", "2").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val",
                AttributeValue.builder().ss("string1", "string2").build());

        testUpdateExpressionFailure(tableName, "ADD a :val", null, expressionAttributeValues,
                "ADD with set of different type");
    }

    // ADD Operation Success Tests

    @Test(timeout = 120000)
    public void testAddNumberToMissingField() {
        // Create item without 'a' attribute - should create with operand value
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().n("10").build());

        testUpdateExpressionSuccess(tableName, "ADD a :val", null, expressionAttributeValues,
                "ADD number to missing field");
    }

    @Test(timeout = 120000)
    public void testAddNumberToExistingNumber() {
        // Create item with 'a' as number
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().n("5").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().n("10").build());

        testUpdateExpressionSuccess(tableName, "ADD a :val", null, expressionAttributeValues,
                "ADD number to existing number");
    }

    @Test(timeout = 120000)
    public void testAddSetToMissingField() {
        // Create item without 'a' attribute - should create set with operand
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val",
                AttributeValue.builder().ss("value1", "value2").build());

        testUpdateExpressionSuccess(tableName, "ADD a :val", null, expressionAttributeValues,
                "ADD set to missing field");
    }

    @Test(timeout = 120000)
    public void testAddToExistingSetSameType() {
        // Create item with 'a' as string set
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().ss("existing1", "existing2").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().ss("new1", "new2").build());

        testUpdateExpressionSuccess(tableName, "ADD a :val", null, expressionAttributeValues,
                "ADD to existing set same type");
    }

    // DELETE Operation Failure Tests

    @Test(timeout = 120000)
    public void testDeleteNotSet() {

        // Create item with 'a' as a string, not a set
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().s("string").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().ss("value").build());

        testUpdateExpressionFailure(tableName, "DELETE a :val", null, expressionAttributeValues,
                "DELETE from set with target not a set");

    }

    @Test(timeout = 120000)
    public void testDeleteDifferentType() {

        // Create item with 'a' as a string set
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().ss("string1", "string2").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().ns("123", "456").build());

        testUpdateExpressionFailure(tableName, "DELETE a :val", null, expressionAttributeValues,
                "DELETE from set with different type");

    }

    // DELETE Operation Success Tests

    @Test(timeout = 120000)
    public void testDeleteFieldMissing() {
        // Create item without 'a' attribute
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().ss("value").build());

        testUpdateExpressionSuccess(tableName, "DELETE a :val", null, expressionAttributeValues,
                "DELETE from set with field missing");
    }

    @Test(timeout = 120000)
    public void testDeleteSameType() {
        // Create item with 'a' as string set
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().ss("value1", "value2", "value3").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val",
                AttributeValue.builder().ss("value1", "value3").build());

        testUpdateExpressionSuccess(tableName, "DELETE a :val", null, expressionAttributeValues,
                "DELETE from set same type");
    }

    // list_append Operation Tests

    /**
     * list_append against an existing list attribute path with a literal list operand.
     */
    @Test(timeout = 120000)
    public void testListAppendExistingList() {
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder()
                .l(AttributeValue.builder().s("x").build()).build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().l(
                AttributeValue.builder().s("y").build(),
                AttributeValue.builder().s("z").build()).build());

        testUpdateExpressionSuccess(tableName, "SET a = list_append(a, :val)", null,
                expressionAttributeValues, "list_append append literal to existing list");
    }

    /**
     * list_append with if_not_exists fallback when the target attribute is missing.
     */
    @Test(timeout = 120000)
    public void testListAppendIfNotExistsMissing() {
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":empty",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        expressionAttributeValues.put(":val", AttributeValue.builder().l(
                AttributeValue.builder().s("y").build()).build());

        testUpdateExpressionSuccess(tableName,
                "SET a = list_append(if_not_exists(a, :empty), :val)", null,
                expressionAttributeValues, "list_append create-or-append on missing attribute");
    }

    /**
     * if_not_exists as the SECOND operand (literal list first). This is the prepend
     * variant of the canonical create-or-append pattern.
     */
    @Test(timeout = 120000)
    public void testListAppendLiteralAndIfNotExists() {
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":empty",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        expressionAttributeValues.put(":val", AttributeValue.builder().l(
                AttributeValue.builder().s("y").build()).build());

        testUpdateExpressionSuccess(tableName,
                "SET a = list_append(:val, if_not_exists(a, :empty))", null,
                expressionAttributeValues, "list_append with literal first and if_not_exists second");
    }

    /**
     * Both operands of list_append are literal value placeholders (no paths, no
     * if_not_exists). Result should be the simple concatenation of the two literals.
     */
    @Test(timeout = 120000)
    public void testListAppendBothLiterals() {
        Map<String, AttributeValue> item = getKey();
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":v1", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("b").build()).build());
        expressionAttributeValues.put(":v2", AttributeValue.builder().l(
                AttributeValue.builder().s("c").build(),
                AttributeValue.builder().s("d").build()).build());

        testUpdateExpressionSuccess(tableName,
                "SET combined = list_append(:v1, :v2)", null,
                expressionAttributeValues, "list_append with two literal-list operands");
    }

    /**
     * Both operands of list_append are if_not_exists.
     */
    @Test(timeout = 120000)
    public void testListAppendBothOperandsAreIfNotExists() {
        // Seed item: 'leftList' exists, 'rightList' does not
        Map<String, AttributeValue> item = getKey();
        item.put("leftList", AttributeValue.builder()
                .l(AttributeValue.builder().s("x").build()).build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":emptyLeft",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());
        expressionAttributeValues.put(":emptyRight",
                AttributeValue.builder().l(java.util.Collections.emptyList()).build());

        testUpdateExpressionSuccess(tableName,
                "SET merged = list_append("
                        + "if_not_exists(leftList, :emptyLeft), "
                        + "if_not_exists(rightList, :emptyRight))",
                null, expressionAttributeValues,
                "list_append with if_not_exists in both operand positions");
    }

    /**
     * Nested list_append (list_append inside list_append) is not part of AWS DynamoDB's
     * documented UpdateExpression grammar -- the only function permitted as an operand is
     * if_not_exists. Both real DDB and the phoenix-adapter must reject it with HTTP 400.
     */
    @Test(timeout = 120000)
    public void testListAppendNestedRejected() {
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder()
                .l(AttributeValue.builder().s("x").build()).build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":v1", AttributeValue.builder().l(
                AttributeValue.builder().s("y").build()).build());
        expressionAttributeValues.put(":v2", AttributeValue.builder().l(
                AttributeValue.builder().s("z").build()).build());

        testUpdateExpressionFailure(tableName,
                "SET a = list_append(list_append(a, :v1), :v2)", null,
                expressionAttributeValues, "nested list_append should be rejected");
    }

    /**
     * list_append where the target path resolves to a non-list value should fail.
     */
    @Test(timeout = 120000)
    public void testListAppendTargetNotList() {
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().n("123").build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().l(
                AttributeValue.builder().s("y").build()).build());

        testUpdateExpressionFailure(tableName, "SET a = list_append(a, :val)", null,
                expressionAttributeValues, "list_append on attribute that is not a list");
    }

    private void putTestItem(String tableName, Map<String, AttributeValue> item) {
        PutItemRequest putRequest =
                PutItemRequest.builder().tableName(tableName).item(item).build();

        dynamoDbClient.putItem(putRequest);
        phoenixDBClientV2.putItem(putRequest);
    }

    private Map<String, AttributeValue> getKey() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("pk", AttributeValue.builder().s("test-key").build());
        return key;
    }

    private UpdateItemRequest buildUpdateItemRequest(String tableName, String updateExpression,
            Map<String, String> expressionAttributeNames,
            Map<String, AttributeValue> expressionAttributeValues) {
        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest.Builder builder =
                UpdateItemRequest.builder().tableName(tableName).key(key)
                        .updateExpression(updateExpression);

        if (expressionAttributeNames != null) {
            builder.expressionAttributeNames(expressionAttributeNames);
        }
        if (expressionAttributeValues != null) {
            builder.expressionAttributeValues(expressionAttributeValues);
        }

        return builder.build();
    }

    private void testUpdateExpressionFailure(String tableName, String updateExpression,
            Map<String, String> expressionAttributeNames,
            Map<String, AttributeValue> expressionAttributeValues, String testDescription) {
        UpdateItemRequest updateRequest =
                buildUpdateItemRequest(tableName, updateExpression, expressionAttributeNames,
                        expressionAttributeValues);

        // Test DynamoDB
        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("Expected ValidationException for DynamoDB - " + testDescription);
        } catch (DynamoDbException e) {
            Assert.assertEquals("Expected 400 status code for DynamoDB - " + testDescription, 400,
                    e.statusCode());
        }

        // Test Phoenix
        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Expected ValidationException for Phoenix - " + testDescription);
        } catch (DynamoDbException e) {
            Assert.assertEquals("Expected 400 status code for Phoenix - " + testDescription, 400,
                    e.statusCode());
        }
    }

    private void testUpdateExpressionSuccess(String tableName, String updateExpression,
            Map<String, String> expressionAttributeNames,
            Map<String, AttributeValue> expressionAttributeValues, String testDescription) {
        UpdateItemRequest updateRequest =
                buildUpdateItemRequest(tableName, updateExpression, expressionAttributeNames,
                        expressionAttributeValues);

        // Test DynamoDB - should succeed
        dynamoDbClient.updateItem(updateRequest);

        // Test Phoenix - should succeed
        phoenixDBClientV2.updateItem(updateRequest);

        // Validate that both DynamoDB and Phoenix have the same final state
        validateItem(tableName, updateRequest.key());
    }

    private void validateItem(String tableName, Map<String, AttributeValue> key) {
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gir);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gir);
        Assert.assertTrue("Item should be identical in DynamoDB and Phoenix",
                ItemComparator.areItemsEqual(dynamoResult.item(), phoenixResult.item()));
    }

    // ---------------------------------------------------------------------------------
    // Grammar / structure validation: malformed UpdateExpression strings should be
    // rejected with HTTP 400 by both DynamoDB and the Phoenix adapter.
    // ---------------------------------------------------------------------------------

    @Test(timeout = 120000)
    public void testEmptySetClauseBody() {
        testUpdateExpressionFailure(tableName, "SET ", null, null, "empty SET body");
    }

    @Test(timeout = 120000)
    public void testEmptyRemoveClauseBody() {
        testUpdateExpressionFailure(tableName, "REMOVE ", null, null, "empty REMOVE body");
    }

    @Test(timeout = 120000)
    public void testEmptyAddClauseBody() {
        testUpdateExpressionFailure(tableName, "ADD ", null, null, "empty ADD body");
    }

    @Test(timeout = 120000)
    public void testEmptyDeleteClauseBody() {
        testUpdateExpressionFailure(tableName, "DELETE ", null, null, "empty DELETE body");
    }

    @Test(timeout = 120000)
    public void testDuplicatePathInSet() {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v1", AttributeValue.builder().s("x").build());
        v.put(":v2", AttributeValue.builder().s("y").build());
        testUpdateExpressionFailure(tableName, "SET a = :v1, a = :v2", null, v,
                "duplicate path in SET");
    }

    @Test(timeout = 120000)
    public void testPathOverlapInSet() {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v1", AttributeValue.builder().s("x").build());
        v.put(":v2", AttributeValue.builder().s("y").build());
        testUpdateExpressionFailure(tableName, "SET a.b = :v1, a = :v2", null, v,
                "overlapping paths in SET");
    }

    @Test(timeout = 120000)
    public void testPathOverlapInRemove() {
        testUpdateExpressionFailure(tableName, "REMOVE a, a.b", null, null,
                "overlapping paths in REMOVE");
    }

    @Test(timeout = 120000)
    public void testDuplicateClauseKeyword() {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v1", AttributeValue.builder().s("x").build());
        v.put(":v2", AttributeValue.builder().s("y").build());
        testUpdateExpressionFailure(tableName, "SET a = :v1 SET b = :v2", null, v,
                "duplicate SET keyword");
    }

    /**
     * SET keyword appears twice with another clause interleaved between the two SETs.
     * Different parse path than the back-to-back duplicate; both DDB and adapter still 400.
     */
    @Test(timeout = 120000)
    public void testDuplicateClauseKeywordInterleaved() {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v1", AttributeValue.builder().s("x").build());
        v.put(":v2", AttributeValue.builder().s("y").build());
        testUpdateExpressionFailure(tableName,
                "SET a = :v1 REMOVE c SET b = :v2", null, v,
                "duplicate SET keyword interleaved with REMOVE");
    }

    @Test(timeout = 120000)
    public void testIfNotExistsArityThree() {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v", AttributeValue.builder().s("x").build());
        v.put(":v2", AttributeValue.builder().s("y").build());
        testUpdateExpressionFailure(tableName, "SET a = if_not_exists(a, :v, :v2)", null, v,
                "if_not_exists arity 3");
    }

    @Test(timeout = 120000)
    public void testIfNotExistsArityOne() {
        testUpdateExpressionFailure(tableName, "SET a = if_not_exists(a)", null, null,
                "if_not_exists arity 1");
    }

    @Test(timeout = 120000)
    public void testIfNotExistsOutsideSet() {
        Map<String, AttributeValue> v = Collections.singletonMap(":v",
                AttributeValue.builder().n("1").build());
        testUpdateExpressionFailure(tableName, "ADD a if_not_exists(a, :v)", null, v,
                "if_not_exists in ADD");
    }

    @Test(timeout = 120000)
    public void testListAppendOutsideSet() {
        Map<String, AttributeValue> v = Collections.singletonMap(":v",
                AttributeValue.builder().l(AttributeValue.builder().s("x").build()).build());
        testUpdateExpressionFailure(tableName, "ADD a list_append(a, :v)", null, v,
                "list_append in ADD");
    }

    @Test(timeout = 120000)
    public void testUndeclaredExpressionAttributeName() {
        Map<String, AttributeValue> v = Collections.singletonMap(":v",
                AttributeValue.builder().s("x").build());
        testUpdateExpressionFailure(tableName, "SET #unknown = :v", null, v,
                "undeclared expression attribute name");
    }

    @Test(timeout = 120000)
    public void testUndeclaredExpressionAttributeValueInSet() {
        testUpdateExpressionFailure(tableName, "SET a = :missing", null, null,
                "undeclared expression attribute value in SET");
    }

    @Test(timeout = 120000)
    public void testUndeclaredExpressionAttributeValueInAdd() {
        testUpdateExpressionFailure(tableName, "ADD a :missing", null, null,
                "undeclared expression attribute value in ADD");
    }

    @Test(timeout = 120000)
    public void testUndeclaredExpressionAttributeValueInDelete() {
        testUpdateExpressionFailure(tableName, "DELETE a :missing", null, null,
                "undeclared expression attribute value in DELETE");
    }

    @Test(timeout = 120000)
    public void testRemoveAndSetOnSamePath() {
        Map<String, AttributeValue> v = Collections.singletonMap(":v",
                AttributeValue.builder().s("x").build());
        testUpdateExpressionFailure(tableName, "SET a = :v REMOVE a", null, v,
                "SET and REMOVE on same path");
    }

    /**
     * {@code list_append(...) + list_append(...)} — list values cannot be arithmetic operands.
     * Both DDB and the adapter return 400.
     */
    @Test(timeout = 120000)
    public void testListAppendAsArithmeticOperandRejected() {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v", AttributeValue.builder().l(
                AttributeValue.builder().s("x").build()).build());
        v.put(":w", AttributeValue.builder().l(
                AttributeValue.builder().s("y").build()).build());
        testUpdateExpressionFailure(tableName,
                "SET col = list_append(a, :v) + list_append(:w, b)", null, v,
                "list_append cannot be an arithmetic operand");
    }

    /**
     * Arithmetic where the {@code if_not_exists} fallback resolves to a non-numeric placeholder.
     * The visitor type-checks the placeholder at parse time so the 400 is anchored at the
     * adapter's parser layer; DDB rejects with the same status.
     */
    @Test(timeout = 120000)
    public void testArithmeticIfNotExistsNonNumericFallbackRejected() {
        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":s", AttributeValue.builder().s("notANumber").build());
        v.put(":one", AttributeValue.builder().n("1").build());
        testUpdateExpressionFailure(tableName,
                "SET counter = if_not_exists(c, :s) + :one", null, v,
                "arithmetic with non-numeric if_not_exists fallback");
    }

    /**
     * Probe DDB and the adapter for case-sensitivity of clause keywords and function names.
     * Asserts parity (both backends return the same status) for each variant. If DDB accepts
     * a case-mixed form that the adapter rejects (or vice versa), this test fails with a
     * clear message naming the offending input — that's how we discover whether our grammar
     * is in fact stricter or looser than DDB.
     */
    @Test(timeout = 120000)
    public void testCaseSensitivityParity() {
        // Re-seed the item before each iteration since some cases may mutate it.
        Map<String, AttributeValue> seed = getKey();
        seed.put("a", AttributeValue.builder().ss("x").build());
        seed.put("evt", AttributeValue.builder().l(
                AttributeValue.builder().s("e0").build()).build());

        AttributeValue numV = AttributeValue.builder().n("1").build();
        AttributeValue ssV = AttributeValue.builder().ss("y").build();
        AttributeValue listV = AttributeValue.builder().l(
                AttributeValue.builder().s("e1").build()).build();
        AttributeValue emptyL = AttributeValue.builder().l(java.util.Collections.emptyList()).build();

        // Each case: input expression -> the values map sized to exactly what it references.
        Map<String, Map<String, AttributeValue>> cases = new java.util.LinkedHashMap<>();
        cases.put("SET a = :v",            singleValue(":v", numV));
        cases.put("set a = :v",            singleValue(":v", numV));
        cases.put("Set a = :v",            singleValue(":v", numV));
        cases.put("REMOVE a",              null);
        cases.put("remove a",              null);
        cases.put("Remove a",              null);
        cases.put("ADD a :v",              singleValue(":v", ssV));
        cases.put("add a :v",              singleValue(":v", ssV));
        cases.put("Add a :v",              singleValue(":v", ssV));
        cases.put("DELETE a :v",           singleValue(":v", ssV));
        cases.put("delete a :v",           singleValue(":v", ssV));
        cases.put("Delete a :v",           singleValue(":v", ssV));
        cases.put("SET myCounter = if_not_exists(myCounter, :one)",
                singleValue(":one", numV));
        cases.put("SET myCounter = If_Not_Exists(myCounter, :one)",
                singleValue(":one", numV));
        cases.put("SET myCounter = IF_NOT_EXISTS(myCounter, :one)",
                singleValue(":one", numV));
        cases.put("SET evt = list_append(evt, :more)",
                singleValue(":more", listV));
        cases.put("SET evt = List_Append(evt, :more)",
                singleValue(":more", listV));
        cases.put("SET evt = LIST_APPEND(evt, :more)",
                singleValue(":more", listV));

        StringBuilder mismatches = new StringBuilder();
        for (Map.Entry<String, Map<String, AttributeValue>> e : cases.entrySet()) {
            String expr = e.getKey();
            // Reset the seed before every probe so prior mutations don't influence later cases.
            putTestItem(tableName, seed);

            UpdateItemRequest req = buildUpdateItemRequest(tableName, expr, null, e.getValue());
            int ddbStatus = invokeStatus(() -> dynamoDbClient.updateItem(req));
            int phxStatus = invokeStatus(() -> phoenixDBClientV2.updateItem(req));
            if (ddbStatus != phxStatus) {
                mismatches.append("\n  expr=`").append(expr)
                        .append("`  ddb=").append(ddbStatus).append("  phoenix=").append(phxStatus);
            }
        }
        Assert.assertEquals("Case-sensitivity parity mismatches:" + mismatches,
                0, mismatches.length());
    }

    private static Map<String, AttributeValue> singleValue(String name, AttributeValue v) {
        return Collections.singletonMap(name, v);
    }

    /**
     * One UpdateItem call combining all four clause keywords in mixed case. Confirms the
     * lexer handles every clause's case-insensitivity end-to-end and that DDB and the adapter
     * end up with byte-equal items.
     */
    @Test(timeout = 120000)
    public void testAllClausesMixedCaseRoundTrip() {
        Map<String, AttributeValue> seed = getKey();
        seed.put("myCounter", AttributeValue.builder().n("10").build());
        seed.put("rmField", AttributeValue.builder().s("removeMe").build());
        seed.put("addCnt", AttributeValue.builder().n("5").build());
        seed.put("delSet", AttributeValue.builder().ss("a", "b", "c").build());
        putTestItem(tableName, seed);

        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":inc", AttributeValue.builder().n("3").build());
        values.put(":addV", AttributeValue.builder().n("7").build());
        values.put(":delV", AttributeValue.builder().ss("a").build());
        testUpdateExpressionSuccess(tableName,
                "Set myCounter = myCounter + :inc remove rmField ADD addCnt :addV dElEtE delSet :delV",
                null, values, "all four clauses in mixed case");
    }

    /** Returns 200 if the action succeeded, otherwise the DynamoDbException status code. */
    private int invokeStatus(Runnable action) {
        try {
            action.run();
            return 200;
        } catch (DynamoDbException e) {
            return e.statusCode();
        }
    }

    /**
     * Bare attribute name cannot start with a digit per DDB's expression syntax. Aliased paths
     * starting with a digit (e.g. {@code #5star}) are still accepted — covered separately by
     * the nested-alias-path tests.
     */
    @Test(timeout = 120000)
    public void testBareAttributeNameStartingWithDigitRejected() {
        Map<String, AttributeValue> v = Collections.singletonMap(":v",
                AttributeValue.builder().s("x").build());
        testUpdateExpressionFailure(tableName, "SET 5col = :v", null, v,
                "bare attribute name starting with digit");
    }

    @Test(timeout = 120000)
    public void testEmptyUpdateExpression() {
        testUpdateExpressionFailure(tableName, "", null, null, "empty UpdateExpression");
    }

    @Test(timeout = 120000)
    public void testWhitespaceOnlyUpdateExpression() {
        testUpdateExpressionFailure(tableName, "   ", null, null,
                "whitespace-only UpdateExpression");
    }

    /**
     * Parent vs nested-field overlap with seed where {@code a} is a Map. Without parser-level
     * overlap detection this would have FAILED parity (DDB 400, Phoenix 200 with silent overwrite)
     * because the BSON descent succeeds when the parent is actually a map.
     */
    @Test(timeout = 120000)
    public void testPathOverlapInSetWhenParentIsMap() {
        Map<String, AttributeValue> nested = new HashMap<>();
        nested.put("b", AttributeValue.builder().s("inner").build());
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().m(nested).build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v1", AttributeValue.builder().s("x").build());
        v.put(":v2", AttributeValue.builder().s("y").build());
        testUpdateExpressionFailure(tableName, "SET a.b = :v1, a = :v2", null, v,
                "parent/nested-field overlap with map-typed parent");
    }

    /**
     * Parent vs array-index overlap with seed where {@code a} is a List.
     */
    @Test(timeout = 120000)
    public void testPathOverlapInSetWhenParentIsList() {
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().l(
                AttributeValue.builder().s("x").build()).build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v1", AttributeValue.builder().s("y").build());
        v.put(":v2", AttributeValue.builder().l(
                AttributeValue.builder().s("z").build()).build());
        testUpdateExpressionFailure(tableName, "SET a[0] = :v1, a = :v2", null, v,
                "parent/array-index overlap with list-typed parent");
    }

    /**
     * Same-clause overlap between two nested paths where one is a strict prefix of the other.
     */
    @Test(timeout = 120000)
    public void testPathOverlapNestedPrefixInSet() {
        Map<String, AttributeValue> inner = new HashMap<>();
        inner.put("c", AttributeValue.builder().s("z").build());
        Map<String, AttributeValue> outer = new HashMap<>();
        outer.put("b", AttributeValue.builder().m(inner).build());
        Map<String, AttributeValue> item = getKey();
        item.put("a", AttributeValue.builder().m(outer).build());
        putTestItem(tableName, item);

        Map<String, AttributeValue> v = new HashMap<>();
        v.put(":v1", AttributeValue.builder().s("x").build());
        v.put(":v2", AttributeValue.builder().s("y").build());
        testUpdateExpressionFailure(tableName, "SET a.b = :v1, a.b.c = :v2", null, v,
                "two paths in same SET clause where one is a strict prefix of the other");
    }

    /**
     * Mixed alias + bare-name nested path on the SET LHS: {@code SET #a.bareField = :v} where
     * {@code #a} resolves to a top-level Map.
     */
    @Test(timeout = 120000)
    public void testAliasThenBareNamePathRoundTrip() {
        Map<String, AttributeValue> nested = new HashMap<>();
        nested.put("bareField", AttributeValue.builder().s("orig").build());
        Map<String, AttributeValue> item = getKey();
        item.put("topMap", AttributeValue.builder().m(nested).build());
        putTestItem(tableName, item);

        Map<String, String> aliases = Collections.singletonMap("#a", "topMap");
        Map<String, AttributeValue> values = Collections.singletonMap(":v",
                AttributeValue.builder().s("updated").build());
        testUpdateExpressionSuccess(tableName, "SET #a.bareField = :v", aliases, values,
                "alias.bareName SET");
    }

    /**
     * {@code SET bareName.#a = :v} — bare top-level name then alias step.
     */
    @Test(timeout = 120000)
    public void testBareNameThenAliasPathRoundTrip() {
        Map<String, AttributeValue> nested = new HashMap<>();
        nested.put("deepField", AttributeValue.builder().s("orig").build());
        Map<String, AttributeValue> item = getKey();
        item.put("topMap", AttributeValue.builder().m(nested).build());
        putTestItem(tableName, item);

        Map<String, String> aliases = Collections.singletonMap("#a", "deepField");
        Map<String, AttributeValue> values = Collections.singletonMap(":v",
                AttributeValue.builder().s("updated").build());
        testUpdateExpressionSuccess(tableName, "SET topMap.#a = :v", aliases, values,
                "bareName.alias SET");
    }

    /**
     * {@code SET #l[0].#nested = :v} — alias with index then alias.
     */
    @Test(timeout = 120000)
    public void testAliasArrayIndexThenAliasPathRoundTrip() {
        Map<String, AttributeValue> inner = new HashMap<>();
        inner.put("deepField", AttributeValue.builder().s("orig").build());
        Map<String, AttributeValue> item = getKey();
        item.put("myList", AttributeValue.builder().l(
                AttributeValue.builder().m(inner).build()).build());
        putTestItem(tableName, item);

        Map<String, String> aliases = new HashMap<>();
        aliases.put("#l", "myList");
        aliases.put("#nested", "deepField");
        Map<String, AttributeValue> values = Collections.singletonMap(":v",
                AttributeValue.builder().s("updated").build());
        testUpdateExpressionSuccess(tableName, "SET #l[0].#nested = :v", aliases, values,
                "alias[idx].alias SET");
    }

    /**
     * {@code SET col = if_not_exists(#a.b, :v)} where {@code #a.b} does NOT exist; fallback wins.
     */
    @Test(timeout = 120000)
    public void testAliasPathInsideIfNotExistsRoundTrip() {
        putTestItem(tableName, getKey());

        Map<String, String> aliases = Collections.singletonMap("#a", "topMap");
        Map<String, AttributeValue> values = Collections.singletonMap(":v",
                AttributeValue.builder().s("fallback").build());
        testUpdateExpressionSuccess(tableName,
                "SET col = if_not_exists(#a.b, :v)", aliases, values,
                "alias path inside if_not_exists");
    }

    /**
     * {@code SET col = list_append(if_not_exists(#l, :empty), :items)}.
     */
    @Test(timeout = 120000)
    public void testAliasInsideIfNotExistsInsideListAppendRoundTrip() {
        putTestItem(tableName, getKey());

        Map<String, String> aliases = Collections.singletonMap("#l", "events");
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":empty", AttributeValue.builder().l(Collections.emptyList()).build());
        values.put(":items", AttributeValue.builder().l(
                AttributeValue.builder().s("a").build(),
                AttributeValue.builder().s("b").build()).build());
        testUpdateExpressionSuccess(tableName,
                "SET col = list_append(if_not_exists(#l, :empty), :items)", aliases, values,
                "alias inside if_not_exists inside list_append");
    }

    /**
     * Multi-step alias-heavy nested path on LHS, mirroring the AWS docs example shape.
     */
    @Test(timeout = 120000)
    public void testNestedAliasPathRoundTrip() {
        Map<String, AttributeValue> reviewerEntry = new HashMap<>();
        reviewerEntry.put("reviewer", AttributeValue.builder().s("alice").build());
        Map<String, AttributeValue> reviews = new HashMap<>();
        reviews.put("FiveStar", AttributeValue.builder().l(
                AttributeValue.builder().m(reviewerEntry).build(),
                AttributeValue.builder().m(reviewerEntry).build()).build());
        Map<String, AttributeValue> item = getKey();
        item.put("ProductReviews", AttributeValue.builder().m(reviews).build());
        putTestItem(tableName, item);

        Map<String, String> aliases = new HashMap<>();
        aliases.put("#pr", "ProductReviews");
        aliases.put("#5star", "FiveStar");
        Map<String, AttributeValue> values = Collections.singletonMap(":v",
                AttributeValue.builder().s("bob").build());
        testUpdateExpressionSuccess(tableName,
                "SET #pr.#5star[1].reviewer = :v", aliases, values,
                "alias.alias[idx].bareName SET");
    }

    /**
     * Alias paths used in REMOVE / ADD / DELETE in a single expression. Pins parity for the
     * non-SET clauses since the rest of the alias-path tests target SET LHS only.
     */
    @Test(timeout = 120000)
    public void testAliasPathsInRemoveAddDeleteRoundTrip() {
        Map<String, AttributeValue> nested = new HashMap<>();
        nested.put("leafField", AttributeValue.builder().s("orig").build());
        Map<String, AttributeValue> item = getKey();
        item.put("topMap", AttributeValue.builder().m(nested).build());
        item.put("counter", AttributeValue.builder().n("5").build());
        item.put("myStringSet", AttributeValue.builder().ss("a", "b").build());
        putTestItem(tableName, item);

        Map<String, String> aliases = new HashMap<>();
        aliases.put("#a", "topMap");
        aliases.put("#leaf", "leafField");
        aliases.put("#c", "counter");
        aliases.put("#s", "myStringSet");
        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":n", AttributeValue.builder().n("3").build());
        values.put(":subset", AttributeValue.builder().ss("a").build());
        testUpdateExpressionSuccess(tableName,
                "REMOVE #a.#leaf ADD #c :n DELETE #s :subset", aliases, values,
                "alias paths in REMOVE/ADD/DELETE");
    }

    /**
     * Alias-as-keyword parity: an ExpressionAttributeName whose value is the literal string
     * {@code "ADD"} (a clause keyword) must produce byte-equal items between DDB and the
     * adapter. Pre-ANTLR the regex parser misclassified the substituted token and corrupted
     * the resulting BSON.
     */
    @Test(timeout = 120000)
    public void testAliasResolvingToClauseKeywordRoundTrip() {
        Map<String, String> aliases = new HashMap<>();
        aliases.put("#attr", "ADD");

        Map<String, AttributeValue> values = new HashMap<>();
        values.put(":v", AttributeValue.builder().s("hello").build());

        UpdateItemRequest req = UpdateItemRequest.builder().tableName(tableName).key(getKey())
                .updateExpression("SET #attr = :v REMOVE oldField")
                .expressionAttributeNames(aliases)
                .expressionAttributeValues(values).build();
        dynamoDbClient.updateItem(req);
        phoenixDBClientV2.updateItem(req);
        validateItem(tableName, getKey());
    }

    // ---------------------------------------------------------------------------------
    // Clause ordering: every non-empty subset of {SET,REMOVE,ADD,DELETE} in every
    // ordering, asserting DDB and Phoenix end up with byte-equal items. Looped in a
    // single test method to avoid the per-test table create/drop and another mini-cluster.
    // ---------------------------------------------------------------------------------

    @Test(timeout = 600000)
    public void testAllClauseOrderingsProduceIdenticalItem() {
        String[] all = {"SET", "REMOVE", "ADD", "DELETE"};
        for (int mask = 1; mask < (1 << all.length); mask++) {
            List<String> chosen = new ArrayList<>();
            for (int i = 0; i < all.length; i++) {
                if ((mask & (1 << i)) != 0) {
                    chosen.add(all[i]);
                }
            }
            for (List<String> ordering : permutations(chosen)) {
                runClauseOrdering(ordering);
            }
        }
    }

    private void runClauseOrdering(List<String> clauseOrder) {
        // PutItem fully replaces, so this resets any attributes a prior iteration mutated.
        Map<String, AttributeValue> seed = getKey();
        seed.put("cnt", AttributeValue.builder().n("10").build());
        seed.put("rmField", AttributeValue.builder().s("removeMe").build());
        seed.put("addCnt", AttributeValue.builder().n("5").build());
        seed.put("delSet", AttributeValue.builder().ss("a", "b", "c").build());
        putTestItem(tableName, seed);

        StringBuilder expr = new StringBuilder();
        for (String clause : clauseOrder) {
            if (expr.length() > 0) {
                expr.append(' ');
            }
            expr.append(clause).append(' ').append(clauseBody(clause));
        }

        Map<String, AttributeValue> values = new HashMap<>();
        if (clauseOrder.contains("SET")) {
            values.put(":inc", AttributeValue.builder().n("3").build());
        }
        if (clauseOrder.contains("ADD")) {
            values.put(":addV", AttributeValue.builder().n("7").build());
        }
        if (clauseOrder.contains("DELETE")) {
            values.put(":delV", AttributeValue.builder().ss("a").build());
        }

        UpdateItemRequest.Builder rb = UpdateItemRequest.builder().tableName(tableName).key(getKey())
                .updateExpression(expr.toString());
        if (!values.isEmpty()) {
            rb.expressionAttributeValues(values);
        }
        UpdateItemRequest req = rb.build();
        dynamoDbClient.updateItem(req);
        phoenixDBClientV2.updateItem(req);

        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(getKey()).build();
        GetItemResponse ddb = dynamoDbClient.getItem(gir);
        GetItemResponse phx = phoenixDBClientV2.getItem(gir);
        Assert.assertTrue("Items should match for ordering " + clauseOrder + " expr=" + expr,
                ItemComparator.areItemsEqual(ddb.item(), phx.item()));
    }

    private static String clauseBody(String clause) {
        switch (clause) {
            case "SET":    return "cnt = cnt + :inc";
            case "REMOVE": return "rmField";
            case "ADD":    return "addCnt :addV";
            case "DELETE": return "delSet :delV";
            default: throw new IllegalArgumentException(clause);
        }
    }

    private static List<List<String>> permutations(List<String> in) {
        List<List<String>> out = new ArrayList<>();
        if (in.size() == 1) {
            out.add(new ArrayList<>(in));
            return out;
        }
        for (int i = 0; i < in.size(); i++) {
            List<String> rest = new ArrayList<>(in);
            String head = rest.remove(i);
            for (List<String> tail : permutations(rest)) {
                List<String> p = new ArrayList<>();
                p.add(head);
                p.addAll(tail);
                out.add(p);
            }
        }
        return out;
    }
}
