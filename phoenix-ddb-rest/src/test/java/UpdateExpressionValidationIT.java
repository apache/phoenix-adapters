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

import java.sql.DriverManager;
import java.util.HashMap;
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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
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
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();

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
}
