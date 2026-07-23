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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
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
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Integration tests for DeleteItem with various condition expressions.
 * Tests combinations of:
 * - Condition expressions that CAN evaluate on empty doc (attribute_not_exists)
 * - Condition expressions that CANNOT evaluate on empty doc (attribute_exists, value comparisons)
 * - Row exists vs row does not exist
 * - ReturnValues: NONE, ALL_OLD
 * - ReturnValuesOnConditionCheckFailure: NONE, ALL_OLD
 * 
 * All tests verify Phoenix behavior matches LocalDynamoDB behavior.
 */
public class DeleteItem2IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteItem2IT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

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

    // ==================== attribute_not_exists (CAN evaluate on empty doc) ====================

    /**
     * Test: attribute_not_exists condition, row EXISTS, ReturnValues=ALL_OLD
     * Expected: Condition fails (attribute exists), ConditionalCheckFailedException thrown
     */
    @Ignore
    public void testAttributeNotExists_RowExists_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "ForumName");
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_not_exists(#attr)")
                .expressionAttributeNames(exprAttrNames)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        // Both should throw ConditionalCheckFailedException
        ConditionalCheckFailedException dynamoException = null;
        ConditionalCheckFailedException phoenixException = null;

        try {
            dynamoDbClient.deleteItem(request);
            Assert.fail("DynamoDB should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            dynamoException = e;
        }

        try {
            phoenixDBClientV2.deleteItem(request);
            Assert.fail("Phoenix should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            phoenixException = e;
        }

        // Verify exceptions match (item should be null since ReturnValuesOnConditionCheckFailure not set)
        Assert.assertEquals("Exception item should match", dynamoException.item(), phoenixException.item());

        // Verify row still exists
        verifyRow(tableName, key);
    }

    /**
     * Test: attribute_not_exists condition, row EXISTS, ReturnValuesOnConditionCheckFailure=ALL_OLD
     * Expected: Condition fails, exception contains the old item
     */
    @Ignore
    public void testAttributeNotExists_RowExists_ReturnValuesOnConditionCheckFailure() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "ForumName");
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_not_exists(#attr)")
                .expressionAttributeNames(exprAttrNames)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        ConditionalCheckFailedException dynamoException = null;
        ConditionalCheckFailedException phoenixException = null;

        try {
            dynamoDbClient.deleteItem(request);
            Assert.fail("DynamoDB should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            dynamoException = e;
        }

        try {
            phoenixDBClientV2.deleteItem(request);
            Assert.fail("Phoenix should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            phoenixException = e;
        }

        // Both should return the old item in the exception
        Assert.assertNotNull("DynamoDB exception should contain item", dynamoException.item());
        Assert.assertEquals("Exception items should match", dynamoException.item(), phoenixException.item());

        // Verify row still exists
        verifyRow(tableName, key);
    }

    /**
     * Test: attribute_not_exists condition on non-key attribute, row EXISTS but attribute does NOT exist
     * Expected: Condition succeeds, row deleted, ALL_OLD returns the deleted item
     */
    @Test(timeout = 120000)
    public void testAttributeNotExists_RowExists_AttributeMissing_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "NonExistentAttribute");
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_not_exists(#attr)")
                .expressionAttributeNames(exprAttrNames)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed and return the old item
        Assert.assertNotNull("DynamoDB should return attributes", dynamoResponse.attributes());
        Assert.assertEquals("Attributes should match", dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify row is deleted
        verifyRow(tableName, key);
    }

    /**
     * Test: attribute_not_exists condition, row does NOT exist, ReturnValues=ALL_OLD
     * Expected: Condition succeeds (attribute doesn't exist because row doesn't exist), 
     *           delete is a no-op, ALL_OLD returns null/empty
     */
    @Test(timeout = 120000)
    public void testAttributeNotExists_RowNotExists_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableOnly(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "ForumName");
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_not_exists(#attr)")
                .expressionAttributeNames(exprAttrNames)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed with no attributes returned (row didn't exist)
        LOGGER.info("DynamoDB response attributes: {}", dynamoResponse.attributes());
        LOGGER.info("Phoenix response attributes: {}", phoenixResponse.attributes());
        Assert.assertEquals("Attributes should match (both empty/null)", 
                dynamoResponse.attributes(), phoenixResponse.attributes());
        Assert.assertTrue("Attributes should be empty or null", 
                dynamoResponse.attributes() == null || dynamoResponse.attributes().isEmpty());

        // Verify row still doesn't exist
        verifyRow(tableName, key);
    }

    /**
     * Test: attribute_not_exists condition, row does NOT exist, no ReturnValues
     * Expected: Condition succeeds, delete is a no-op
     */
    @Test(timeout = 120000)
    public void testAttributeNotExists_RowNotExists_NoReturnValues() {
        final String tableName = testName.getMethodName();
        createTableOnly(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "ForumName");
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_not_exists(#attr)")
                .expressionAttributeNames(exprAttrNames)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed
        Assert.assertEquals("Attributes should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify row still doesn't exist
        verifyRow(tableName, key);
    }

    // ==================== attribute_exists (CANNOT evaluate on empty doc) ====================

    /**
     * Test: attribute_exists condition, row EXISTS, ReturnValues=ALL_OLD
     * Expected: Condition succeeds, row deleted, ALL_OLD returns the deleted item
     */
    @Test(timeout = 120000)
    public void testAttributeExists_RowExists_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "ForumName");
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_exists(#attr)")
                .expressionAttributeNames(exprAttrNames)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed and return the old item
        Assert.assertNotNull("DynamoDB should return attributes", dynamoResponse.attributes());
        Assert.assertEquals("Attributes should match", dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify row is deleted
        verifyRow(tableName, key);
    }

    /**
     * Test: attribute_exists condition, row EXISTS but attribute does NOT exist
     * Expected: Condition fails, ConditionalCheckFailedException thrown
     */
    @Test(timeout = 120000)
    public void testAttributeExists_RowExists_AttributeMissing_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "NonExistentAttribute");
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_exists(#attr)")
                .expressionAttributeNames(exprAttrNames)
                .returnValues(ReturnValue.ALL_OLD)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        ConditionalCheckFailedException dynamoException = null;
        ConditionalCheckFailedException phoenixException = null;

        try {
            dynamoDbClient.deleteItem(request);
            Assert.fail("DynamoDB should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            dynamoException = e;
        }

        try {
            phoenixDBClientV2.deleteItem(request);
            Assert.fail("Phoenix should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            phoenixException = e;
        }

        Assert.assertEquals("Exception items should match", dynamoException.item(), phoenixException.item());

        // Verify row still exists
        verifyRow(tableName, key);
    }

    /**
     * Test: attribute_exists condition, row does NOT exist, ReturnValues=ALL_OLD
     * Expected: Condition fails, ConditionalCheckFailedException thrown
     */
    @Test(timeout = 120000)
    public void testAttributeExists_RowNotExists_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableOnly(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "ForumName");
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_exists(#attr)")
                .expressionAttributeNames(exprAttrNames)
                .returnValues(ReturnValue.ALL_OLD)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        ConditionalCheckFailedException dynamoException = null;
        ConditionalCheckFailedException phoenixException = null;

        try {
            dynamoDbClient.deleteItem(request);
            Assert.fail("DynamoDB should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            dynamoException = e;
        }

        try {
            phoenixDBClientV2.deleteItem(request);
            Assert.fail("Phoenix should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            phoenixException = e;
        }

        // Both should have null/empty item (row didn't exist)
        LOGGER.info("DynamoDB exception item: {}", dynamoException.item());
        LOGGER.info("Phoenix exception item: {}", phoenixException.item());
        Assert.assertEquals("Exception items should match", dynamoException.item(), phoenixException.item());

        // Verify row still doesn't exist
        verifyRow(tableName, key);
    }

    // ==================== Value comparison (CANNOT evaluate on empty doc) ====================

    /**
     * Test: Value comparison condition succeeds, row EXISTS, ReturnValues=ALL_OLD
     * Expected: Condition succeeds, row deleted, ALL_OLD returns the deleted item
     */
    @Test(timeout = 120000)
    public void testValueComparison_RowExists_ConditionSucceeds_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "Status");
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":val", AttributeValue.builder().s("active").build());
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("#attr = :val")
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrValues)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed and return the old item
        Assert.assertNotNull("DynamoDB should return attributes", dynamoResponse.attributes());
        Assert.assertEquals("Attributes should match", dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify row is deleted
        verifyRow(tableName, key);
    }

    /**
     * Test: Value comparison condition fails, row EXISTS, ReturnValuesOnConditionCheckFailure=ALL_OLD
     * Expected: Condition fails, exception contains the old item
     */
    @Test(timeout = 120000)
    public void testValueComparison_RowExists_ConditionFails_ReturnValuesOnConditionCheckFailure() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "Status");
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":val", AttributeValue.builder().s("inactive").build());
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("#attr = :val")
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrValues)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        ConditionalCheckFailedException dynamoException = null;
        ConditionalCheckFailedException phoenixException = null;

        try {
            dynamoDbClient.deleteItem(request);
            Assert.fail("DynamoDB should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            dynamoException = e;
        }

        try {
            phoenixDBClientV2.deleteItem(request);
            Assert.fail("Phoenix should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            phoenixException = e;
        }

        // Both should return the old item in the exception
        Assert.assertNotNull("DynamoDB exception should contain item", dynamoException.item());
        Assert.assertEquals("Exception items should match", dynamoException.item(), phoenixException.item());

        // Verify row still exists
        verifyRow(tableName, key);
    }

    /**
     * Test: Value comparison condition, row does NOT exist
     * Expected: Condition fails, ConditionalCheckFailedException thrown
     */
    @Test(timeout = 120000)
    public void testValueComparison_RowNotExists() {
        final String tableName = testName.getMethodName();
        createTableOnly(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "Status");
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":val", AttributeValue.builder().s("active").build());
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("#attr = :val")
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrValues)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        ConditionalCheckFailedException dynamoException = null;
        ConditionalCheckFailedException phoenixException = null;

        try {
            dynamoDbClient.deleteItem(request);
            Assert.fail("DynamoDB should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            dynamoException = e;
        }

        try {
            phoenixDBClientV2.deleteItem(request);
            Assert.fail("Phoenix should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            phoenixException = e;
        }

        // Both should have null/empty item (row didn't exist)
        Assert.assertEquals("Exception items should match", dynamoException.item(), phoenixException.item());

        // Verify row still doesn't exist
        verifyRow(tableName, key);
    }

    // ==================== Combined conditions ====================

    /**
     * Test: Combined condition (attribute_not_exists OR value=X), row does NOT exist
     * Expected: Condition succeeds (attribute_not_exists is true), delete is a no-op
     */
    @Test(timeout = 120000)
    public void testCombinedCondition_AttributeNotExistsOrValue_RowNotExists() {
        final String tableName = testName.getMethodName();
        createTableOnly(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "ForumName");
        exprAttrNames.put("#status", "Status");
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":val", AttributeValue.builder().s("active").build());
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_not_exists(#attr) OR #status = :val")
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrValues)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed with no attributes (row didn't exist)
        Assert.assertEquals("Attributes should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify row still doesn't exist
        verifyRow(tableName, key);
    }

    /**
     * Test: Combined condition (attribute_exists AND value=X), row EXISTS, condition succeeds
     * Expected: Condition succeeds, row deleted
     */
    @Test(timeout = 120000)
    public void testCombinedCondition_AttributeExistsAndValue_RowExists_ConditionSucceeds() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#attr", "ForumName");
        exprAttrNames.put("#status", "Status");
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":val", AttributeValue.builder().s("active").build());
        
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .conditionExpression("attribute_exists(#attr) AND #status = :val")
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrValues)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed and return the old item
        Assert.assertNotNull("DynamoDB should return attributes", dynamoResponse.attributes());
        Assert.assertEquals("Attributes should match", dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify row is deleted
        verifyRow(tableName, key);
    }

    // ==================== No condition expression ====================

    /**
     * Test: No condition, row EXISTS, ReturnValues=ALL_OLD
     * Expected: Delete succeeds, ALL_OLD returns the deleted item
     */
    @Test(timeout = 120000)
    public void testNoCondition_RowExists_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableAndPutItem(tableName);

        Map<String, AttributeValue> key = createKey();
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed and return the old item
        Assert.assertNotNull("DynamoDB should return attributes", dynamoResponse.attributes());
        Assert.assertEquals("Attributes should match", dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify row is deleted
        verifyRow(tableName, key);
    }

    /**
     * Test: No condition, row does NOT exist, ReturnValues=ALL_OLD
     * Expected: Delete is a no-op, ALL_OLD returns null/empty
     */
    @Test(timeout = 120000)
    public void testNoCondition_RowNotExists_ReturnAllOld() {
        final String tableName = testName.getMethodName();
        createTableOnly(tableName);

        Map<String, AttributeValue> key = createKey();
        DeleteItemRequest request = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .returnValues(ReturnValue.ALL_OLD)
                .build();

        DeleteItemResponse dynamoResponse = dynamoDbClient.deleteItem(request);
        DeleteItemResponse phoenixResponse = phoenixDBClientV2.deleteItem(request);

        // Both should succeed with no attributes (row didn't exist)
        Assert.assertEquals("Attributes should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());
        Assert.assertTrue("Attributes should be empty or null", 
                dynamoResponse.attributes() == null || dynamoResponse.attributes().isEmpty());

        // Verify row still doesn't exist
        verifyRow(tableName, key);
    }

    // ==================== Helper methods ====================

    private void createTableOnly(String tableName) {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S,
                        "sk", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
    }

    private void createTableAndPutItem(String tableName) {
        createTableOnly(tableName);
        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(createItem()).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);
    }

    private Map<String, AttributeValue> createKey() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("pk", AttributeValue.builder().s("partition1").build());
        key.put("sk", AttributeValue.builder().s("sort1").build());
        return key;
    }

    private Map<String, AttributeValue> createItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", AttributeValue.builder().s("partition1").build());
        item.put("sk", AttributeValue.builder().s("sort1").build());
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Status", AttributeValue.builder().s("active").build());
        item.put("Count", AttributeValue.builder().n("42").build());
        item.put("Tags", AttributeValue.builder().ss("tag1", "tag2").build());
        return item;
    }

    private void verifyRow(String tableName, Map<String, AttributeValue> key) {
        GetItemRequest getRequest = GetItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .build();

        GetItemResponse dynamoResponse = dynamoDbClient.getItem(getRequest);
        GetItemResponse phoenixResponse = phoenixDBClientV2.getItem(getRequest);
        Assert.assertEquals("GetItem responses should match", 
                dynamoResponse.item(), phoenixResponse.item());
    }
}
