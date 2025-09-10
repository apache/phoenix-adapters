import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_NEW;
import static software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure.ALL_OLD;

/**
 * Tests for UpdateItem API with conditional expressions.
 * {@link UpdateItemBaseTests} for tests with different kinds of update expressions.
 */
public class UpdateItemIT extends UpdateItemBaseTests {

    public UpdateItemIT(boolean isSortKeyPresent) {
        super(isSortKeyPresent);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckSuccess() {
        final String tableName = "._404-" + isSortKeyPresent + "DR1FT-Crystal_Echo__";
        createTableAndPutItem(tableName, true);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = :v1, #2 = #2 + :v2, #3 = #3 - :v3");
        uir.conditionExpression("#4.#5[0].#6 = :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "Reviews");
        exprAttrNames.put("#5", "FiveStar");
        exprAttrNames.put("#6", "reviewer");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("3.2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("89.34").build());
        exprAttrVal.put(":condVal", AttributeValue.builder().s("Alice").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckWithOldItemSuccess() {
        final String tableName = "._404-" + isSortKeyPresent + "DR12_--FT-Crystal_Echo__";
        createTableAndPutItem(tableName, true);

        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = :v1, #2 = #2 + :v2, #3 = #3 - :v3");
        uir.conditionExpression("#4.#5[0].#6 = :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "Reviews");
        exprAttrNames.put("#5", "FiveStar");
        exprAttrNames.put("#6", "reviewer");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("3.2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("89.34").build());
        exprAttrVal.put(":condVal", AttributeValue.builder().s("Alice").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ReturnValue.ALL_OLD);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckFailure() {
        final String tableName = "-_-" + isSortKeyPresent + "Ax0n.D3t0nate-Memory_Blue123....";
        createTableAndPutItem(tableName, true);
        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("REMOVE #3");
        uir.conditionExpression("#3 > :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#3", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v3", AttributeValue.builder().n("4.3").build());
        uir.expressionAttributeValues(exprAttrVal);

        Map<String, AttributeValue> dynamoExceptionItem = null;
        try {
            dynamoDbClient.updateItem(uir.build());
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoExceptionItem = e.item();
        }
        try {
            phoenixDBClientV2.updateItem(uir.build());
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(dynamoExceptionItem, e.item());
        }

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckFailureReturnValue() {
        final String tableName = "__---Quasar__Glitch-Surge--O.o" + isSortKeyPresent;
        createTableAndPutItem(tableName, true);
        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("REMOVE #3");
        uir.conditionExpression("#3 > :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#3", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v3", AttributeValue.builder().n("4.3").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValuesOnConditionCheckFailure(ALL_OLD);
        Map<String, AttributeValue> dynamoReturnAttr = null, phoenixReturnAttr = null;
        try {
            dynamoDbClient.updateItem(uir.build());
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoReturnAttr = e.item();
        }
        try {
            phoenixDBClientV2.updateItem(uir.build());
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            phoenixReturnAttr = e.item();
        }
        Assert.assertEquals(dynamoReturnAttr, phoenixReturnAttr);
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConcurrentConditionalUpdateWithReturnValues() {
        final String tableName = "Ne0N._Crypt-x_B0tNet_Transmission-23X__" + isSortKeyPresent;
        createTableAndPutItem(tableName, true);

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        UpdateItemRequest.Builder uir =
                UpdateItemRequest.builder().tableName(tableName).key(getKey());
        uir.updateExpression("SET #1 = #1 + :v1");
        uir.conditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().n("10").build());
        exprAttrVal.put(":condVal", AttributeValue.builder().n("5").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        uir.returnValuesOnConditionCheckFailure(ALL_OLD);
        Map<String, AttributeValue> newItem = dynamoDbClient.updateItem(uir.build()).attributes();

        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                Map<String, AttributeValue> oldItem = null;
                try {
                    dynamoDbClient.updateItem(uir.build());
                } catch (ConditionalCheckFailedException e) {
                    oldItem = e.item();
                }
                try {
                    UpdateItemResponse result = phoenixDBClientV2.updateItem(uir.build());
                    Assert.assertEquals(newItem, result.attributes());
                    updateCount.incrementAndGet();
                } catch (ConditionalCheckFailedException e) {
                    Assert.assertEquals(oldItem, e.item());
                    errorCount.incrementAndGet();
                }
            });
        }
        executorService.shutdown();
        try {
            boolean terminated = executorService.awaitTermination(30, TimeUnit.SECONDS);
            if (terminated) {
                Assert.assertEquals(1, updateCount.get());
                Assert.assertEquals(4, errorCount.get());
            } else {
                Assert.fail(
                        "testConcurrentConditionalUpdateWithReturnValues: threads did not terminate.");
            }
        } catch (InterruptedException e) {
            Assert.fail("testConcurrentConditionalUpdateWithReturnValues was interrupted.");
        }
    }

    /**
     * Test ADD operation creating a new item.
     * DynamoDB semantics: ADD on non-existing item should create item with ADD value.
     */
    @Test(timeout = 120000)
    public void testAddOperationCreateNewItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // ADD operation on non-existing item
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("ADD numericField :val").expressionAttributeValues(
                        Collections.singletonMap(":val", AttributeValue.builder().n("5").build()))
                .returnValues(ALL_NEW).build();

        // Execute on both DynamoDB and Phoenix
        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // Verify both responses match
        Assert.assertEquals("ADD operation responses should match", dynamoResponse.attributes(),
                phoenixResponse.attributes());

        // Verify the item was created with correct value
        Assert.assertNotNull("Item should be created", dynamoResponse.attributes());
        Assert.assertEquals("Numeric field should have ADD value", "5",
                dynamoResponse.attributes().get("numericField").n());
        
        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test mixed SET and ADD operations creating a new item.
     */
    @Test(timeout = 120000)
    public void testMixedSetAddOperationCreateNewItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":str", AttributeValue.builder().s("test").build());
        expressionAttributeValues.put(":num", AttributeValue.builder().n("10").build());

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET stringField = :str ADD numericField :num")
                .expressionAttributeValues(expressionAttributeValues).returnValues(ALL_NEW).build();

        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        Assert.assertEquals("Mixed operation responses should match", dynamoResponse.attributes(),
                phoenixResponse.attributes());
        Assert.assertEquals("String field should be set", "test",
                dynamoResponse.attributes().get("stringField").s());
        Assert.assertEquals("Numeric field should have ADD value", "10",
                dynamoResponse.attributes().get("numericField").n());
        
        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test REMOVE-only operation on non-existing item (should be no-op).
     */
    @Test(timeout = 120000)
    public void testRemoveOnlyOperationOnNonExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("REMOVE nonExistentField").returnValues(ALL_NEW).build();

        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // REMOVE on non-existing item should create empty item (keys only)
        Assert.assertEquals("REMOVE operation responses should match", dynamoResponse.attributes(),
                phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test condition that can be satisfied on empty document.
     */
    @Test(timeout = 120000)
    public void testConditionTrueOnEmptyDocument() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // Condition "attribute_not_exists(someField)" should be true on empty document
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET newField = :val")
                .conditionExpression("attribute_not_exists(someField)").expressionAttributeValues(
                        Collections.singletonMap(":val",
                                AttributeValue.builder().s("created").build()))
                .returnValues(ALL_NEW).build();

        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        Assert.assertEquals("Conditional create responses should match",
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test condition that cannot be satisfied on empty document.
     */
    @Test(timeout = 120000)
    public void testConditionFalseOnEmptyDocument() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // Condition "attribute_exists(someField)" should be false on empty document
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET newField = :val")
                .conditionExpression("attribute_exists(someField)").expressionAttributeValues(
                        Collections.singletonMap(":val",
                                AttributeValue.builder().s("shouldNotCreate").build())).build();

        // Both should throw ConditionalCheckFailedException
        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("DynamoDB should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            // Expected
        }

        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Phoenix should throw ConditionalCheckFailedException");
        } catch (ConditionalCheckFailedException e) {
            // Expected
        }

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test ADD operation on existing item (should add to existing value).
     */
    @Test(timeout = 120000)
    public void testAddOperationOnExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // First, set a numeric field
        UpdateItemRequest setRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("SET numericField = :val").expressionAttributeValues(
                        Collections.singletonMap(":val", AttributeValue.builder().n("5").build()))
                .build();

        dynamoDbClient.updateItem(setRequest);
        phoenixDBClientV2.updateItem(setRequest);

        // Now ADD to the existing numeric field
        UpdateItemRequest addRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
                .updateExpression("ADD numericField :val").expressionAttributeValues(
                        Collections.singletonMap(":val", AttributeValue.builder().n("3").build()))
                .returnValues(ALL_NEW).build();

        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(addRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(addRequest);

        Assert.assertEquals("ADD to existing responses should match", dynamoResponse.attributes(),
                phoenixResponse.attributes());
        
        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test ADD operation with StringSet on non-existing item.
     * DynamoDB semantics: ADD on non-existing item should create item with ADD value.
     */
    @Test(timeout = 120000)
    public void testAddStringSetOnNonExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // ADD StringSet operation on non-existing item
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("ADD stringSet :val")
                .expressionAttributeValues(Collections.singletonMap(":val", 
                        AttributeValue.builder().ss("value1", "value2").build()))
                .returnValues(ALL_NEW)
                .build();

        // Execute on both DynamoDB and Phoenix
        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // Verify both responses match
        Assert.assertEquals("ADD StringSet operation responses should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test ADD operation with NumberSet on non-existing item.
     * DynamoDB semantics: ADD on non-existing item should create item with ADD value.
     */
    @Test(timeout = 120000)
    public void testAddNumberSetOnNonExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // ADD NumberSet operation on non-existing item
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("ADD numberSet :val")
                .expressionAttributeValues(Collections.singletonMap(":val", 
                        AttributeValue.builder().ns("1", "2", "3").build()))
                .returnValues(ALL_NEW)
                .build();

        // Execute on both DynamoDB and Phoenix
        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // Verify both responses match
        Assert.assertEquals("ADD NumberSet operation responses should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }

    /**
     * Test ADD operation with BinarySet on non-existing item.
     * DynamoDB semantics: ADD on non-existing item should create item with ADD value.
     */
    @Test(timeout = 120000)
    public void testUpdateItemReturnValuesValidation() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("test").build());

        // Test NONE - should succeed
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues).returnValues(ReturnValue.NONE)
            .build();
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Test ALL_OLD - should succeed
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues).returnValues(ReturnValue.ALL_OLD)
            .build();
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        // Test ALL_NEW - should succeed
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues).returnValues(ReturnValue.ALL_NEW)
            .build();
        UpdateItemResponse dynamoResult3 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult3 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult3.attributes(), phoenixResult3.attributes());

        // Test UPDATED_OLD - should fail with error status code in phoenixDBClientV2
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValues(ReturnValue.UPDATED_OLD).build();

        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for UPDATED_OLD");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage()
                .contains("UPDATED_OLD or UPDATED_NEW is not supported for ReturnValue"));
        }

        // Test UPDATED_NEW - should fail with error status code in phoenixDBClientV2
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValues(ReturnValue.UPDATED_NEW).build();

        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for UPDATED_NEW");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage()
                .contains("UPDATED_OLD or UPDATED_NEW is not supported for ReturnValue"));
        }

        // Test invalid value - should fail with same error status code in both clients
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues).returnValues("INVALID_VALUE")
            .build();

        int phoenixStatusCode = -1;
        int dynamoStatusCode = -1;
        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage()
                .contains("ReturnValues value 'INVALID_VALUE' is not valid for UpdateItem"));
        }
        Assert.assertEquals("Status codes should match for INVALID_VALUE validation error",
            dynamoStatusCode, phoenixStatusCode);
    }

    @Test(timeout = 120000)
    public void testUpdateItemReturnValuesOnConditionCheckFailureValidation() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":val", AttributeValue.builder().s("test").build());

        // Test NONE - should succeed (validation passes)
        UpdateItemRequest updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.NONE).build();
        UpdateItemResponse dynamoResult1 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult1 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult1.attributes(), phoenixResult1.attributes());

        // Test ALL_OLD - should succeed (validation passes)
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
            .build();
        UpdateItemResponse dynamoResult2 = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResult2 = phoenixDBClientV2.updateItem(updateRequest);
        Assert.assertEquals(dynamoResult2.attributes(), phoenixResult2.attributes());

        // Test invalid value - should fail with same error status code in both clients
        updateRequest = UpdateItemRequest.builder().tableName(tableName).key(key)
            .updateExpression("SET stringField = :val")
            .expressionAttributeValues(expressionAttributeValues)
            .returnValuesOnConditionCheckFailure("INVALID_VALUE").build();
        int dynamoStatusCode = -1;
        int phoenixStatusCode = -1;
        try {
            dynamoDbClient.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            dynamoStatusCode = e.statusCode();
        }
        try {
            phoenixDBClientV2.updateItem(updateRequest);
            Assert.fail("Expected DynamoDbException for invalid value");
        } catch (DynamoDbException e) {
            phoenixStatusCode = e.statusCode();
            Assert.assertTrue(e.getMessage().contains(
                "ReturnValuesOnConditionCheckFailure value 'INVALID_VALUE' is not valid"));
        }
        Assert.assertEquals("Status codes should match for INVALID_VALUE validation error",
            dynamoStatusCode, phoenixStatusCode);
    }

    @Test(timeout = 120000)
    public void testAddBinarySetOnNonExistingItem() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName, false);

        Map<String, AttributeValue> key = getKey();

        // ADD BinarySet operation on non-existing item
        SdkBytes binary1 = SdkBytes.fromUtf8String("data1");
        SdkBytes binary2 = SdkBytes.fromUtf8String("data2");
        
        UpdateItemRequest updateRequest = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .updateExpression("ADD binarySet :val")
                .expressionAttributeValues(Collections.singletonMap(":val", 
                        AttributeValue.builder().bs(binary1, binary2).build()))
                .returnValues(ALL_NEW)
                .build();

        // Execute on both DynamoDB and Phoenix
        UpdateItemResponse dynamoResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(updateRequest);

        // Verify both responses match
        Assert.assertEquals("ADD BinarySet operation responses should match", 
                dynamoResponse.attributes(), phoenixResponse.attributes());

        // Verify final state by querying both Phoenix and DDB
        validateItem(tableName, key);
    }
}
