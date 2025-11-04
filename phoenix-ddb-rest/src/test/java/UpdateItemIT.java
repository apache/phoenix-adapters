import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import static software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_NEW;
import static software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure.ALL_OLD;

/**
 * Tests for UpdateItem API with conditional expressions.
 * {@link UpdateItemBaseTests} for tests with different kinds of update expressions.
 */
public class UpdateItemIT extends UpdateItemBaseTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemIT.class);

    public UpdateItemIT(boolean isSortKeyPresent) {
        super(isSortKeyPresent);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckSuccess() {
        final String tableName = "._404-" + isSortKeyPresent + "DR1FT-Crystal_Echo__";
        createTableAndPutItem(tableName);

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
    public void testConditionalCheckFailure() {
        final String tableName = "-_-" + isSortKeyPresent + "Ax0n.D3t0nate-Memory_Blue123....";
        createTableAndPutItem(tableName);
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
        createTableAndPutItem(tableName);
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
        createTableAndPutItem(tableName);

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
}
