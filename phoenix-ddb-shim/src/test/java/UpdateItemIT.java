import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.amazonaws.services.dynamodbv2.model.ReturnValue.ALL_NEW;
import static com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure.ALL_OLD;

/**
 * Tests for UpdateItem API with conditional expressions.
 * {@link UpdateItemBaseTests} for tests with different kinds of update expressions.
 *
 */
public class UpdateItemIT extends UpdateItemBaseTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemIT.class);

    public UpdateItemIT(boolean isSortKeyPresent) {
        super(isSortKeyPresent);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckSuccess() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("SET #1 = :v1, #2 = #2 + :v2, #3 = #3 - :v3");
        uir.setConditionExpression("#4.#5[0].#6 = :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "Reviews");
        exprAttrNames.put("#5", "FiveStar");
        exprAttrNames.put("#6", "reviewer");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", new AttributeValue().withS("TiTlE2"));
        exprAttrVal.put(":v2", new AttributeValue().withN("3.2"));
        exprAttrVal.put(":v3", new AttributeValue().withN("89.34"));
        exprAttrVal.put(":condVal", new AttributeValue().withS("Alice"));
        uir.setExpressionAttributeValues(exprAttrVal);
        uir.setReturnValues(ALL_NEW);
        UpdateItemResult dynamoResult = amazonDynamoDB.updateItem(uir);
        UpdateItemResult phoenixResult = phoenixDBClient.updateItem(uir);
        Assert.assertEquals(dynamoResult.getAttributes(), phoenixResult.getAttributes());
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckFailure() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);
        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("REMOVE #3");
        uir.setConditionExpression("#3 > :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#3", "COL1");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v3", new AttributeValue().withN("4.3"));
        uir.setExpressionAttributeValues(exprAttrVal);
        try {
            amazonDynamoDB.updateItem(uir);
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }
        try {
            phoenixDBClient.updateItem(uir);
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConditionalCheckFailureReturnValue() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);
        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("REMOVE #3");
        uir.setConditionExpression("#3 > :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#3", "COL1");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v3", new AttributeValue().withN("4.3"));
        uir.setExpressionAttributeValues(exprAttrVal);
        uir.setReturnValuesOnConditionCheckFailure(ALL_OLD);
        Map<String, AttributeValue> dynamoReturnAttr = null, phoenixReturnAttr = null;
        try {
            amazonDynamoDB.updateItem(uir);
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoReturnAttr = e.getItem();
        }
        try {
            phoenixDBClient.updateItem(uir);
            Assert.fail("UpdateItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            phoenixReturnAttr = e.getItem();
        }
        Assert.assertEquals(dynamoReturnAttr, phoenixReturnAttr);
        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testConcurrentConditionalUpdateWithReturnValues() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(getKey());
        uir.setUpdateExpression("SET #1 = #1 + :v1");
        uir.setConditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", new AttributeValue().withN("10"));
        exprAttrVal.put(":condVal", new AttributeValue().withN("5"));
        uir.setExpressionAttributeValues(exprAttrVal);
        uir.setReturnValues(ALL_NEW);
        uir.setReturnValuesOnConditionCheckFailure(ALL_OLD);
        Map<String, AttributeValue> newItem = amazonDynamoDB.updateItem(uir).getAttributes();

        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                Map<String, AttributeValue> oldItem = null;
                try {
                    amazonDynamoDB.updateItem(uir);
                } catch (ConditionalCheckFailedException e) {
                    oldItem = e.getItem();
                }
                try {
                    UpdateItemResult result = phoenixDBClient.updateItem(uir);
                    Assert.assertEquals(newItem, result.getAttributes());
                    updateCount.incrementAndGet();
                } catch (ConditionalCheckFailedException e) {
                    Assert.assertEquals(oldItem, e.getItem());
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
                Assert.fail("testConcurrentConditionalUpdateWithReturnValues: threads did not terminate.");
            }
        } catch (InterruptedException e) {
            Assert.fail("testConcurrentConditionalUpdateWithReturnValues was interrupted.");
        }

    }
}
