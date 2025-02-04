import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests for DeleteItem API. Brings up local DynamoDB server and HBase miniCluster, and tests
 * DeleteItem API with same request against both DDB and HBase/Phoenix servers and
 * compares the response.
 */
public class DeleteItemIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteItemIT.class);

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

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
    }

    @AfterClass
    public static void stopLocalDynamoDb() throws IOException, SQLException {
        LocalDynamoDbTestBase.localDynamoDb().stop();
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

    @Test(timeout = 120000)
    public void testWithOnlyPartitionKey() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon RDS"));

        //deleting the item with that key
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);

        //trying to get the same key. we will see returned result is empty
        GetItemRequest gI = new GetItemRequest(tableName, key);
        String projectionExpr2 = "LastPostDateTime, Tags";
        gI.setProjectionExpression(projectionExpr2);
        GetItemResult dynamoResult2 = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult2 = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult2.getItem(), phoenixResult2.getItem());
    }

    @Test(timeout = 120000)
    public void testWithBothPartitionAndSortKey() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "SubjectNumber", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem5());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        key.put("SubjectNumber", new AttributeValue().withN("20"));

        //deleting the item with that key
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);

        //trying to get the same key. we will see returned result is empty
        GetItemRequest gI = new GetItemRequest(tableName, key);
        String projectionExpr = "LastPostDateTime, Message";
        gI.setProjectionExpression(projectionExpr);
        GetItemResult dynamoResult2 = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult2 = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult2.getItem(), phoenixResult2.getItem());

    }

    @Test(timeout = 120000)
    public void testSortKeyNotFound() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "SubjectNumber", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem5());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        //partition key exists but sort key does not
        key.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        key.put("SubjectNumber", new AttributeValue().withN("25"));
        //delete item will not work since sort key value does not exist in table
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);

        //since item was not deleted we will still see 1 item in the table
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
        }
    }

    @Test(timeout = 120000)
    public void testBothKeysNotFound() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "SubjectNumber", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem5());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        //both keys do not exist in the table
        key.put("ForumName", new AttributeValue().withS("Amazon RDS"));
        key.put("SubjectNumber", new AttributeValue().withN("25"));

        //delete item will not work since sort key value does not exist in table
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);

        //since item was not deleted we will still see 1 item in the table
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT COUNT(*) FROM " + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
        }

    }

    @Test(timeout = 120000)
    public void testConditionalCheckSuccessWithReturnValue(){
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "LastPostDateTime", ScalarAttributeType.S);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem5());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        key.put("LastPostDateTime", new AttributeValue().withS("201303201023"));


        //deleting the item with that key and returning return value if deleted
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        dI.setConditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "SubjectNumber");
        dI.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":condVal", new AttributeValue().withN("45"));
        dI.setExpressionAttributeValues(exprAttrVal);
        dI.setReturnValues(com.amazonaws.services.dynamodbv2.model.ReturnValue.ALL_OLD);

        DeleteItemResult dynamoResult = amazonDynamoDB.deleteItem(dI);
        DeleteItemResult phoenixResult = phoenixDBClient.deleteItem(dI);
        Assert.assertEquals(dynamoResult, phoenixResult);

        //trying to get the same key. we will see returned result is empty
        GetItemRequest gI = new GetItemRequest(tableName, key);
        GetItemResult dynamoResult2 = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult2 = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult2.getItem(), phoenixResult2.getItem());

    }

    @Test(timeout = 120000)
    public void testConditionalCheckFailureWithReturnValue(){
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon RDS"));

        //deleting the item with that key and returning no value because condExpr is false
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        dI.setConditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "SubjectNumber");
        dI.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":condVal", new AttributeValue().withN("2"));
        dI.setExpressionAttributeValues(exprAttrVal);
        dI.setReturnValues(com.amazonaws.services.dynamodbv2.model.ReturnValue.ALL_OLD);

        try {
            amazonDynamoDB.deleteItem(dI);
            Assert.fail("DeleteItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }
        try {
            phoenixDBClient.deleteItem(dI);
            Assert.fail("DeleteItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }

        //key was not deleted and is still there
        GetItemRequest gI = new GetItemRequest(tableName, key);
        GetItemResult dynamoResult2 = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult2 = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult2.getItem(), phoenixResult2.getItem());

    }
    @Test(timeout = 120000)
    public void testWithReturnValuesOnConditionCheckFailure() {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //creating key to delete and setting ReturnValuesOnConditionCheckFailure
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon RDS"));

        //deleting the item with that key
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        dI.setConditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "SubjectNumber");
        dI.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":condVal", new AttributeValue().withN("5"));
        dI.setExpressionAttributeValues(exprAttrVal);
        dI.setReturnValuesOnConditionCheckFailure(com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure.ALL_OLD);
        try {
            amazonDynamoDB.deleteItem(dI);
            Assert.fail("Delete item should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            //dynamodb returns item if condition expression fails and ReturnValuesOnConditionCheckFailure is set
            Assert.assertNotNull(e.getItem());
        }
        try {
            phoenixDBClient.deleteItem(dI);
            Assert.fail("Delete item should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            //phoenix returns null if condition expression fails and ReturnValuesOnConditionCheckFailure is set
            Assert.assertNull(e.getItem());
        }

        //key was not deleted and is still there
        GetItemRequest gI = new GetItemRequest(tableName, key);
        GetItemResult dynamoResult2 = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult2 = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult2.getItem(), phoenixResult2.getItem());
    }

    @Test(timeout = 120000)
    public void testConcurrentConditionalUpdateWithReturnValues() {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        //creating key to delete and setting ReturnValuesOnConditionCheckFailure
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon RDS"));

        //deleting the item with that key
        DeleteItemRequest dI = new DeleteItemRequest(tableName, key);
        dI.setConditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "SubjectNumber");
        dI.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":condVal", new AttributeValue().withN("45"));
        dI.setExpressionAttributeValues(exprAttrVal);
        dI.setReturnValues(com.amazonaws.services.dynamodbv2.model.ReturnValue.ALL_OLD);
        for (int i = 0; i < 5; i++) {
            executorService.submit(() -> {
                try {
                    amazonDynamoDB.deleteItem(dI);
                } catch (ConditionalCheckFailedException e) {
                    Assert.assertNull(e.getItem());
                }
                try {
                    phoenixDBClient.deleteItem(dI);
                    updateCount.incrementAndGet();
                } catch (ConditionalCheckFailedException e) {
                    Assert.assertNull(e.getItem());
                    errorCount.incrementAndGet();
                }
            });
        }

        //key was deleted once and is not there and returned result is empty
        GetItemRequest gI = new GetItemRequest(tableName, key);
        GetItemResult dynamoResult2 = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult2 = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult2.getItem(), phoenixResult2.getItem());

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


    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Amazon RDS"));
        item.put("LastPostedBy", new AttributeValue().withS("brad@example.com"));
        item.put("LastPostDateTime", new AttributeValue().withS("201303201042"));
        item.put("Tags", new AttributeValue().withS(("Update")));
        item.put("SubjectNumber", new AttributeValue().withN("35"));
        return item;
    }
    private static Map<String, AttributeValue> getItem5() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        item.put("SubjectNumber", new AttributeValue().withN("20"));
        item.put("LastPostedBy", new AttributeValue().withS("fred@example.com"));
        item.put("LastPostDateTime", new AttributeValue().withS("201303201023"));
        item.put("Tags", new AttributeValue().withS(("Update,Multiple Items,HelpMe")));
        item.put("Subject", new AttributeValue().withS(("How do I update multiple items?")));
        item.put("Message", new AttributeValue().withS("I want to update multiple items in a single call. What's the best way to do that?"));
        return item;
    }

}