import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

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
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class DeleteItemIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteItemIT.class);

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

    @Test(timeout = 120000)
    public void testWithOnlyPartitionKey() throws Exception {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());

        //deleting the item with that key
        DeleteItemRequest.Builder dI = DeleteItemRequest.builder().tableName(tableName).key(key);
        DeleteItemResponse dynamoResult = dynamoDbClient.deleteItem(dI.build());
        DeleteItemResponse phoenixResult = phoenixDBClientV2.deleteItem(dI.build());

        //trying to get the same key. we will see returned result is empty
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr2 = "LastPostDateTime, Tags";
        gI.projectionExpression(projectionExpr2);
        GetItemResponse dynamoResult2 = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult2 = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult2.item(), phoenixResult2.item());
    }

    @Test(timeout = 120000)
    public void testWithBothPartitionAndSortKey() throws Exception {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "SubjectNumber", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem5()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("SubjectNumber", AttributeValue.builder().n("20").build());

        //deleting the item with that key
        DeleteItemRequest.Builder dI = DeleteItemRequest.builder().tableName(tableName).key(key);
        DeleteItemResponse dynamoResult = dynamoDbClient.deleteItem(dI.build());
        DeleteItemResponse phoenixResult = phoenixDBClientV2.deleteItem(dI.build());

        //trying to get the same key. we will see returned result is empty
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "LastPostDateTime, Message";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult2 = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult2 = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult2.item(), phoenixResult2.item());

    }

    @Test(timeout = 120000)
    public void testSortKeyNotFound() throws Exception {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "SubjectNumber", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem5()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        //partition key exists but sort key does not
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("SubjectNumber", AttributeValue.builder().n("25").build());
        //delete item will not work since sort key value does not exist in table
        DeleteItemRequest.Builder dI = DeleteItemRequest.builder().tableName(tableName).key(key);
        DeleteItemResponse dynamoResult = dynamoDbClient.deleteItem(dI.build());
        DeleteItemResponse phoenixResult = phoenixDBClientV2.deleteItem(dI.build());

        //since item was not deleted we will still see 1 item in the table
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT COUNT(*) FROM DDB.\"" + tableName + "\"");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
        }
    }

    @Test(timeout = 120000)
    public void testBothKeysNotFound() throws Exception {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "SubjectNumber", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem5()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        //both keys do not exist in the table
        key.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());
        key.put("SubjectNumber", AttributeValue.builder().n("25").build());

        //delete item will not work since sort key value does not exist in table
        DeleteItemRequest.Builder dI = DeleteItemRequest.builder().tableName(tableName).key(key);
        DeleteItemResponse dynamoResult = dynamoDbClient.deleteItem(dI.build());
        DeleteItemResponse phoenixResult = phoenixDBClientV2.deleteItem(dI.build());

        //since item was not deleted we will still see 1 item in the table
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement()
                    .executeQuery("SELECT COUNT(*) FROM DDB.\"" + tableName + "\"");
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
        }

    }

    @Test(timeout = 120000)
    public void testConditionalCheckSuccessWithReturnValue() {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "LastPostDateTime", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem5()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("LastPostDateTime", AttributeValue.builder().s("201303201023").build());

        //deleting the item with that key and returning return value if deleted
        DeleteItemRequest.Builder dI = DeleteItemRequest.builder().tableName(tableName).key(key);
        dI.conditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "SubjectNumber");
        dI.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":condVal", AttributeValue.builder().n("45").build());
        dI.expressionAttributeValues(exprAttrVal);
        dI.returnValues(software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_OLD);

        DeleteItemResponse dynamoResult = dynamoDbClient.deleteItem(dI.build());
        DeleteItemResponse phoenixResult = phoenixDBClientV2.deleteItem(dI.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        //trying to get the same key. we will see returned result is empty
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        GetItemResponse dynamoResult2 = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult2 = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult2.item(), phoenixResult2.item());

    }

    @Test(timeout = 120000)
    public void testConditionalCheckFailureWithReturnValue() {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to delete
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());

        //deleting the item with that key and returning no value because condExpr is false
        DeleteItemRequest.Builder dI = DeleteItemRequest.builder().tableName(tableName).key(key);
        dI.conditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "SubjectNumber");
        dI.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":condVal", AttributeValue.builder().n("2").build());
        dI.expressionAttributeValues(exprAttrVal);
        dI.returnValues(software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_OLD);

        Map<String, AttributeValue> dynamoExceptionItem = null;
        try {
            dynamoDbClient.deleteItem(dI.build());
            Assert.fail("DeleteItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            dynamoExceptionItem = e.item();
        }
        try {
            phoenixDBClientV2.deleteItem(dI.build());
            Assert.fail("DeleteItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(dynamoExceptionItem, e.item());
        }

        //key was not deleted and is still there
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        GetItemResponse dynamoResult2 = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult2 = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult2.item(), phoenixResult2.item());

    }

    @Test(timeout = 120000)
    public void testWithReturnValuesOnConditionCheckFailure() {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to delete and setting ReturnValuesOnConditionCheckFailure
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());

        //deleting the item with that key
        DeleteItemRequest.Builder dI = DeleteItemRequest.builder().tableName(tableName).key(key);
        dI.conditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "SubjectNumber");
        dI.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":condVal", AttributeValue.builder().n("5").build());
        dI.expressionAttributeValues(exprAttrVal);
        dI.returnValuesOnConditionCheckFailure(
                software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure.ALL_OLD);
        try {
            dynamoDbClient.deleteItem(dI.build());
            Assert.fail("Delete item should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            //dynamodb returns item if condition expression fails and ReturnValuesOnConditionCheckFailure is set
            Assert.assertNotNull(e.item());
        }
        try {
            phoenixDBClientV2.deleteItem(dI.build());
            Assert.fail("Delete item should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            //phoenix returns null if condition expression fails and ReturnValuesOnConditionCheckFailure is set
            // sdkv2 will have empty map
            Assert.assertTrue(e.item().isEmpty());
        }

        //key was not deleted and is still there
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        GetItemResponse dynamoResult2 = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult2 = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult2.item(), phoenixResult2.item());
    }

    @Test(timeout = 120000)
    public void testConcurrentConditionalUpdateWithReturnValues() {
        final String tableName = testName.getMethodName();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        ExecutorService executorService = Executors.newFixedThreadPool(5);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        //creating key to delete and setting ReturnValuesOnConditionCheckFailure
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());

        //deleting the item with that key
        DeleteItemRequest.Builder dI = DeleteItemRequest.builder().tableName(tableName).key(key);
        dI.conditionExpression("#1 < :condVal");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "SubjectNumber");
        dI.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":condVal", AttributeValue.builder().n("45").build());
        dI.expressionAttributeValues(exprAttrVal);
        dI.returnValues(software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_OLD);
        List<Future<?>> futures = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            futures.add(executorService.submit(() -> {
                try {
                    dynamoDbClient.deleteItem(dI.build());
                } catch (ConditionalCheckFailedException e) {
                }
                try {
                    phoenixDBClientV2.deleteItem(dI.build());
                    updateCount.incrementAndGet();
                } catch (ConditionalCheckFailedException e) {
                    errorCount.incrementAndGet();
                }
            }));
        }
        futures.forEach(future -> {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        });

        //key was deleted once and is not there and returned result is empty
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        GetItemResponse dynamoResult2 = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult2 = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult2.item(), phoenixResult2.item());

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

    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon RDS").build());
        item.put("LastPostedBy", AttributeValue.builder().s("brad@example.com").build());
        item.put("LastPostDateTime", AttributeValue.builder().s("201303201042").build());
        item.put("Tags", AttributeValue.builder().s(("Update")).build());
        item.put("SubjectNumber", AttributeValue.builder().n("35").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem5() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("SubjectNumber", AttributeValue.builder().n("20").build());
        item.put("LastPostedBy", AttributeValue.builder().s("fred@example.com").build());
        item.put("LastPostDateTime", AttributeValue.builder().s("201303201023").build());
        item.put("Tags", AttributeValue.builder().s(("Update,Multiple Items,HelpMe")).build());
        item.put("Subject",
                AttributeValue.builder().s(("How do I update multiple items?")).build());
        item.put("Message", AttributeValue.builder()
                .s("I want to update multiple items in a single call. What's the best way to do that?")
                .build());
        return item;
    }

}