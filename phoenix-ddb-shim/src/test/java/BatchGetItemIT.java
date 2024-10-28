import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchGetItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;
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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class BatchGetItemIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchGetItemIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static PhoenixDBClient phoenixDBClient = null;

    private static String url;

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

        final String tableName1 = "FORUM";
        final String tableName2 = "DATABASE";

        //create table
        CreateTableRequest createTableRequest1 =
                DDLTestUtils.getCreateTableRequest(tableName1, "ForumName",
                        ScalarAttributeType.S, null, null);
        CreateTableRequest createTableRequest2 =
                DDLTestUtils.getCreateTableRequest(tableName2, "DatabaseName",
                        ScalarAttributeType.S, "Id", ScalarAttributeType.N);
        phoenixDBClient = new PhoenixDBClient(url);
        AmazonDynamoDB amazonDynamoDB =
                LocalDynamoDbTestBase.localDynamoDb().createV1Client();

        phoenixDBClient.createTable(createTableRequest1);
        amazonDynamoDB.createTable(createTableRequest1);
        phoenixDBClient.createTable(createTableRequest2);
        amazonDynamoDB.createTable(createTableRequest2);

        //put items in both tables
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName1, getItem1());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        PutItemRequest putItemRequest2 = new PutItemRequest(tableName1, getItem2());
        phoenixDBClient.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest2);

        PutItemRequest putItemRequest4 = new PutItemRequest(tableName1, getItem4());
        phoenixDBClient.putItem(putItemRequest4);
        amazonDynamoDB.putItem(putItemRequest4);

        PutItemRequest putItemRequest3 = new PutItemRequest(tableName2, getItem3());
        phoenixDBClient.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest3);
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

    @Test
    public void testAllTablesWithResultsFound() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        Map<String, AttributeValue> key2 = new HashMap<>();
        key2.put("ForumName", new AttributeValue().withS("Amazon RDS"));

        //putting keys for FORUM table in a list since KeyAndAttributes is List<Map<String, AttributeValue>>
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        forumKeys.add(key1);
        forumKeys.add(key2);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes forForum = new KeysAndAttributes();
        forForum.setKeys(forumKeys);
        //set projection expression for that item
        String projectionExprForForum = "ForumName, Threads, Messages";
        forForum.setProjectionExpression(projectionExprForForum);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("FORUM", forForum);

        //making keys for DATABASE table
        Map<String, AttributeValue> key3 = new HashMap<>();
        key3.put("DatabaseName", new AttributeValue().withS("Amazon Redshift"));
        key3.put("Id", new AttributeValue().withN("25"));

        //putting keys for DATABASE table in a list
        List<Map<String, AttributeValue>> databaseKeys = new ArrayList<>();
        databaseKeys.add(key3);

        //putting that list in KeysAndAttributes Object
        KeysAndAttributes forDatabase = new KeysAndAttributes();
        forDatabase.setKeys(databaseKeys);
        //set projection expression for that item
        String projectionExprForDatabase = "Tags, Message";
        forDatabase.setProjectionExpression(projectionExprForDatabase);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("DATABASE", forDatabase);
        BatchGetItemRequest gI = new BatchGetItemRequest(requestItems);
        BatchGetItemResult dynamoResult = amazonDynamoDB.batchGetItem(gI);
        BatchGetItemResult phoenixResult = phoenixDBClient.batchGetItem(gI);
        Assert.assertEquals(dynamoResult.getResponses(), phoenixResult.getResponses());
    }

    @Test
    public void testOneTableHasNoResultFound() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table
        //key1 is not found in the table
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", new AttributeValue().withS("Amazon API"));
        Map<String, AttributeValue> key2 = new HashMap<>();
        key2.put("ForumName", new AttributeValue().withS("Amazon RDS"));

        //putting keys for FORUM table in a list since KeyAndAttributes is List<Map<String, AttributeValue>>
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        forumKeys.add(key1);
        forumKeys.add(key2);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes forForum = new KeysAndAttributes();
        forForum.setKeys(forumKeys);
        //set projection expression for that item
        String projectionExprForForum = "ForumName, Threads, Messages";
        forForum.setProjectionExpression(projectionExprForForum);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("FORUM", forForum);

        //making keys for DATABASE table
        Map<String, AttributeValue> key3 = new HashMap<>();
        key3.put("DatabaseName", new AttributeValue().withS("Amazon Redshift"));
        key3.put("Id", new AttributeValue().withN("25"));

        //putting keys for DATABASE table in a list
        List<Map<String, AttributeValue>> databaseKeys = new ArrayList<>();
        databaseKeys.add(key3);

        //putting that list in KeysAndAttributes Object
        KeysAndAttributes forDatabase = new KeysAndAttributes();
        forDatabase.setKeys(databaseKeys);
        //set projection expression for that item
        String projectionExprForDatabase = "Tags, Message";
        forDatabase.setProjectionExpression(projectionExprForDatabase);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("DATABASE", forDatabase);

        BatchGetItemRequest gI = new BatchGetItemRequest(requestItems);
        BatchGetItemResult dynamoResult = amazonDynamoDB.batchGetItem(gI);
        BatchGetItemResult phoenixResult = phoenixDBClient.batchGetItem(gI);
        Assert.assertEquals(dynamoResult.getResponses(), phoenixResult.getResponses());
    }


    @Test
    public void testBothTablesHaveNoResultFound() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table which doesnt exist
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", new AttributeValue().withS("Amazon API"));

        //putting key for FORUM table in a list since KeyAndAttributes is List<Map<String, AttributeValue>>
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        forumKeys.add(key1);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes forForum = new KeysAndAttributes();
        forForum.setKeys(forumKeys);
        //set projection expression for that item
        String projectionExprForForum = "ForumName, Threads, Messages";
        forForum.setProjectionExpression(projectionExprForForum);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("FORUM", forForum);

        //making keys for DATABASE table which doesn't exist
        Map<String, AttributeValue> key3 = new HashMap<>();
        //Partition Key exists but not sort key
        key3.put("DatabaseName", new AttributeValue().withS("Amazon RDS"));
        key3.put("Id", new AttributeValue().withN("39"));


        //putting key for DATABASE table in a list
        List<Map<String, AttributeValue>> databaseKeys = new ArrayList<>();
        databaseKeys.add(key3);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes forDatabase = new KeysAndAttributes();
        forDatabase.setKeys(databaseKeys);
        //set projection expression for that item
        String projectionExprForDatabase = "Tags, Message";
        forDatabase.setProjectionExpression(projectionExprForDatabase);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("DATABASE", forDatabase);

        BatchGetItemRequest gI = new BatchGetItemRequest(requestItems);
        BatchGetItemResult dynamoResult = amazonDynamoDB.batchGetItem(gI);
        BatchGetItemResult phoenixResult = phoenixDBClient.batchGetItem(gI);
        Assert.assertEquals(dynamoResult.getResponses(), phoenixResult.getResponses());
    }

    @Test
    public void testWithNestedList() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        Map<String, AttributeValue> key2 = new HashMap<>();
        key2.put("ForumName", new AttributeValue().withS("Amazon RDS"));

        //putting keys for FORUM table in a list since KeyAndAttributes is List<Map<String, AttributeValue>>
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        forumKeys.add(key1);
        forumKeys.add(key2);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes forForum = new KeysAndAttributes();
        forForum.setKeys(forumKeys);
        //set expression attribute name for that item
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "DatabaseName");
        exprAttrNames.put("#1", "Id");
        exprAttrNames.put("#2", "Feedback");
        forForum.setExpressionAttributeNames(exprAttrNames);
        //set projection expression for that item
        String projectionExpr = "#0, #1, #2.FeedbackDetails[0].Sender";
        forForum.setProjectionExpression(projectionExpr);

        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("FORUM", forForum);

        BatchGetItemRequest gI = new BatchGetItemRequest(requestItems);
        BatchGetItemResult dynamoResult = amazonDynamoDB.batchGetItem(gI);
        BatchGetItemResult phoenixResult = phoenixDBClient.batchGetItem(gI);
        //sort both the responses since dynamodb doesnt guarantee any order
        dynamoResult.getResponses().get("FORUM").
                sort(Comparator.comparingInt(entry -> (int) Integer.parseInt(entry.get("Id").getN())));
        phoenixResult.getResponses().get("FORUM").
                sort(Comparator.comparingInt(entry -> (int) Integer.parseInt(entry.get("Id").getN())));
        Assert.assertEquals(dynamoResult.getResponses(), phoenixResult.getResponses());
    }

    @Test
    public void testWithUnprocessedKeys() throws Exception {
        //create BatchGetItem request by adding keys per table
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();

        //making keys for FORUM table
        Map<String, AttributeValue> key1 = new HashMap<>();
        key1.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        Map<String, AttributeValue> key2 = new HashMap<>();
        key2.put("ForumName", new AttributeValue().withS("Amazon RDS"));
        Map<String, AttributeValue> key4 = new HashMap<>();
        key4.put("ForumName", new AttributeValue().withS("Amazon DBS"));

        //putting 102 keys for FORUM table with limit of keys to process at once as 100
        List<Map<String, AttributeValue>> forumKeys = new ArrayList<>();
        for(int i = 0; i < 100; i++){
            forumKeys.add(key1);
        }
        forumKeys.add(key2);
        forumKeys.add(key4);

        //putting that list in KeysAndAttribute object
        KeysAndAttributes forForum = new KeysAndAttributes();
        forForum.setKeys(forumKeys);
        //set projection expression for that item
        String projectionExpr = "DatabaseName, Id";
        forForum.setProjectionExpression(projectionExpr);

        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("FORUM", forForum);

        //making keys for DATABASE table
        Map<String, AttributeValue> key3 = new HashMap<>();
        key3.put("DatabaseName", new AttributeValue().withS("Amazon Redshift"));
        key3.put("Id", new AttributeValue().withN("25"));

        //putting keys for DATABASE table in a list
        List<Map<String, AttributeValue>> databaseKeys = new ArrayList<>();
        databaseKeys.add(key3);

        //putting that list in KeysAndAttributes Object
        KeysAndAttributes forDatabase = new KeysAndAttributes();
        forDatabase.setKeys(databaseKeys);
        //set projection expression for that item
        String projectionExprForDatabase = "Tags, Message";
        forDatabase.setProjectionExpression(projectionExprForDatabase);
        //putting the KeyAndAttribute object with the table name in the BatchGetItem request
        requestItems.put("DATABASE", forDatabase);

        BatchGetItemRequest gI = new BatchGetItemRequest(requestItems);
        //we dont test dynamo result here since it gives exception on duplicate keys
        BatchGetItemResult phoenixResult = phoenixDBClient.batchGetItem(gI);
        //since 102 keys were sent in the request, we expect 2 keys to not be processed for FORUM table
        Assert.assertEquals(2, phoenixResult.getUnprocessedKeys().get("FORUM").getKeys().size());
        //since 1 key was sent in the request, we expect DATABASE table to not be in the unprocessed key set
        Assert.assertEquals(false, phoenixResult.getUnprocessedKeys().containsKey("DATABASE"));

        //doing batch get item request on the unprocessed request that was returned
        BatchGetItemRequest gI2 = new BatchGetItemRequest(phoenixResult.getUnprocessedKeys());
        BatchGetItemResult dynamoResult2 = amazonDynamoDB.batchGetItem(gI2);
        BatchGetItemResult phoenixResult2 = phoenixDBClient.batchGetItem(gI2);
        Assert.assertEquals(dynamoResult2.getResponses(), phoenixResult2.getResponses());
        //no unprocessed keys are returned
        Assert.assertEquals(dynamoResult2.getUnprocessedKeys(), phoenixResult2.getUnprocessedKeys());
    }



    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DatabaseName", new AttributeValue().withS("Amazon DynamoDB"));
        item.put("Id", new AttributeValue().withN("5"));
        Map<String, AttributeValue> messageMap1 = new HashMap<>();
        messageMap1.put("Sender", new AttributeValue().withS("Jake"));
        messageMap1.put("Type", new AttributeValue().withS("Positive Feedback"));
        Map<String, AttributeValue> feedback = new HashMap<>();
        feedback.put("FeedbackDetails", new AttributeValue().withL(new AttributeValue().withM(messageMap1)));
        item.put("Feedback", new AttributeValue().withM(feedback));
        item.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        item.put("Subject", new AttributeValue().withS("Concurrent Reads"));
        item.put("Tags", new AttributeValue().withS("Reads"));
        item.put("Message", new AttributeValue().withS("How many users can read a single data item at a time? Are there any limits?"));
        item.put("Threads", new AttributeValue().withN("12"));
        item.put("Messages", new AttributeValue().withN("55"));
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DatabaseName", new AttributeValue().withS("Amazon RDS"));
        item.put("Id", new AttributeValue().withN("20"));
        Map<String, AttributeValue> messageMap1 = new HashMap<>();
        messageMap1.put("Sender", new AttributeValue().withS("Bob"));
        messageMap1.put("Type", new AttributeValue().withS("Neutral Feedback"));
        Map<String, AttributeValue> feedback = new HashMap<>();
        feedback.put("FeedbackDetails", new AttributeValue().withL(new AttributeValue().withM(messageMap1)));
        item.put("Feedback", new AttributeValue().withM(feedback));
        item.put("ForumName", new AttributeValue().withS("Amazon RDS"));
        item.put("Subject", new AttributeValue().withS("Concurrent Reads"));
        item.put("Tags", new AttributeValue().withS("Writes"));
        item.put("Message", new AttributeValue().withS("How many users can read multiple data item at a time? Are there any limits?"));
        item.put("Threads", new AttributeValue().withN("8"));
        item.put("Messages", new AttributeValue().withN("32"));
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DatabaseName", new AttributeValue().withS("Amazon DBS"));
        item.put("Id", new AttributeValue().withN("35"));
        Map<String, AttributeValue> messageMap1 = new HashMap<>();
        messageMap1.put("Sender", new AttributeValue().withS("Lynn"));
        messageMap1.put("Type", new AttributeValue().withS("Neutral Feedback"));
        Map<String, AttributeValue> feedback = new HashMap<>();
        feedback.put("FeedbackDetails", new AttributeValue().withL(new AttributeValue().withM(messageMap1)));
        item.put("Feedback", new AttributeValue().withM(feedback));
        item.put("ForumName", new AttributeValue().withS("Amazon DBS"));
        item.put("Subject", new AttributeValue().withS("Concurrent Reads"));
        item.put("Tags", new AttributeValue().withS("Writes"));
        item.put("Message", new AttributeValue().withS("How many users can read multiple data item at a time? Are there any limits?"));
        item.put("Threads", new AttributeValue().withN("8"));
        item.put("Messages", new AttributeValue().withN("32"));
        return item;
    }


    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DatabaseName", new AttributeValue().withS("Amazon Redshift"));
        item.put("Id", new AttributeValue().withN("25"));
        Map<String, AttributeValue> messageMap1 = new HashMap<>();
        messageMap1.put("Sender", new AttributeValue().withS("Fred"));
        messageMap1.put("Type", new AttributeValue().withS("Negative Feedback"));
        Map<String, AttributeValue> feedback = new HashMap<>();
        feedback.put("FeedbackDetails", new AttributeValue().withL(new AttributeValue().withM(messageMap1)));
        item.put("Feedback", new AttributeValue().withM(feedback));
        item.put("ForumName", new AttributeValue().withS("Amazon Redshift"));
        item.put("Subject", new AttributeValue().withS("Concurrent Writes"));
        item.put("Tags", new AttributeValue().withS("Writes"));
        item.put("Message", new AttributeValue().withS("How many users can write multiple data item at a time? Are there any limits?"));
        item.put("Threads", new AttributeValue().withN("12"));
        item.put("Messages", new AttributeValue().withN("55"));
        return item;
    }
}
