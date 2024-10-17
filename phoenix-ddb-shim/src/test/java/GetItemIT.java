import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class GetItemIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetItemIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

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
    public void testWithPartitionAndSortCol() throws Exception {
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

        //creating key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        key.put("SubjectNumber", new AttributeValue().withN("20"));
        GetItemRequest gI = new GetItemRequest(tableName, key);
        String projectionExpr = "LastPostDateTime, Message";
        gI.setProjectionExpression(projectionExpr);
        GetItemResult dynamoResult = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult.getItem(), phoenixResult.getItem());
    }

    @Test(timeout = 120000)
    public void testWithOnlyPartitionCol() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem4());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        //create key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        GetItemRequest gI = new GetItemRequest(tableName, key);
        String projectionExpr = "Message, Tag";
        gI.setProjectionExpression(projectionExpr);
        GetItemResult dynamoResult = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult.getItem(), phoenixResult.getItem());
    }

    @Test(timeout = 120000)
    public void testWithTwoItemsHavingSamePartitionColNames() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "Subject", ScalarAttributeType.S);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put multiple items
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        PutItemRequest putItemRequest2= new PutItemRequest(tableName, getItem2());
        phoenixDBClient.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest2);

        PutItemRequest putItemRequest3 = new PutItemRequest(tableName, getItem3());
        phoenixDBClient.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest3);

        //create key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        key.put("Subject", new AttributeValue().withS("How do I update multiple items?"));
        GetItemRequest gI = new GetItemRequest(tableName, key);
        String projectionExpr = "LastPostDateTime, Message, Tag";
        gI.setProjectionExpression(projectionExpr);
        GetItemResult dynamoResult = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult.getItem(), phoenixResult.getItem());
    }

    @Test(timeout = 120000)
    public void testWithNoResultFound() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "Subject", ScalarAttributeType.S);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", new AttributeValue().withS("Phoenix"));
        key.put("Subject", new AttributeValue().withS("How do I update multiple items?"));
        GetItemRequest gI = new GetItemRequest(tableName, key);
        String projectionExpr = "LastPostDateTime, Message, Tag";
        gI.setProjectionExpression(projectionExpr);
        GetItemResult dynamoResult = amazonDynamoDB.getItem(gI);
        GetItemResult phoenixResult = phoenixDBClient.getItem(gI);
        Assert.assertEquals(dynamoResult.getItem(), phoenixResult.getItem());
    }


    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        item.put("Subject", new AttributeValue().withS("How do I update multiple items?"));
        item.put("Tag", new AttributeValue().withS("Update"));
        item.put("LastPostDateTime", new AttributeValue().withN("201303190436"));
        item.put("Message", new AttributeValue().withS("I want to update multiple items in a single call. What's the best way to do that?"));
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Phoenix"));
        item.put("Subject", new AttributeValue().withS("How do I update multiple items?"));
        item.put("Tag", new AttributeValue().withS("Update"));
        item.put("LastPostDateTime", new AttributeValue().withN("201303190429"));
        item.put("Message", new AttributeValue().withS("I want to update multiple items in a single call. What's the best way to do that?"));
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        item.put("Subject", new AttributeValue().withS("How do I update a single items?"));
        item.put("Tag", new AttributeValue().withS("Update"));
        item.put("LastPostDateTime", new AttributeValue().withN("2013031906422"));
        item.put("Message", new AttributeValue().withS("I want to update multiple items in a single call. What's the best way to do that?"));
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        item.put("Tag", new AttributeValue().withS("Update"));
        item.put("LastPostDateTime", new AttributeValue().withN("201303190317"));
        item.put("Message", new AttributeValue().withS("I want to update multiple items in a single call. What's the best way to do that?"));
        return item;
    }

    private static Map<String, AttributeValue> getItem5() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", new AttributeValue().withS("Amazon DynamoDB"));
        item.put("SubjectNumber", new AttributeValue().withN("20"));
        item.put("Tag", new AttributeValue().withS("Update"));
        item.put("LastPostDateTime", new AttributeValue().withN("201303190436"));
        item.put("Message", new AttributeValue().withS("I want to update multiple items in a single call. What's the best way to do that?"));
        return item;
    }

}
