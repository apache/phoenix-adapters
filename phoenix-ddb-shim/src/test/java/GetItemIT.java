import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClientV2;
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

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

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
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem5()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //creating key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("SubjectNumber", AttributeValue.builder().n("20").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "LastPostDateTime, Message";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }

    @Test(timeout = 120000)
    public void testWithOnlyPartitionCol() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        //create key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "Message, Tag";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }

    @Test(timeout = 120000)
    public void testWithTwoItemsHavingSamePartitionColNames() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "Subject", ScalarAttributeType.S);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put multiple items
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        PutItemRequest putItemRequest2= PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        phoenixDBClientV2.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest2);

        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        phoenixDBClientV2.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest3);

        //create key to get
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        key.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "LastPostDateTime, Message, Tag";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }

    @Test(timeout = 120000)
    public void testWithNoResultFound() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName",
                        ScalarAttributeType.S, "Subject", ScalarAttributeType.S);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ForumName", AttributeValue.builder().s("Phoenix").build());
        key.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        GetItemRequest.Builder gI = GetItemRequest.builder().tableName(tableName).key(key);
        String projectionExpr = "LastPostDateTime, Message, Tag";
        gI.projectionExpression(projectionExpr);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gI.build());
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gI.build());
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }


    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("201303190436").build());
        item.put("Message", AttributeValue.builder().s("I want to update multiple items in a single call. What's the best way to do that?").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Phoenix").build());
        item.put("Subject", AttributeValue.builder().s("How do I update multiple items?").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("201303190429").build());
        item.put("Message", AttributeValue.builder().s("I want to update multiple items in a single call. What's the best way to do that?").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Subject", AttributeValue.builder().s("How do I update a single items?").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("2013031906422").build());
        item.put("Message", AttributeValue.builder().s("I want to update multiple items in a single call. What's the best way to do that?").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("201303190317").build());
        item.put("Message", AttributeValue.builder().s("I want to update multiple items in a single call. What's the best way to do that?").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem5() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s("Amazon DynamoDB").build());
        item.put("SubjectNumber", AttributeValue.builder().n("20").build());
        item.put("Tag", AttributeValue.builder().s("Update").build());
        item.put("LastPostDateTime", AttributeValue.builder().n("201303190436").build());
        item.put("Message", AttributeValue.builder().s("I want to update multiple items in a single call. What's the best way to do that?").build());
        return item;
    }

}
