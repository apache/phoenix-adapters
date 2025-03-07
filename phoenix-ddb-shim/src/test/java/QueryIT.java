import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
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

public class QueryIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIT.class);

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
    public void queryLimitAndFilterTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("#0 = :v0 AND #1 < :v1");
        qr.filterExpression("#2 <= :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "Id2");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("4").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("1000.10").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result, should return 2 items
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 2);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());

        // limit, should return 1 item
        qr.limit(1);
        phoenixResult = phoenixDBClientV2.query(qr.build());
        dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
    }

    @Test(timeout = 120000)
    public void queryBetweenTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("#0 = :v0 AND #1 BETWEEN :v1 AND :v2");
        qr.filterExpression("#2 > :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "Id1");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("1").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("5").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("0").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result, should return 1 item
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void queryBeginsWithTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "title", ScalarAttributeType.S);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("#0 = :v0 AND begins_with(#1, :v1)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "title");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        exprAttrVal.put(":v1", AttributeValue.builder().s("Title4").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result, should return 1 item
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void querySimpleProjectionTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);

        //query request
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        qr.expressionAttributeValues(exprAttrVal);
        String projectionExpr = "Id2, title, #proj";
        qr.projectionExpression(projectionExpr);
        exprAttrNames.put("#proj", "A.B");
        qr.expressionAttributeNames(exprAttrNames);

        // query result, should return 1 item with only the projected attributes
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void queryScanIndexForwardTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("#0 = :v0 AND #1 > :v1");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("1").build());
        qr.expressionAttributeValues(exprAttrVal);
        qr.scanIndexForward(false);

        // query result, should return 3 items in descending order of sort key
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 3);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (int i=0; i<phoenixResult.items().size()-1;i++) {
            int sortKeyVal1 = Integer.parseInt(phoenixResult.items().get(i).get("attr_1").n());
            int sortKeyVal2 = Integer.parseInt(phoenixResult.items().get(i+1).get("attr_1").n());
            Assert.assertTrue(sortKeyVal1 >= sortKeyVal2);
        }
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void queryLastEvaluatedKeyPagingWithHashSortKeysTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("#0 = :v0 AND #1 > :v1");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("1").build());
        qr.expressionAttributeValues(exprAttrVal);
        qr.limit(2);

        // query result, should return 2 items
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 2);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals("B", lastKey.get("attr_0").s());
        Assert.assertEquals(3, Integer.parseInt(lastKey.get("attr_1").n()));

        // provide lastEvaluatedKey as exclusiveStartKey, remaining 1 item should be returned
        qr.limit(null);
        qr.exclusiveStartKey(lastKey);
        phoenixResult = phoenixDBClientV2.query(qr.build());
        dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertTrue(phoenixResult.count() == 1);

        // check last key
        lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals("B", lastKey.get("attr_0").s());
        Assert.assertEquals(4, Integer.parseInt(lastKey.get("attr_1").n()));

        // note that dynamo's last evaluated key will be null here
        // sdkv2 returns empty item
        Assert.assertTrue(dynamoResult.lastEvaluatedKey().isEmpty());

        // provide lastEvaluatedKey as exclusiveStartKey, no items should be returned
        qr.exclusiveStartKey(lastKey);
        phoenixResult = phoenixDBClientV2.query(qr.build());
        dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertTrue(phoenixResult.count() == 0);
    }

    @Test(timeout = 120000)
    public void queryLastEvaluatedKeyPagingWithHashKeyTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        //query request
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        qr.expressionAttributeValues(exprAttrVal);
        qr.limit(1);
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());

        while (phoenixResult.count() > 0) {
            Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
            Assert.assertEquals(dynamoResult.items(), phoenixResult.items());
            Assert.assertEquals(dynamoResult.lastEvaluatedKey(), phoenixResult.lastEvaluatedKey());
            qr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
            phoenixResult = phoenixDBClientV2.query(qr.build());
            dynamoResult = dynamoDbClient.query(qr.build());
        }
    }

    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("A").build());
        item.put("attr_1", AttributeValue.builder().n("1").build());
        item.put("Id1", AttributeValue.builder().n("-5").build());
        item.put("Id2", AttributeValue.builder().n("10.10").build());
        item.put("title", AttributeValue.builder().s("Title1").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Alice").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 1").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("B").build());
        item.put("attr_1", AttributeValue.builder().n("2").build());
        item.put("Id1", AttributeValue.builder().n("-15").build());
        item.put("Id2", AttributeValue.builder().n("150.10").build());
        item.put("title", AttributeValue.builder().s("Title2").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Bob1").build());
        Map<String, AttributeValue> reviewMap2 = new HashMap<>();
        reviewMap2.put("reviewer", AttributeValue.builder().s("Bob2").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(
                AttributeValue.builder().m(reviewMap1).build(),
                AttributeValue.builder().m(reviewMap2).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 2").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("B").build());
        item.put("attr_1", AttributeValue.builder().n("3").build());
        item.put("Id1", AttributeValue.builder().n("11").build());
        item.put("Id2", AttributeValue.builder().n("1000.10").build());
        item.put("title", AttributeValue.builder().s("Title3").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Carl").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 3").build());
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("B").build());
        item.put("attr_1", AttributeValue.builder().n("4").build());
        item.put("Id1", AttributeValue.builder().n("-23").build());
        item.put("Id2", AttributeValue.builder().n("99.10").build());
        item.put("title", AttributeValue.builder().s("Title40").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Drake").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        item.put("A.B", AttributeValue.builder().s("not nested field 4").build());
        return item;
    }
}
