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

/**
 * Tests for index functionality with other QueryRequest parameters -
 * lastEvaluatedKey, scanIndexForward, filterExpression, and limit.
 */
public class QueryIndex2IT {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIndex2IT.class);

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
    public void testPaginationWithLastKey() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        //query request using index
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        qr.keyConditionExpression("#0 = :v0 AND #1 < :v1");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "attr_1");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("str_val_1").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("453.23").build());
        qr.expressionAttributeValues(exprAttrVal);
        qr.limit(1);

        // query result
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

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testFilterExpressionWithIndex() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        qr.keyConditionExpression("#0 = :v0 AND #1 < :v1");
        qr.filterExpression("#2 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "Title");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("str_val_1").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("453.23").build());
        exprAttrVal.put(":v2", AttributeValue.builder().s("hello").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testScanIndexForward() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        qr.keyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("str_val_1").build());
        qr.expressionAttributeValues(exprAttrVal);
        qr.scanIndexForward(false);

        // query result, should return 3 items in descending order of sort key
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (int i=0; i<phoenixResult.items().size()-1;i++) {
            double sortKeyVal1 = Double.parseDouble(phoenixResult.items().get(i).get("attr_1").n());
            double sortKeyVal2 = Double.parseDouble(phoenixResult.items().get(i+1).get("attr_1").n());
            Assert.assertTrue(sortKeyVal1 >= sortKeyVal2);
        }

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testIndexBeginsWith() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_1",
                        ScalarAttributeType.N, null, null);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "Title", ScalarAttributeType.S);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        qr.keyConditionExpression("#0 = :v0 AND begins_with(#1, :v1)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "Title");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("str_val_1").build());
        exprAttrVal.put(":v1", AttributeValue.builder().s("he").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testIndexBetween() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        qr.keyConditionExpression("#0 = :v0 AND #1 BETWEEN :v1 AND :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "attr_1");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("str_val_1").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("0.0001").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("30.121").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testProjectionScanIndexForwardFilterLimit() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        qr.keyConditionExpression("#0 = :v0 AND #1 > :v1");
        qr.filterExpression("#2 > :v2");
        qr.projectionExpression("Title");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "num");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("str_val_1").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("0.0").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("1.01").build());
        qr.expressionAttributeValues(exprAttrVal);
        qr.scanIndexForward(false);
        qr.limit(1);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // check last Key
        Map<String, AttributeValue> lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals(getItem1(), lastKey);

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("11295.03").build());
        item.put("Title", AttributeValue.builder().s("hi").build());
        item.put("num", AttributeValue.builder().n("1.1").build());
        return item;
    }

    private Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("30.12").build());
        item.put("Title", AttributeValue.builder().s("hello").build());
        item.put("num", AttributeValue.builder().n("1.2").build());
        return item;
    }

    private Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("-21.06").build());
        item.put("Title", AttributeValue.builder().s("hey").build());
        item.put("num", AttributeValue.builder().n("1.3").build());
        return item;
    }

    private Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("0.1").build());
        item.put("Title", AttributeValue.builder().s("hie").build());
        item.put("num", AttributeValue.builder().n("-0.8").build());
        return item;
    }

}
