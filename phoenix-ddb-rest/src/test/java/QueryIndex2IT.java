import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
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

/**
 * Tests for index functionality with other QueryRequest parameters -
 * lastEvaluatedKey, scanIndexForward, filterExpression, and limit.
 */
public class QueryIndex2IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIndex2IT.class);

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
    public void testPaginationWithLastKey() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName();
        final String indexName = "G_IdX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
        final String tableName = testName.getMethodName();
        final String indexName = "g_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
        final String tableName = testName.getMethodName();
        final String indexName = "G_iDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
        final String tableName = testName.getMethodName();
        final String indexName = "G_IDx_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_1",
                        ScalarAttributeType.N, null, null);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "Title", ScalarAttributeType.S);
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
        phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testIndexBetween() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName();
        final String indexName = "G_iDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
        final String tableName = testName.getMethodName();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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

    @Test(timeout = 120000)
    public void testQueryWithBeginsWithFilter() throws SQLException {
        final String tableName = testName.getMethodName();
        final String indexName = "idx.123_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title", ScalarAttributeType.S, "num",
                        ScalarAttributeType.N);
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("Title", AttributeValue.builder().s("item1").build());
        item1.put("num", AttributeValue.builder().n("1").build());
        item1.put("Attr_0", AttributeValue.builder().s("test_prefix").build());
        item1.put("attr_1", AttributeValue.builder().n("10").build());
        item1.put("str_attr", AttributeValue.builder().s("hello_world").build());
        item1.put("bin_attr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x01, 0x02, 0x03}))
                        .build());
        item1.put("category", AttributeValue.builder().s("A").build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("Title", AttributeValue.builder().s("item2").build());
        item2.put("num", AttributeValue.builder().n("2").build());
        item2.put("Attr_0", AttributeValue.builder().s("test_prefix").build());
        item2.put("attr_1", AttributeValue.builder().n("20").build());
        item2.put("str_attr", AttributeValue.builder().s("hello_universe").build());
        item2.put("bin_attr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x01, 0x02, 0x04}))
                        .build());
        item2.put("category", AttributeValue.builder().s("B").build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("Title", AttributeValue.builder().s("item3").build());
        item3.put("num", AttributeValue.builder().n("3").build());
        item3.put("Attr_0", AttributeValue.builder().s("test_prefix").build());
        item3.put("attr_1", AttributeValue.builder().n("30").build());
        item3.put("str_attr", AttributeValue.builder().s("goodbye_world").build());
        item3.put("bin_attr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x05, 0x06, 0x07}))
                        .build());
        item3.put("category", AttributeValue.builder().s("A").build());

        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put("Title", AttributeValue.builder().s("item4").build());
        item4.put("num", AttributeValue.builder().n("4").build());
        item4.put("Attr_0", AttributeValue.builder().s("test_prefix").build());
        item4.put("attr_1", AttributeValue.builder().n("40").build());
        item4.put("str_attr", AttributeValue.builder().s("hi_there").build());
        item4.put("bin_attr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x01, 0x03, 0x05}))
                        .build());
        item4.put("category", AttributeValue.builder().s("C").build());

        // Put items into both DynamoDB and Phoenix
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(item4).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        // Test 1: begins_with with String attribute using FilterExpression
        QueryRequest.Builder qr1 = QueryRequest.builder().tableName(tableName);
        qr1.indexName(indexName);
        qr1.keyConditionExpression("#0 = :v0");
        qr1.filterExpression("begins_with(#1, :v1)");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#0", "Attr_0");
        exprAttrNames1.put("#1", "str_attr");
        qr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal1.put(":v1", AttributeValue.builder().s("hello").build());
        qr1.expressionAttributeValues(exprAttrVal1);

        QueryResponse dynamoResult1 = dynamoDbClient.query(qr1.build());
        QueryResponse phoenixResult1 = phoenixDBClientV2.query(qr1.build());
        assertQueryResultsEqual(dynamoResult1, phoenixResult1, 2);

        // Test 2: begins_with with Binary attribute using FilterExpression
        QueryRequest.Builder qr2 = QueryRequest.builder().tableName(tableName);
        qr2.indexName(indexName);
        qr2.keyConditionExpression("#0 = :v0");
        qr2.filterExpression("begins_with(#1, :v1)");
        Map<String, String> exprAttrNames2 = new HashMap<>();
        exprAttrNames2.put("#0", "Attr_0");
        exprAttrNames2.put("#1", "bin_attr");
        qr2.expressionAttributeNames(exprAttrNames2);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal2.put(":v1",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x01, 0x02}))
                        .build());
        qr2.expressionAttributeValues(exprAttrVal2);

        QueryResponse dynamoResult2 = dynamoDbClient.query(qr2.build());
        QueryResponse phoenixResult2 = phoenixDBClientV2.query(qr2.build());
        assertQueryResultsEqual(dynamoResult2, phoenixResult2, 2);

        // Test 3: begins_with with String attribute - negative case (no matches)
        QueryRequest.Builder qr3 = QueryRequest.builder().tableName(tableName);
        qr3.indexName(indexName);
        qr3.keyConditionExpression("#0 = :v0");
        qr3.filterExpression("begins_with(#1, :v1)");
        Map<String, String> exprAttrNames3 = new HashMap<>();
        exprAttrNames3.put("#0", "Attr_0");
        exprAttrNames3.put("#1", "str_attr");
        qr3.expressionAttributeNames(exprAttrNames3);
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal3.put(":v1", AttributeValue.builder().s("xyz").build());
        qr3.expressionAttributeValues(exprAttrVal3);

        QueryResponse dynamoResult3 = dynamoDbClient.query(qr3.build());
        QueryResponse phoenixResult3 = phoenixDBClientV2.query(qr3.build());
        assertQueryResultsEqual(dynamoResult3, phoenixResult3, 0);

        // Test 4: begins_with combined with AND condition in FilterExpression
        QueryRequest.Builder qr4 = QueryRequest.builder().tableName(tableName);
        qr4.indexName(indexName);
        qr4.keyConditionExpression("#0 = :v0");
        qr4.filterExpression("begins_with(#1, :v1) AND #2 = :v2");
        Map<String, String> exprAttrNames4 = new HashMap<>();
        exprAttrNames4.put("#0", "Attr_0");
        exprAttrNames4.put("#1", "str_attr");
        exprAttrNames4.put("#2", "category");
        qr4.expressionAttributeNames(exprAttrNames4);
        Map<String, AttributeValue> exprAttrVal4 = new HashMap<>();
        exprAttrVal4.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal4.put(":v1", AttributeValue.builder().s("hello").build());
        exprAttrVal4.put(":v2", AttributeValue.builder().s("A").build());
        qr4.expressionAttributeValues(exprAttrVal4);

        QueryResponse dynamoResult4 = dynamoDbClient.query(qr4.build());
        QueryResponse phoenixResult4 = phoenixDBClientV2.query(qr4.build());
        assertQueryResultsEqual(dynamoResult4, phoenixResult4, 1);

        // Test 5: begins_with combined with OR condition in FilterExpression
        QueryRequest.Builder qr5 = QueryRequest.builder().tableName(tableName);
        qr5.indexName(indexName);
        qr5.keyConditionExpression("#0 = :v0");
        qr5.filterExpression("#2 = :v2  OR begins_with(#1, :v1 )");
        Map<String, String> exprAttrNames5 = new HashMap<>();
        exprAttrNames5.put("#0", "Attr_0");
        exprAttrNames5.put("#1", "str_attr");
        exprAttrNames5.put("#2", "category");
        qr5.expressionAttributeNames(exprAttrNames5);
        Map<String, AttributeValue> exprAttrVal5 = new HashMap<>();
        exprAttrVal5.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal5.put(":v1", AttributeValue.builder().s("goodbye").build());
        exprAttrVal5.put(":v2", AttributeValue.builder().s("C").build());
        qr5.expressionAttributeValues(exprAttrVal5);

        QueryResponse dynamoResult5 = dynamoDbClient.query(qr5.build());
        QueryResponse phoenixResult5 = phoenixDBClientV2.query(qr5.build());
        assertQueryResultsEqual(dynamoResult5, phoenixResult5, 2);

        // Test 6: begins_with with exact match (prefix equals entire string)
        QueryRequest.Builder qr6 = QueryRequest.builder().tableName(tableName);
        qr6.indexName(indexName);
        qr6.keyConditionExpression("#0 = :v0");
        qr6.filterExpression("begins_with(#1, :v1)");
        Map<String, String> exprAttrNames6 = new HashMap<>();
        exprAttrNames6.put("#0", "Attr_0");
        exprAttrNames6.put("#1", "str_attr");
        qr6.expressionAttributeNames(exprAttrNames6);
        Map<String, AttributeValue> exprAttrVal6 = new HashMap<>();
        exprAttrVal6.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal6.put(":v1", AttributeValue.builder().s("hello_world").build());
        qr6.expressionAttributeValues(exprAttrVal6);

        QueryResponse dynamoResult6 = dynamoDbClient.query(qr6.build());
        QueryResponse phoenixResult6 = phoenixDBClientV2.query(qr6.build());
        assertQueryResultsEqual(dynamoResult6, phoenixResult6, 1);

        // Test 7: begins_with with empty prefix (should match all)
        QueryRequest.Builder qr7 = QueryRequest.builder().tableName(tableName);
        qr7.indexName(indexName);
        qr7.keyConditionExpression("#0 = :v0");
        qr7.filterExpression("begins_with(#1, :v1)");
        Map<String, String> exprAttrNames7 = new HashMap<>();
        exprAttrNames7.put("#0", "Attr_0");
        exprAttrNames7.put("#1", "str_attr");
        qr7.expressionAttributeNames(exprAttrNames7);
        Map<String, AttributeValue> exprAttrVal7 = new HashMap<>();
        exprAttrVal7.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal7.put(":v1", AttributeValue.builder().s("").build());
        qr7.expressionAttributeValues(exprAttrVal7);

        QueryResponse dynamoResult7 = dynamoDbClient.query(qr7.build());
        QueryResponse phoenixResult7 = phoenixDBClientV2.query(qr7.build());
        assertQueryResultsEqual(dynamoResult7, phoenixResult7, 4);

        // Test 8: begins_with with binary data - more specific prefix
        QueryRequest.Builder qr8 = QueryRequest.builder().tableName(tableName);
        qr8.indexName(indexName);
        qr8.keyConditionExpression("#0 = :v0");
        qr8.filterExpression("begins_with ( #1, :v1)");
        Map<String, String> exprAttrNames8 = new HashMap<>();
        exprAttrNames8.put("#0", "Attr_0");
        exprAttrNames8.put("#1", "bin_attr");
        qr8.expressionAttributeNames(exprAttrNames8);
        Map<String, AttributeValue> exprAttrVal8 = new HashMap<>();
        exprAttrVal8.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal8.put(":v1",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x01, 0x02, 0x03}))
                        .build());
        qr8.expressionAttributeValues(exprAttrVal8);

        QueryResponse dynamoResult8 = dynamoDbClient.query(qr8.build());
        QueryResponse phoenixResult8 = phoenixDBClientV2.query(qr8.build());
        assertQueryResultsEqual(dynamoResult8, phoenixResult8, 1);

        // Test 9: begins_with for non-existent attribute with NOT operator
        QueryRequest.Builder qr9 = QueryRequest.builder().tableName(tableName);
        qr9.indexName(indexName);
        qr9.keyConditionExpression("#0 = :v0");
        qr9.filterExpression("NOT begins_with(#1, :v1)");
        Map<String, String> exprAttrNames9 = new HashMap<>();
        exprAttrNames9.put("#0", "Attr_0");
        exprAttrNames9.put("#1", "non_existent_attr");
        qr9.expressionAttributeNames(exprAttrNames9);
        Map<String, AttributeValue> exprAttrVal9 = new HashMap<>();
        exprAttrVal9.put(":v0", AttributeValue.builder().s("test_prefix").build());
        exprAttrVal9.put(":v1", AttributeValue.builder().s("any").build());
        qr9.expressionAttributeValues(exprAttrVal9);

        QueryResponse dynamoResult9 = dynamoDbClient.query(qr9.build());
        QueryResponse phoenixResult9 = phoenixDBClientV2.query(qr9.build());
        assertQueryResultsEqual(dynamoResult9, phoenixResult9, 4);

        // Test 10: Query main table (without index) with keyConditionExpression on hash+sort
        // key and begins_with in filterExpression
        QueryRequest.Builder qr10 = QueryRequest.builder().tableName(tableName);
        // No indexName - query the main table directly
        qr10.keyConditionExpression("#title = :title AND #num >= :num");
        qr10.filterExpression("begins_with(#str, :str_prefix)");
        Map<String, String> exprAttrNames10 = new HashMap<>();
        exprAttrNames10.put("#title", "Title");
        exprAttrNames10.put("#num", "num");
        exprAttrNames10.put("#str", "str_attr");
        qr10.expressionAttributeNames(exprAttrNames10);
        Map<String, AttributeValue> exprAttrVal10 = new HashMap<>();
        exprAttrVal10.put(":title", AttributeValue.builder().s("item1").build());
        exprAttrVal10.put(":num", AttributeValue.builder().n("1").build());
        exprAttrVal10.put(":str_prefix", AttributeValue.builder().s("hello").build());
        qr10.expressionAttributeValues(exprAttrVal10);

        QueryResponse dynamoResult10 = dynamoDbClient.query(qr10.build());
        QueryResponse phoenixResult10 = phoenixDBClientV2.query(qr10.build());
        assertQueryResultsEqual(dynamoResult10, phoenixResult10, 1);

        // Test 11: Query main table with BETWEEN operator in keyConditionExpression and
        // begins_with in filterExpression
        QueryRequest.Builder qr11 = QueryRequest.builder().tableName(tableName);
        // No indexName - query the main table directly
        qr11.keyConditionExpression("#title = :title AND #num BETWEEN :num1 AND :num2");
        qr11.filterExpression("begins_with(#str, :str_prefix)");
        Map<String, String> exprAttrNames11 = new HashMap<>();
        exprAttrNames11.put("#title", "Title");
        exprAttrNames11.put("#num", "num");
        exprAttrNames11.put("#str", "str_attr");
        qr11.expressionAttributeNames(exprAttrNames11);
        Map<String, AttributeValue> exprAttrVal11 = new HashMap<>();
        exprAttrVal11.put(":title", AttributeValue.builder().s("item2").build());
        exprAttrVal11.put(":num1", AttributeValue.builder().n("1").build());
        exprAttrVal11.put(":num2", AttributeValue.builder().n("3").build());
        exprAttrVal11.put(":str_prefix", AttributeValue.builder().s("hello").build());
        qr11.expressionAttributeValues(exprAttrVal11);

        QueryResponse dynamoResult11 = dynamoDbClient.query(qr11.build());
        QueryResponse phoenixResult11 = phoenixDBClientV2.query(qr11.build());
        assertQueryResultsEqual(dynamoResult11, phoenixResult11, 1);

        // Test 12: Query main table that returns 0 items due to begins_with filter
        QueryRequest.Builder qr12 = QueryRequest.builder().tableName(tableName);
        qr12.keyConditionExpression("#title = :title AND #num >= :num");
        qr12.filterExpression("begins_with(#str, :str_prefix)");
        Map<String, String> exprAttrNames12 = new HashMap<>();
        exprAttrNames12.put("#title", "Title");
        exprAttrNames12.put("#num", "num");
        exprAttrNames12.put("#str", "str_attr");
        qr12.expressionAttributeNames(exprAttrNames12);
        Map<String, AttributeValue> exprAttrVal12 = new HashMap<>();
        exprAttrVal12.put(":title", AttributeValue.builder().s("item3").build());
        exprAttrVal12.put(":num", AttributeValue.builder().n("1").build());
        exprAttrVal12.put(":str_prefix", AttributeValue.builder().s("hello").build());
        qr12.expressionAttributeValues(exprAttrVal12);

        QueryResponse dynamoResult12 = dynamoDbClient.query(qr12.build());
        QueryResponse phoenixResult12 = phoenixDBClientV2.query(qr12.build());
        assertQueryResultsEqual(dynamoResult12, phoenixResult12, 0);

        // Test 13: begins_with with Set data type - should throw 400 error
        Map<String, AttributeValue> itemWithSet = new HashMap<>();
        itemWithSet.put("Title", AttributeValue.builder().s("item_with_set").build());
        itemWithSet.put("num", AttributeValue.builder().n("5").build());
        itemWithSet.put("Attr_0", AttributeValue.builder().s("test_prefix").build());
        itemWithSet.put("attr_1", AttributeValue.builder().n("50").build());
        itemWithSet.put("string_set",
                AttributeValue.builder().ss("value1", "value2", "value3").build());

        PutItemRequest putItemWithSet =
                PutItemRequest.builder().tableName(tableName).item(itemWithSet).build();
        phoenixDBClientV2.putItem(putItemWithSet);
        dynamoDbClient.putItem(putItemWithSet);

        QueryRequest.Builder qr13 = QueryRequest.builder().tableName(tableName);
        qr13.keyConditionExpression("#title = :title");
        qr13.filterExpression("begins_with(#set_attr, :set_prefix)");
        Map<String, String> exprAttrNames13 = new HashMap<>();
        exprAttrNames13.put("#title", "Title");
        exprAttrNames13.put("#set_attr", "string_set");
        qr13.expressionAttributeNames(exprAttrNames13);
        Map<String, AttributeValue> exprAttrVal13 = new HashMap<>();
        exprAttrVal13.put(":title", AttributeValue.builder().s("item_with_set").build());
        exprAttrVal13.put(":set_prefix", AttributeValue.builder().ss("value").build());
        qr13.expressionAttributeValues(exprAttrVal13);

        try {
            dynamoDbClient.query(qr13.build());
            throw new RuntimeException("Query should fail");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.query(qr13.build());
            throw new RuntimeException("Query should fail");
        } catch (DynamoDbException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        // Test 14: Query main table that returns 0 items due to begins_with filter
        QueryRequest.Builder qr14 = QueryRequest.builder().tableName(tableName);
        qr14.keyConditionExpression("#title = :title AND #num >= :num");
        qr14.filterExpression("begins_with(#str, :b_prefix)");
        Map<String, String> exprAttrNames14 = new HashMap<>();
        exprAttrNames14.put("#title", "Title");
        exprAttrNames14.put("#num", "num");
        exprAttrNames14.put("#str", "str_attr");
        qr14.expressionAttributeNames(exprAttrNames14);
        Map<String, AttributeValue> exprAttrVal14 = new HashMap<>();
        exprAttrVal14.put(":title", AttributeValue.builder().s("item3").build());
        exprAttrVal14.put(":num", AttributeValue.builder().n("1").build());
        exprAttrVal14.put(":b_prefix",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0x01}))
                        .build());
        qr14.expressionAttributeValues(exprAttrVal14);

        QueryResponse dynamoResult14 = dynamoDbClient.query(qr14.build());
        QueryResponse phoenixResult14 = phoenixDBClientV2.query(qr14.build());
        assertQueryResultsEqual(dynamoResult14, phoenixResult14, 0);

        // explain plan for index test
        TestUtils.validateIndexUsed(qr1.build(), url);
    }

    @Test(timeout = 120000)
    public void testQueryWithContainsFilter() throws SQLException {
        final String tableName = testName.getMethodName();
        final String indexName = "idx_contains_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.N);
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "category",
                        ScalarAttributeType.S, "num_attr", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        putItemsForQueryWithContainsFilter(tableName);

        // Test 1: contains() with String attribute - substring search
        QueryRequest.Builder qr1 = QueryRequest.builder().tableName(tableName);
        qr1.indexName(indexName);
        qr1.keyConditionExpression("#cat = :cat");
        qr1.filterExpression("contains(#strAttr, :substring)");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#cat", "category");
        exprAttrNames1.put("#strAttr", "stringAttr");
        qr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal1.put(":substring", AttributeValue.builder().s("world").build());
        qr1.expressionAttributeValues(exprAttrVal1);

        executeAndAssertQuery(qr1.build(), 1);

        // Test 2: contains() with String Set - element search
        QueryRequest.Builder qr2 = QueryRequest.builder().tableName(tableName);
        qr2.indexName(indexName);
        qr2.keyConditionExpression("#cat = :cat");
        qr2.filterExpression("contains(stringSet, :element)");
        Map<String, String> exprAttrNames2 = new HashMap<>();
        exprAttrNames2.put("#cat", "category");
        qr2.expressionAttributeNames(exprAttrNames2);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal2.put(":element", AttributeValue.builder().s("apple").build());
        qr2.expressionAttributeValues(exprAttrVal2);

        executeAndAssertQuery(qr2.build(), 1);

        // Test 3: contains() with Number Set - element search
        QueryRequest.Builder qr3 = QueryRequest.builder().tableName(tableName);
        qr3.indexName(indexName);
        qr3.keyConditionExpression("#cat = :cat");
        qr3.filterExpression("contains(numberSet, :number)");
        Map<String, String> exprAttrNames3 = new HashMap<>();
        exprAttrNames3.put("#cat", "category");
        qr3.expressionAttributeNames(exprAttrNames3);
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":cat", AttributeValue.builder().s("books").build());
        exprAttrVal3.put(":number", AttributeValue.builder().n("5").build());
        qr3.expressionAttributeValues(exprAttrVal3);

        QueryResponse phoenixResult3 = phoenixDBClientV2.query(qr3.build());
        QueryResponse dynamoResult3 = dynamoDbClient.query(qr3.build());

        assertQueryResultsEqual(dynamoResult3, phoenixResult3, 1);

        // Test 4: contains() with Binary Set - element search
        QueryRequest.Builder qr4 = QueryRequest.builder().tableName(tableName);
        qr4.indexName(indexName);
        qr4.keyConditionExpression("#cat = :cat");
        qr4.filterExpression("contains(binarySet, :binaryElement)");
        Map<String, String> exprAttrNames4 = new HashMap<>();
        exprAttrNames4.put("#cat", "category");
        qr4.expressionAttributeNames(exprAttrNames4);
        Map<String, AttributeValue> exprAttrVal4 = new HashMap<>();
        exprAttrVal4.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal4.put(":binaryElement",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        qr4.expressionAttributeValues(exprAttrVal4);

        QueryResponse phoenixResult4 = phoenixDBClientV2.query(qr4.build());
        QueryResponse dynamoResult4 = dynamoDbClient.query(qr4.build());

        assertQueryResultsEqual(dynamoResult4, phoenixResult4, 1);

        // Test 5: contains() with List attribute - element search (String)
        QueryRequest.Builder qr5 = QueryRequest.builder().tableName(tableName);
        qr5.indexName(indexName);
        qr5.keyConditionExpression("#cat = :cat");
        qr5.filterExpression("contains(listAttr, :listElement)");
        Map<String, String> exprAttrNames5 = new HashMap<>();
        exprAttrNames5.put("#cat", "category");
        qr5.expressionAttributeNames(exprAttrNames5);
        Map<String, AttributeValue> exprAttrVal5 = new HashMap<>();
        exprAttrVal5.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal5.put(":listElement", AttributeValue.builder().s("red").build());
        qr5.expressionAttributeValues(exprAttrVal5);

        QueryResponse phoenixResult5 = phoenixDBClientV2.query(qr5.build());
        QueryResponse dynamoResult5 = dynamoDbClient.query(qr5.build());

        assertQueryResultsEqual(dynamoResult5, phoenixResult5, 1);

        // Test 6: contains() with List attribute - element search (Number)
        QueryRequest.Builder qr6 = QueryRequest.builder().tableName(tableName);
        qr6.indexName(indexName);
        qr6.keyConditionExpression("#cat = :cat");
        qr6.filterExpression("contains(listAttr, :listElement)");
        Map<String, String> exprAttrNames6 = new HashMap<>();
        exprAttrNames6.put("#cat", "category");
        qr6.expressionAttributeNames(exprAttrNames6);
        Map<String, AttributeValue> exprAttrVal6 = new HashMap<>();
        exprAttrVal6.put(":cat", AttributeValue.builder().s("books").build());
        exprAttrVal6.put(":listElement", AttributeValue.builder().n("42").build());
        qr6.expressionAttributeValues(exprAttrVal6);

        QueryResponse phoenixResult6 = phoenixDBClientV2.query(qr6.build());
        QueryResponse dynamoResult6 = dynamoDbClient.query(qr6.build());

        assertQueryResultsEqual(dynamoResult6, phoenixResult6, 1);

        // Test 7: contains() combined with AND condition
        QueryRequest.Builder qr7 = QueryRequest.builder().tableName(tableName);
        qr7.indexName(indexName);
        qr7.keyConditionExpression("#cat = :cat");
        qr7.filterExpression("contains(stringAttr, :substring) AND contains(stringSet, :element)");
        Map<String, String> exprAttrNames7 = new HashMap<>();
        exprAttrNames7.put("#cat", "category");
        qr7.expressionAttributeNames(exprAttrNames7);
        Map<String, AttributeValue> exprAttrVal7 = new HashMap<>();
        exprAttrVal7.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal7.put(":substring", AttributeValue.builder().s("world").build());
        exprAttrVal7.put(":element", AttributeValue.builder().s("apple").build());
        qr7.expressionAttributeValues(exprAttrVal7);

        QueryResponse phoenixResult7 = phoenixDBClientV2.query(qr7.build());
        QueryResponse dynamoResult7 = dynamoDbClient.query(qr7.build());

        assertQueryResultsEqual(dynamoResult7, phoenixResult7, 1);

        // Test 8: contains() with OR condition
        QueryRequest.Builder qr8 = QueryRequest.builder().tableName(tableName);
        qr8.indexName(indexName);
        qr8.keyConditionExpression("#cat = :cat");
        qr8.filterExpression("contains(stringSet, :fruit1) OR contains(stringSet, :fruit2)");
        Map<String, String> exprAttrNames8 = new HashMap<>();
        exprAttrNames8.put("#cat", "category");
        qr8.expressionAttributeNames(exprAttrNames8);
        Map<String, AttributeValue> exprAttrVal8 = new HashMap<>();
        exprAttrVal8.put(":cat", AttributeValue.builder().s("books").build());
        exprAttrVal8.put(":fruit1", AttributeValue.builder().s("orange").build());
        exprAttrVal8.put(":fruit2", AttributeValue.builder().s("grape").build());
        qr8.expressionAttributeValues(exprAttrVal8);

        QueryResponse phoenixResult8 = phoenixDBClientV2.query(qr8.build());
        QueryResponse dynamoResult8 = dynamoDbClient.query(qr8.build());

        assertQueryResultsEqual(dynamoResult8, phoenixResult8, 1);

        // Test 9: contains() with NOT operator
        QueryRequest.Builder qr9 = QueryRequest.builder().tableName(tableName);
        qr9.indexName(indexName);
        qr9.keyConditionExpression("#cat = :cat");
        qr9.filterExpression("NOT contains(stringAttr, :substring)");
        Map<String, String> exprAttrNames9 = new HashMap<>();
        exprAttrNames9.put("#cat", "category");
        qr9.expressionAttributeNames(exprAttrNames9);
        Map<String, AttributeValue> exprAttrVal9 = new HashMap<>();
        exprAttrVal9.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal9.put(":substring", AttributeValue.builder().s("world").build());
        qr9.expressionAttributeValues(exprAttrVal9);

        QueryResponse phoenixResult9 = phoenixDBClientV2.query(qr9.build());
        QueryResponse dynamoResult9 = dynamoDbClient.query(qr9.build());

        assertQueryResultsEqual(dynamoResult9, phoenixResult9, 1);

        // Test 10: contains() with empty string (should match all strings)
        QueryRequest.Builder qr10 = QueryRequest.builder().tableName(tableName);
        qr10.indexName(indexName);
        qr10.keyConditionExpression("#cat = :cat");
        qr10.filterExpression("contains(stringAttr, :emptyString)");
        Map<String, String> exprAttrNames10 = new HashMap<>();
        exprAttrNames10.put("#cat", "category");
        qr10.expressionAttributeNames(exprAttrNames10);
        Map<String, AttributeValue> exprAttrVal10 = new HashMap<>();
        exprAttrVal10.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal10.put(":emptyString", AttributeValue.builder().s("").build());
        qr10.expressionAttributeValues(exprAttrVal10);

        QueryResponse phoenixResult10 = phoenixDBClientV2.query(qr10.build());
        QueryResponse dynamoResult10 = dynamoDbClient.query(qr10.build());

        assertQueryResultsEqual(dynamoResult10, phoenixResult10, 2);

        // Test 11: contains() for non-existent attribute
        QueryRequest.Builder qr11 = QueryRequest.builder().tableName(tableName);
        qr11.indexName(indexName);
        qr11.keyConditionExpression("#cat = :cat");
        qr11.filterExpression("NOT contains(nonExistentAttr, :value)");
        Map<String, String> exprAttrNames11 = new HashMap<>();
        exprAttrNames11.put("#cat", "category");
        qr11.expressionAttributeNames(exprAttrNames11);
        Map<String, AttributeValue> exprAttrVal11 = new HashMap<>();
        exprAttrVal11.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal11.put(":value", AttributeValue.builder().s("test").build());
        qr11.expressionAttributeValues(exprAttrVal11);

        QueryResponse phoenixResult11 = phoenixDBClientV2.query(qr11.build());
        QueryResponse dynamoResult11 = dynamoDbClient.query(qr11.build());

        assertQueryResultsEqual(dynamoResult11, phoenixResult11, 2);

        // explain plan for index test
        TestUtils.validateIndexUsed(qr1.build(), url);
    }

    @Test(timeout = 120000)
    public void testQueryWithSizeFilter() throws SQLException {
        final String tableName = testName.getMethodName();
        final String indexName = "idx_size_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.N);
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "category",
                        ScalarAttributeType.S, "num_attr", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        putItemsForQueryWithSizeFilter(tableName);

        // Test 1: size() with String attribute - find strings longer than 5 characters
        QueryRequest.Builder qr1 = QueryRequest.builder().tableName(tableName);
        qr1.indexName(indexName);
        qr1.keyConditionExpression("#cat = :cat");
        qr1.filterExpression("size(#strAttr) > :size");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#cat", "category");
        exprAttrNames1.put("#strAttr", "description");
        qr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":cat", AttributeValue.builder().s("tech").build());
        exprAttrVal1.put(":size", AttributeValue.builder().n("5").build());
        qr1.expressionAttributeValues(exprAttrVal1);

        executeAndAssertQuery(qr1.build(), 1);

        // Test 2: size() with List attribute - find lists with exactly 2 elements
        QueryRequest.Builder qr2 = QueryRequest.builder().tableName(tableName);
        qr2.indexName(indexName);
        qr2.keyConditionExpression("#cat = :cat");
        qr2.filterExpression("size(tags) = :listSize");
        Map<String, String> exprAttrNames2 = new HashMap<>();
        exprAttrNames2.put("#cat", "category");
        qr2.expressionAttributeNames(exprAttrNames2);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":cat", AttributeValue.builder().s("books").build());
        exprAttrVal2.put(":listSize", AttributeValue.builder().n("2").build());
        qr2.expressionAttributeValues(exprAttrVal2);

        QueryResponse phoenixResult2 = phoenixDBClientV2.query(qr2.build());
        QueryResponse dynamoResult2 = dynamoDbClient.query(qr2.build());

        assertQueryResultsEqual(dynamoResult2, phoenixResult2, 1);

        // Test 3: size() with String Set - find sets with at least 3 elements
        QueryRequest.Builder qr3 = QueryRequest.builder().tableName(tableName);
        qr3.indexName(indexName);
        qr3.keyConditionExpression("#cat = :cat");
        qr3.filterExpression("size(colors) >= :setSize");
        Map<String, String> exprAttrNames3 = new HashMap<>();
        exprAttrNames3.put("#cat", "category");
        qr3.expressionAttributeNames(exprAttrNames3);
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":cat", AttributeValue.builder().s("tech").build());
        exprAttrVal3.put(":setSize", AttributeValue.builder().n("3").build());
        qr3.expressionAttributeValues(exprAttrVal3);

        QueryResponse phoenixResult3 = phoenixDBClientV2.query(qr3.build());
        QueryResponse dynamoResult3 = dynamoDbClient.query(qr3.build());

        assertQueryResultsEqual(dynamoResult3, phoenixResult3, 2);

        // explain plan for index test
        TestUtils.validateIndexUsed(qr1.build(), url);
    }

    @Test(timeout = 120000)
    public void testQueryWithAttributeTypeFilter() throws SQLException {
        final String tableName = testName.getMethodName();
        final String indexName = "idx_attr_type_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.N);
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "category",
                        ScalarAttributeType.S, "num_attr", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        putItemsForQueryWithAttributeTypeFilter(tableName);

        // Test 1: attribute_type() for String attribute - find items with String titles
        QueryRequest.Builder qr1 = QueryRequest.builder().tableName(tableName);
        qr1.indexName(indexName);
        qr1.keyConditionExpression("#cat = :cat");
        qr1.filterExpression("attribute_type(title, :stringType)");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#cat", "category");
        qr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal1.put(":stringType", AttributeValue.builder().s("S").build());
        qr1.expressionAttributeValues(exprAttrVal1);

        executeAndAssertQuery(qr1.build(), 2);

        // Test 2: attribute_type() for Number attribute - find items with Number prices
        QueryRequest.Builder qr2 = QueryRequest.builder().tableName(tableName);
        qr2.indexName(indexName);
        qr2.keyConditionExpression("#cat = :cat");
        qr2.filterExpression("attribute_type(price, :numberType)");
        Map<String, String> exprAttrNames2 = new HashMap<>();
        exprAttrNames2.put("#cat", "category");
        qr2.expressionAttributeNames(exprAttrNames2);
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":cat", AttributeValue.builder().s("books").build());
        exprAttrVal2.put(":numberType", AttributeValue.builder().s("N").build());
        qr2.expressionAttributeValues(exprAttrVal2);

        QueryResponse phoenixResult2 = phoenixDBClientV2.query(qr2.build());
        QueryResponse dynamoResult2 = dynamoDbClient.query(qr2.build());

        assertQueryResultsEqual(dynamoResult2, phoenixResult2, 1);

        // Test 3: attribute_type() for List attribute - find items with List types
        QueryRequest.Builder qr3 = QueryRequest.builder().tableName(tableName);
        qr3.indexName(indexName);
        qr3.keyConditionExpression("#cat = :cat");
        qr3.filterExpression("attribute_type(features, :listType)");
        Map<String, String> exprAttrNames3 = new HashMap<>();
        exprAttrNames3.put("#cat", "category");
        qr3.expressionAttributeNames(exprAttrNames3);
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":cat", AttributeValue.builder().s("electronics").build());
        exprAttrVal3.put(":listType", AttributeValue.builder().s("L").build());
        qr3.expressionAttributeValues(exprAttrVal3);

        executeAndAssertQuery(qr3.build(), 2);

        // explain plan for index test
        TestUtils.validateIndexUsed(qr1.build(), url);
    }

    private void putItemsForQueryWithContainsFilter(String tableName) {
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("item1").build());
        item1.put("sk", AttributeValue.builder().n("1").build());
        item1.put("stringAttr", AttributeValue.builder().s("hello world test").build());
        item1.put("stringSet", AttributeValue.builder().ss("apple", "banana", "cherry").build());
        item1.put("numberSet", AttributeValue.builder().ns("1", "2", "3").build());
        item1.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {1, 2}),
                        SdkBytes.fromByteArray(new byte[] {3, 4})).build());
        item1.put("listAttr", AttributeValue.builder().l(AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("green").build()).build());
        item1.put("category", AttributeValue.builder().s("electronics").build());
        item1.put("num_attr", AttributeValue.builder().n("100").build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("item2").build());
        item2.put("sk", AttributeValue.builder().n("2").build());
        item2.put("stringAttr", AttributeValue.builder().s("testing contains function").build());
        item2.put("stringSet", AttributeValue.builder().ss("orange", "grape", "kiwi").build());
        item2.put("numberSet", AttributeValue.builder().ns("4", "5", "6").build());
        item2.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {7, 8})).build());
        item2.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("black").build(),
                        AttributeValue.builder().n("42").build()).build());
        item2.put("category", AttributeValue.builder().s("books").build());
        item2.put("num_attr", AttributeValue.builder().n("200").build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("item3").build());
        item3.put("sk", AttributeValue.builder().n("3").build());
        item3.put("stringAttr", AttributeValue.builder().s("sample data").build());
        item3.put("stringSet", AttributeValue.builder().ss("mango", "watermelon").build());
        item3.put("numberSet", AttributeValue.builder().ns("7", "8", "9").build());
        item3.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {9, 10})).build());
        item3.put("listAttr", AttributeValue.builder().l(AttributeValue.builder().s("blue").build(),
                AttributeValue.builder().s("yellow").build()).build());
        item3.put("category", AttributeValue.builder().s("electronics").build());
        item3.put("num_attr", AttributeValue.builder().n("300").build());

        // Put items into both DynamoDB and Phoenix
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
    }

    private void putItemsForQueryWithSizeFilter(String tableName) {
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("laptop").build());
        item1.put("sk", AttributeValue.builder().n("1").build());
        item1.put("description", AttributeValue.builder().s("Gaming laptop").build());
        item1.put("tags", AttributeValue.builder().l(
                AttributeValue.builder().s("gaming").build(),
                AttributeValue.builder().s("laptop").build(),
                AttributeValue.builder().s("high-performance").build()).build());
        item1.put("colors", AttributeValue.builder().ss("black", "silver", "red", "blue").build());
        item1.put("category", AttributeValue.builder().s("tech").build());
        item1.put("num_attr", AttributeValue.builder().n("100").build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("novel").build());
        item2.put("sk", AttributeValue.builder().n("2").build());
        item2.put("description", AttributeValue.builder().s("Sci-fi").build());
        item2.put("tags", AttributeValue.builder().l(
                AttributeValue.builder().s("fiction").build(),
                AttributeValue.builder().s("bestseller").build()).build());
        item2.put("colors", AttributeValue.builder().ss("white", "blue").build());
        item2.put("category", AttributeValue.builder().s("books").build());
        item2.put("num_attr", AttributeValue.builder().n("200").build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("tablet").build());
        item3.put("sk", AttributeValue.builder().n("3").build());
        item3.put("description", AttributeValue.builder().s("iPad").build());
        item3.put("tags", AttributeValue.builder().l(
                AttributeValue.builder().s("tablet").build(),
                AttributeValue.builder().s("apple").build(),
                AttributeValue.builder().s("premium").build()).build());
        item3.put("colors", AttributeValue.builder().ss("gold", "space-gray", "silver").build());
        item3.put("category", AttributeValue.builder().s("tech").build());
        item3.put("num_attr", AttributeValue.builder().n("300").build());

        // Put items into both DynamoDB and Phoenix
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
    }

    private void putItemsForQueryWithAttributeTypeFilter(String tableName) {
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("smartphone").build());
        item1.put("sk", AttributeValue.builder().n("1").build());
        item1.put("title", AttributeValue.builder().s("iPhone 14").build());
        item1.put("price", AttributeValue.builder().n("999.99").build());
        item1.put("features", AttributeValue.builder().l(
                AttributeValue.builder().s("5G").build(),
                AttributeValue.builder().s("camera").build()).build());
        item1.put("category", AttributeValue.builder().s("electronics").build());
        item1.put("num_attr", AttributeValue.builder().n("100").build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("textbook").build());
        item2.put("sk", AttributeValue.builder().n("2").build());
        item2.put("title", AttributeValue.builder().s("Java Programming").build());
        item2.put("price", AttributeValue.builder().n("59.99").build());
        item2.put("features", AttributeValue.builder().l(
                AttributeValue.builder().s("examples").build(),
                AttributeValue.builder().s("exercises").build()).build());
        item2.put("category", AttributeValue.builder().s("books").build());
        item2.put("num_attr", AttributeValue.builder().n("200").build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("headphones").build());
        item3.put("sk", AttributeValue.builder().n("3").build());
        item3.put("title", AttributeValue.builder().s("AirPods Pro").build());
        item3.put("price", AttributeValue.builder().n("249.99").build());
        item3.put("features", AttributeValue.builder().l(
                AttributeValue.builder().s("noise-cancelling").build(),
                AttributeValue.builder().s("wireless").build(),
                AttributeValue.builder().s("premium").build()).build());
        item3.put("category", AttributeValue.builder().s("electronics").build());
        item3.put("num_attr", AttributeValue.builder().n("300").build());

        // Put items into both DynamoDB and Phoenix
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
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

    /**
     * Helper method to consolidate repetitive assertions for comparing query results
     * between DynamoDB and Phoenix.
     *
     * @param dynamoResult The query result from DynamoDB
     * @param phoenixResult The query result from Phoenix
     * @param expectedCount Optional expected count (can be null to skip count assertion)
     */
    private void assertQueryResultsEqual(QueryResponse dynamoResult, QueryResponse phoenixResult,
            Integer expectedCount) {
        if (expectedCount != null) {
            Assert.assertEquals(expectedCount.intValue(), dynamoResult.count().intValue());
        }
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(dynamoResult.items(), phoenixResult.items()));
    }

    /**
     * Helper method that executes queries on both DynamoDB and Phoenix clients and
     * then performs the standard assertions.
     *
     * @param queryRequest  The query request to execute
     * @param expectedCount Optional expected count (can be null to skip count assertion)
     */
    private void executeAndAssertQuery(QueryRequest queryRequest, Integer expectedCount) {
        QueryResponse phoenixResult = phoenixDBClientV2.query(queryRequest);
        QueryResponse dynamoResult = dynamoDbClient.query(queryRequest);
        assertQueryResultsEqual(dynamoResult, phoenixResult, expectedCount);
    }

}
