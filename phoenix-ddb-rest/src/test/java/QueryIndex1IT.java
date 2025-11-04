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
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests for basic index functionality without other QueryRequest parameters.
 */
public class QueryIndex1IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIndex1IT.class);

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
    public void testGlobalIndexNoSortKey1() throws SQLException {
        // create table with keys [attr_0]
        final String tableName = "_X__Error.Chain__99.Recursive_Trap";
        final String indexName = "_Y__Error.Chain__98.Recursive_Trap";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        // create index on IdS
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "IdS",
                ScalarAttributeType.S, null, null);
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
        qr.keyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "IdS");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("101.01").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals("101.01", lastKey.get("IdS").s());

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testGlobalIndexNoSortKey2() throws SQLException {
        // create table with keys [attr_0, attr_1]
        final String tableName = "_414-Unicorn.MemoryHole--..--_";
        final String indexName = "....." + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        // create index on IdS
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "IdS",
                ScalarAttributeType.S, null, null);
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
        qr.keyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "IdS");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("101.01").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals("101.01", lastKey.get("IdS").s());

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testGlobalIndexWithSortKey1() throws SQLException {
        // create table with keys [attr_0]
        final String tableName = testName.getMethodName();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        // create index on IdS, n_attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "IdS",
                ScalarAttributeType.S, "Id2", ScalarAttributeType.N);
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
        exprAttrNames.put("#0", "IdS");
        exprAttrNames.put("#1", "Id2");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("101.01").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("2.1").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals("101.01", lastKey.get("IdS").s());
        Assert.assertEquals(1.1, Double.parseDouble(lastKey.get("Id2").n()), 0);

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testGlobalIndexWithSortKey2() throws SQLException {
        // create table with keys [attr_0, attr_1]
        final String tableName = testName.getMethodName();
        final String indexName = "G_IDx_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        // create index on IdS, n_attr_1
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "IdS",
                ScalarAttributeType.S, "Id2", ScalarAttributeType.N);
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
        exprAttrNames.put("#0", "IdS");
        exprAttrNames.put("#1", "Id2");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("101.01").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("2.1").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals("101.01", lastKey.get("IdS").s());
        Assert.assertEquals(1.1, Double.parseDouble(lastKey.get("Id2").n()), 0);

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    @Test(timeout = 120000)
    public void testLocalIndex() throws SQLException {
        // create table with keys [attr_0, attr_1]
        final String tableName = testName.getMethodName();
        final String indexName = "l_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        // create index on attr_0, IdS
        createTableRequest = DDLTestUtils.addIndexToRequest(false, createTableRequest, indexName, "attr_0",
                ScalarAttributeType.S, "IdS", ScalarAttributeType.S);
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
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "IdS");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("str_val_1").build());
        exprAttrVal.put(":v1", AttributeValue.builder().s("101.02").build());
        qr.expressionAttributeValues(exprAttrVal);

        // query result
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.lastEvaluatedKey();
        Assert.assertEquals("str_val_1", lastKey.get("attr_0").s());
        Assert.assertEquals("101.01", lastKey.get("IdS").s());

        // explain plan
        TestUtils.validateIndexUsed(qr.build(), url);
    }

    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("str_val_1").build());
        item.put("attr_1", AttributeValue.builder().n("11295.03").build());
        item.put("IdS", AttributeValue.builder().s("101.01").build());
        item.put("Id2", AttributeValue.builder().n("1.1").build());
        return item;
    }

    private Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("str_val_2").build());
        item.put("attr_1", AttributeValue.builder().n("21295.03").build());
        item.put("IdS", AttributeValue.builder().s("202.02").build());
        item.put("Id2", AttributeValue.builder().n("2.2").build());
        return item;
    }

    private Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("str_val_3").build());
        item.put("attr_1", AttributeValue.builder().n("31295.03").build());
        item.put("IdS", AttributeValue.builder().s("303.03").build());
        item.put("Id2", AttributeValue.builder().n("3.3").build());
        return item;
    }

    private Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("str_val_4").build());
        item.put("attr_1", AttributeValue.builder().n("41295.03").build());
        item.put("IdS", AttributeValue.builder().s("404.04").build());
        item.put("Id2", AttributeValue.builder().n("4.4").build());
        return item;
    }
}
