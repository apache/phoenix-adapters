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
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ScanIndexIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanIndexIT.class);

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
    public void testScanIndexOnlyHashKey() throws SQLException {
        //create table
        final String tableName = "X_x-X.xFRACTAL_.void-0mega__99xX-x_X";
        final String indexName = "G_IDx123-345aoe.jh";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "DataGrave-_-Obscure_Dream_.404",
                        ScalarAttributeType.S, ".-_.-_AnOtHeR_We1rD-Attr_9_9_9_._.-_.-",
                        ScalarAttributeType.N);

        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "title",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.indexName(indexName);
        sr.filterExpression("#0 BETWEEN :v1 AND :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "title");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":v1", AttributeValue.builder().s("Title1").build());
        exprAttrVals.put(":v2", AttributeValue.builder().s("Title4").build());
        sr.expressionAttributeValues(exprAttrVals);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        // dynamo does not guarantee ordering of partition keys in Scan, so only check count
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());

        // explain plan
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 120000)
    public void testScanIndexBothKeys() throws SQLException {
        //create table
        final String tableName = ".no_Signal--FuzZy_Matrix_XOX";
        final String indexName = "..no_Signal--FuzZy_Matrix_XOX";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "DataGrave-_-Obscure_Dream_.404",
                        ScalarAttributeType.S, ".-_.-_AnOtHeR_We1rD-Attr_9_9_9_._.-_.-",
                        ScalarAttributeType.N);

        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "title",
                        ScalarAttributeType.S, "Id2", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.indexName(indexName);
        sr.filterExpression("#1 > :v3 AND #0 BETWEEN :v1 AND :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "title");
        exprAttrNames.put("#1", "Id2");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":v1", AttributeValue.builder().s("Title1").build());
        exprAttrVals.put(":v2", AttributeValue.builder().s("Title4").build());
        exprAttrVals.put(":v3", AttributeValue.builder().n("150.09").build());
        sr.expressionAttributeValues(exprAttrVals);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        // dynamo does not guarantee ordering of partition keys in Scan, so only check count
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());

        // explain plan
        TestUtils.validateIndexUsed(sr.build(), url, "FULL SCAN ");
    }

    @Test(timeout = 120000)
    public void testScanIndexWithPagination() throws SQLException {
        //create table
        final String tableName = testName.getMethodName();
        final String indexName = "g_IDX" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "DataGrave-_-Obscure_Dream_.404",
                        ScalarAttributeType.S, ".-_.-_AnOtHeR_We1rD-Attr_9_9_9_._.-_.-",
                        ScalarAttributeType.N);

        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "title",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.indexName(indexName);
        sr.filterExpression("#0 BETWEEN :v1 AND :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "title");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":v1", AttributeValue.builder().s("Title1").build());
        exprAttrVals.put(":v2", AttributeValue.builder().s("Title3").build());
        sr.expressionAttributeValues(exprAttrVals);
        sr.limit(1);
        ScanResponse phoenixResult, dynamoResult;
        int paginationCount = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            dynamoResult = dynamoDbClient.scan(sr.build());
            Assert.assertEquals(dynamoResult.items(), phoenixResult.items());
            paginationCount++;
            TestUtils.validateIndexUsed(sr.build(), url,
                    paginationCount == 1 ? "FULL SCAN " : "RANGE SCAN ");
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        // 1 more than total number of results expected
        Assert.assertEquals(4, paginationCount);
    }

    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DataGrave-_-Obscure_Dream_.404", AttributeValue.builder().s("A").build());
        item.put(".-_.-_AnOtHeR_We1rD-Attr_9_9_9_._.-_.-", AttributeValue.builder().n("1").build());
        item.put("Id1", AttributeValue.builder().n("-5").build());
        item.put("Id2", AttributeValue.builder().n("10.10").build());
        item.put("title", AttributeValue.builder().s("Title1").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Alice").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar",
                AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DataGrave-_-Obscure_Dream_.404", AttributeValue.builder().s("B").build());
        item.put(".-_.-_AnOtHeR_We1rD-Attr_9_9_9_._.-_.-", AttributeValue.builder().n("2").build());
        item.put("Id1", AttributeValue.builder().n("-15").build());
        item.put("Id2", AttributeValue.builder().n("150.10").build());
        item.put("title", AttributeValue.builder().s("Title2").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Bob1").build());
        Map<String, AttributeValue> reviewMap2 = new HashMap<>();
        reviewMap2.put("reviewer", AttributeValue.builder().s("Bob2").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder()
                .l(AttributeValue.builder().m(reviewMap1).build(),
                        AttributeValue.builder().m(reviewMap2).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DataGrave-_-Obscure_Dream_.404", AttributeValue.builder().s("C").build());
        item.put(".-_.-_AnOtHeR_We1rD-Attr_9_9_9_._.-_.-", AttributeValue.builder().n("3").build());
        item.put("Id1", AttributeValue.builder().n("11").build());
        item.put("Id2", AttributeValue.builder().n("1000.10").build());
        item.put("title", AttributeValue.builder().s("Title3").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Carl").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar",
                AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("DataGrave-_-Obscure_Dream_.404", AttributeValue.builder().s("D").build());
        item.put(".-_.-_AnOtHeR_We1rD-Attr_9_9_9_._.-_.-", AttributeValue.builder().n("4").build());
        item.put("Id1", AttributeValue.builder().n("-23").build());
        item.put("Id2", AttributeValue.builder().n("99.10").build());
        item.put("title", AttributeValue.builder().s("Title40").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Drake").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar",
                AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }
}
