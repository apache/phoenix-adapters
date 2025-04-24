import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
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

public class QueryProjectionIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryProjectionIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    private static String TABLE_NAME = "PROJECTION_TEST_TABLE";

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

        DynamoDbClient dynamoDbClient =
                LocalDynamoDbTestBase.localDynamoDb().createV2Client();

        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(TABLE_NAME, "attr_0",
                        ScalarAttributeType.S, null, null);

        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1
                = PutItemRequest.builder().tableName(TABLE_NAME).item(getItem()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

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

    /* Item Schema */
    private static Map<String, AttributeValue> getItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("B").build());
        item.put("attr_1", AttributeValue.builder().n("2").build());
        item.put("Id1", AttributeValue.builder().n("-15").build());
        item.put("Id2", AttributeValue.builder().n("150.10").build());
        item.put("title", AttributeValue.builder().s("Title2").build());

        // list of maps
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Bob1").build());
        Map<String, AttributeValue> reviewMap2 = new HashMap<>();
        reviewMap2.put("reviewer", AttributeValue.builder().s("Bob2").build());
        Map<String, AttributeValue> reviewMap3 = new HashMap<>();
        reviewMap3.put("reviewer", AttributeValue.builder().s("Bob3").build());
        Map<String, AttributeValue> reviewMap4 = new HashMap<>();
        reviewMap4.put("reviewer", AttributeValue.builder().s("Bob4").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(
                AttributeValue.builder().m(reviewMap1).build(),
                AttributeValue.builder().m(reviewMap2).build(),
                AttributeValue.builder().m(reviewMap3).build(),
                AttributeValue.builder().m(reviewMap4).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());

        // nested maps
        Map<String, AttributeValue> nestedMap1 = new HashMap<>();
        nestedMap1.put("val1", AttributeValue.builder().s("val1").build());
        nestedMap1.put("val2", AttributeValue.builder().s("val2").build());
        nestedMap1.put("val3", AttributeValue.builder().s("val3").build());
        Map<String, AttributeValue> nestedMap2 = new HashMap<>();
        nestedMap2.put("map2", AttributeValue.builder().m(nestedMap1).build());
        item.put("map3", AttributeValue.builder().m(nestedMap2).build());

        //nested list with different types
        item.put("nestedList", AttributeValue.builder().l(
                AttributeValue.builder().l(
                        AttributeValue.builder().s("a").build(),
                        AttributeValue.builder().n("1").build()
                ).build(),
                AttributeValue.builder().l(
                        AttributeValue.builder().l(
                                AttributeValue.builder().s("c").build(),
                                AttributeValue.builder().s("d").build()
                        ).build()
                ).build(),
                AttributeValue.builder().l(
                        AttributeValue.builder().s("b").build(),
                        AttributeValue.builder().n("2").build()
                ).build()
        ).build());

        // nested list->list->map->list
        // track[0].shot[2][0].city.standard[2]
        AttributeValue list1 = AttributeValue.builder().l(
                AttributeValue.builder().n("1").build(),
                AttributeValue.builder().n("2").build(),
                AttributeValue.builder().n("3").build()
        ).build();
        Map<String, AttributeValue> map1 = new HashMap<>();
        map1.put("standard", list1);
        Map<String, AttributeValue> map2 = new HashMap<>();
        map2.put("city", AttributeValue.builder().m(map1).build());

        AttributeValue shot = AttributeValue.builder().l(
                AttributeValue.builder().l(
                        AttributeValue.builder().s("x").build()
                ).build(),
                AttributeValue.builder().l(
                        AttributeValue.builder().m(map2).build(),
                        AttributeValue.builder().s("s").build()
                ).build()
        ).build();

        Map<String, AttributeValue> shotMap = new HashMap<>();
        shotMap.put("shot", shot);
        AttributeValue list2 = AttributeValue.builder().l(
                AttributeValue.builder().m(shotMap).build(),
                AttributeValue.builder().s("hello").build()
        ).build();
        item.put("track", list2);
        item.put("A.B", AttributeValue.builder().s("not nested field 1").build());
        return item;
    }

    /* TESTS */

    @Test(timeout = 120000)
    public void test1() {
        test("Id2, title, Reviews");
    }

    @Test(timeout = 120000)
    public void test2() {
        test("Id2, title, Reviews.FiveStar");
    }

    @Test(timeout = 120000)
    public void test3() {
        test("Id2, title, Reviews.FiveStar[0]");
    }

    @Test(timeout = 120000)
    public void test4() {
        test("Id2, title, Reviews.FiveStar[2]");
    }

    @Test(timeout = 120000)
    public void test5() {
        test("Id2, title, Reviews.FiveStar[1].reviewer");
    }

    @Test(timeout = 120000)
    public void test6() {
        test("Id2, title, Reviews.FiveStar[1].reviewer,Reviews.FiveStar[2].reviewer,Reviews.FiveStar[3].reviewer");
    }

    @Test(timeout = 120000)
    public void test7() {
        test("Id2, title, Reviews.FiveStar[3].reviewer,Reviews.FiveStar[2].reviewer,Reviews.FiveStar[1].reviewer");
    }

    @Test(timeout = 120000)
    public void test8() {
        test("Id2, title, Reviews.FiveStar[3].reviewer,Reviews.FiveStar[1].reviewer,Reviews.FiveStar[2].reviewer");
    }

    @Test(timeout = 120000)
    public void test9() {
        test("Id2, title, map3");
    }

    @Test(timeout = 120000)
    public void test10() {
        test("Id2, title, map3.map2");
    }

    @Test(timeout = 120000)
    public void test11() {
        test("map3.map2.val2,Id2, title, map3.map2.val1");
    }

    @Test(timeout = 120000)
    public void test12() {
        test("Id2, title, nestedList");
    }

    @Test(timeout = 120000)
    public void test13() {
        test("Id2, title, nestedList[0]");
    }

    @Test(timeout = 120000)
    public void test14() {
        test("nestedList[0][1], Id2, title");
    }

    @Test(timeout = 120000)
    public void test15() {
        test("nestedList[0][1], Id2, nestedList[2][0], title, nestedList[0][0]");
    }

    @Test(timeout = 120000)
    public void test16() {
        test("nestedList[0][0], nestedList[2][1], nestedList[1][0][0], nestedList[2][0]");
    }

    @Test(timeout = 120000)
    public void test17() {
        test("track[0].shot[2][0].city.standard[1],track[0].shot[2][0].city.standard[2], track[0].shot[2][0].city.standard[0]");
    }

    @Test(timeout = 120000)
    public void test18() {
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "track");
        exprAttrNames.put("#1", "shot");
        exprAttrNames.put("#2", "city");
        exprAttrNames.put("#3", "standard");
        exprAttrNames.put("#4", "A.B");
        testWithMap("#0[0].#1[2][0].#2.#3[1],#0[0].#1[2][0].#2.#3[2], #0[0].#1[2][0].#2.#3[0], #4", exprAttrNames);
    }

    private QueryRequest.Builder getQueryRequest(Map<String, String> exprAttrNames) {
        QueryRequest.Builder qr = QueryRequest.builder().tableName(TABLE_NAME);
        qr.keyConditionExpression("#hashKey = :v0");
        if (exprAttrNames == null) {
            exprAttrNames = new HashMap<>();
        }
        exprAttrNames.put("#hashKey", "attr_0");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        qr.expressionAttributeValues(exprAttrVal);
        return qr;
    }

    private void test(String projectionExpr) {
        QueryRequest.Builder qr = getQueryRequest(null);
        qr.projectionExpression(projectionExpr);
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
    }

    private void testWithMap(String projectionExpr, Map<String, String> exprAttrNames) {
        QueryRequest.Builder qr = getQueryRequest(exprAttrNames);
        qr.projectionExpression(projectionExpr);
        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
    }
}