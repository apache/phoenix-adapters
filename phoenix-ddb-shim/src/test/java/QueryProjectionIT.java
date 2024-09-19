

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class QueryProjectionIT {

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static PhoenixDBClient phoenixDBClient = null;

    private static String url;

    private static String TABLE_NAME = "PROJECTION_TEST_TABLE";

    @BeforeClass
    public static void initialize() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        HBaseTestingUtility utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        phoenixDBClient = new PhoenixDBClient(url);
        AmazonDynamoDB amazonDynamoDB =
                LocalDynamoDbTestBase.localDynamoDb().createV1Client();

        //create table
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(TABLE_NAME, "attr_0",
                        ScalarAttributeType.S, null, null);

        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = new PutItemRequest(TABLE_NAME, getItem());
        phoenixDBClient.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest1);

    }

    @AfterClass
    public static void stopLocalDynamoDb() {
        LocalDynamoDbTestBase.localDynamoDb().stop();
    }

    /* Item Schema */
    private static Map<String, AttributeValue> getItem() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("B"));
        item.put("attr_1", new AttributeValue().withN("2"));
        item.put("Id1", new AttributeValue().withN("-15"));
        item.put("Id2", new AttributeValue().withN("150.10"));
        item.put("title", new AttributeValue().withS("Title2"));

        // list of maps
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Bob1"));
        Map<String, AttributeValue> reviewMap2 = new HashMap<>();
        reviewMap2.put("reviewer", new AttributeValue().withS("Bob2"));
        Map<String, AttributeValue> reviewMap3 = new HashMap<>();
        reviewMap3.put("reviewer", new AttributeValue().withS("Bob3"));
        Map<String, AttributeValue> reviewMap4 = new HashMap<>();
        reviewMap4.put("reviewer", new AttributeValue().withS("Bob4"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(
                new AttributeValue().withM(reviewMap1),
                new AttributeValue().withM(reviewMap2),
                new AttributeValue().withM(reviewMap3),
                new AttributeValue().withM(reviewMap4)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));

        // nested maps
        Map<String, AttributeValue> nestedMap1 = new HashMap<>();
        nestedMap1.put("val1", new AttributeValue().withS("val1"));
        nestedMap1.put("val2", new AttributeValue().withS("val2"));
        nestedMap1.put("val3", new AttributeValue().withS("val3"));
        Map<String, AttributeValue> nestedMap2 = new HashMap<>();
        nestedMap2.put("map2", new AttributeValue().withM(nestedMap1));
        item.put("map3", new AttributeValue().withM(nestedMap2));

        //nested list with different types
        item.put("nestedList", new AttributeValue().withL(
                new AttributeValue().withL(
                        new AttributeValue().withS("a"),
                        new AttributeValue().withN("1")
                ),
                new AttributeValue().withL(
                        new AttributeValue().withL(
                                new AttributeValue().withS("c"),
                                new AttributeValue().withS("d")
                        )
                ),
                new AttributeValue().withL(
                        new AttributeValue().withS("b"),
                        new AttributeValue().withN("2")
                )
        ));

        // nested list->list->map->list
        // track[0].shot[2][0].city.standard[2]
        AttributeValue list1 = new AttributeValue().withL(
                new AttributeValue().withN("1"),
                new AttributeValue().withN("2"),
                new AttributeValue().withN("3")
        );
        Map<String, AttributeValue> map1 = new HashMap<>();
        map1.put("standard", list1);
        Map<String, AttributeValue> map2 = new HashMap<>();
        map2.put("city", new AttributeValue().withM(map1));

        AttributeValue shot = new AttributeValue().withL(
                new AttributeValue().withL(),
                new AttributeValue().withL(
                        new AttributeValue().withS("x")
                ),
                new AttributeValue().withL(
                        new AttributeValue().withM(map2),
                        new AttributeValue().withS("s")
                )
        );

        Map<String, AttributeValue> shotMap = new HashMap<>();
        shotMap.put("shot", shot);
        AttributeValue list2 = new AttributeValue().withL(
                new AttributeValue().withM(shotMap),
                new AttributeValue().withS("hello")
        );
        item.put("track", list2);
        item.put("A.B", new AttributeValue().withS("not nested field 1"));
        return item;
    }

    /* TESTS */

    @Test
    public void test1() {
        test("Id2, title, Reviews");
    }

    @Test
    public void test2() {
        test("Id2, title, Reviews.FiveStar");
    }

    @Test
    public void test3() {
        test("Id2, title, Reviews.FiveStar[0]");
    }

    @Test
    public void test4() {
        test("Id2, title, Reviews.FiveStar[2]");
    }

    @Test
    public void test5() {
        test("Id2, title, Reviews.FiveStar[1].reviewer");
    }

    @Test
    public void test6() {
        test("Id2, title, Reviews.FiveStar[1].reviewer,Reviews.FiveStar[2].reviewer,Reviews.FiveStar[3].reviewer");
    }

    @Test
    public void test7() {
        test("Id2, title, Reviews.FiveStar[3].reviewer,Reviews.FiveStar[2].reviewer,Reviews.FiveStar[1].reviewer");
    }

    @Test
    public void test8() {
        test("Id2, title, Reviews.FiveStar[3].reviewer,Reviews.FiveStar[1].reviewer,Reviews.FiveStar[2].reviewer");
    }

    @Test
    public void test9() {
        test("Id2, title, map3");
    }

    @Test
    public void test10() {
        test("Id2, title, map3.map2");
    }

    @Test
    public void test11() {
        test("map3.map2.val2,Id2, title, map3.map2.val1");
    }

    @Test
    public void test12() {
        test("Id2, title, nestedList");
    }

    @Test
    public void test13() {
        test("Id2, title, nestedList[0]");
    }

    @Test
    public void test14() {
        test("nestedList[0][1], Id2, title");
    }

    @Test
    public void test15() {
        test("nestedList[0][1], Id2, nestedList[2][0], title, nestedList[0][0]");
    }

    @Test
    public void test16() {
        test("nestedList[0][0], nestedList[2][1], nestedList[1][0][0], nestedList[2][0]");
    }

    @Test
    public void test17() {
        test("track[0].shot[2][0].city.standard[1],track[0].shot[2][0].city.standard[2], track[0].shot[2][0].city.standard[0]");
    }

    @Test
    public void test18() {
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "track");
        exprAttrNames.put("#1", "shot");
        exprAttrNames.put("#2", "city");
        exprAttrNames.put("#3", "standard");
        exprAttrNames.put("#4", "A.B");
        testWithMap("#0[0].#1[2][0].#2.#3[1],#0[0].#1[2][0].#2.#3[2], #0[0].#1[2][0].#2.#3[0], #4", exprAttrNames);
    }

    private QueryRequest getQueryRequest(Map<String, String> exprAttrNames) {
        QueryRequest qr = new QueryRequest(TABLE_NAME);
        qr.setKeyConditionExpression("#hashKey = :v0");
        if (exprAttrNames == null) exprAttrNames = new HashMap<>();
        exprAttrNames.put("#hashKey", "attr_0");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("B"));
        qr.setExpressionAttributeValues(exprAttrVal);
        return qr;
    }

    private void test(String projectionExpr) {
        QueryRequest qr = getQueryRequest(null);
        qr.setProjectionExpression(projectionExpr);
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));
    }

    private void testWithMap(String projectionExpr, Map<String, String> exprAttrNames) {
        QueryRequest qr = getQueryRequest(exprAttrNames);
        qr.setProjectionExpression(projectionExpr);
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));
    }
}