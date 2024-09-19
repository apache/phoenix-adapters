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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class QueryIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIT.class);

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void initialize() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        HBaseTestingUtility utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
    }

    @AfterClass
    public static void stopLocalDynamoDb() {
        LocalDynamoDbTestBase.localDynamoDb().stop();
    }

    @Test
    public void queryLimitAndFilterTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());
        PutItemRequest putItemRequest3 = new PutItemRequest(tableName, getItem3());
        PutItemRequest putItemRequest4 = new PutItemRequest(tableName, getItem4());
        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);
        phoenixDBClient.putItem(putItemRequest3);
        phoenixDBClient.putItem(putItemRequest4);
        amazonDynamoDB.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest4);

        //query request
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("#0 = :v0 AND #1 < :v1");
        qr.setFilterExpression("#2 <= :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "Id2");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("B"));
        exprAttrVal.put(":v1", new AttributeValue().withN("4"));
        exprAttrVal.put(":v2", new AttributeValue().withN("1000.10"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result, should return 2 items
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertTrue(dynamoResult.getCount() == 2);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());

        // limit, should return 1 item
        qr.setLimit(1);
        phoenixResult = phoenixDBClient.query(qr);
        dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertTrue(dynamoResult.getCount() == 1);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));
    }

    @Test
    public void queryBetweenTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());
        PutItemRequest putItemRequest3 = new PutItemRequest(tableName, getItem3());
        PutItemRequest putItemRequest4 = new PutItemRequest(tableName, getItem4());
        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);
        phoenixDBClient.putItem(putItemRequest3);
        phoenixDBClient.putItem(putItemRequest4);
        amazonDynamoDB.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest4);

        //query request
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("#0 = :v0 AND #1 BETWEEN :v1 AND :v2");
        qr.setFilterExpression("#2 > :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "Id1");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("B"));
        exprAttrVal.put(":v1", new AttributeValue().withN("1"));
        exprAttrVal.put(":v2", new AttributeValue().withN("5"));
        exprAttrVal.put(":v3", new AttributeValue().withN("0"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result, should return 1 item
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertTrue(dynamoResult.getCount() == 1);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));
    }

    @Test
    public void queryBeginsWithTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "title", ScalarAttributeType.S);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());
        PutItemRequest putItemRequest3 = new PutItemRequest(tableName, getItem3());
        PutItemRequest putItemRequest4 = new PutItemRequest(tableName, getItem4());
        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);
        phoenixDBClient.putItem(putItemRequest3);
        phoenixDBClient.putItem(putItemRequest4);
        amazonDynamoDB.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest4);

        //query request
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("#0 = :v0 AND begins_with(#1, :v1)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "title");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("B"));
        exprAttrVal.put(":v1", new AttributeValue().withS("Title4"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result, should return 1 item
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertTrue(dynamoResult.getCount() == 1);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));
    }

    @Test
    public void querySimpleProjectionTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());

        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest2);

        //query request
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("B"));
        qr.setExpressionAttributeValues(exprAttrVal);
        String projectionExpr = "Id2, title, #proj";
        qr.setProjectionExpression(projectionExpr);
        exprAttrNames.put("#proj", "A.B");
        qr.setExpressionAttributeNames(exprAttrNames);

        // query result, should return 1 item with only the projected attributes
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertTrue(dynamoResult.getCount() == 1);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));
    }

    @Test
    public void queryScanIndexForwardTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());
        PutItemRequest putItemRequest3 = new PutItemRequest(tableName, getItem3());
        PutItemRequest putItemRequest4 = new PutItemRequest(tableName, getItem4());
        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);
        phoenixDBClient.putItem(putItemRequest3);
        phoenixDBClient.putItem(putItemRequest4);
        amazonDynamoDB.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest4);

        //query request
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("#0 = :v0 AND #1 > :v1");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("B"));
        exprAttrVal.put(":v1", new AttributeValue().withN("1"));
        qr.setExpressionAttributeValues(exprAttrVal);
        qr.setScanIndexForward(false);

        // query result, should return 3 items in descending order of sort key
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertTrue(dynamoResult.getCount() == 3);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        for (int i=0; i<phoenixResult.getItems().size()-1;i++) {
            int sortKeyVal1 = Integer.parseInt(phoenixResult.getItems().get(i).get("attr_1").getN());
            int sortKeyVal2 = Integer.parseInt(phoenixResult.getItems().get(i+1).get("attr_1").getN());
            Assert.assertTrue(sortKeyVal1 >= sortKeyVal2);
        }
    }

    @Test
    public void queryLastEvaluatedKeyPagingWithHashSortKeysTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());
        PutItemRequest putItemRequest3 = new PutItemRequest(tableName, getItem3());
        PutItemRequest putItemRequest4 = new PutItemRequest(tableName, getItem4());
        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);
        phoenixDBClient.putItem(putItemRequest3);
        phoenixDBClient.putItem(putItemRequest4);
        amazonDynamoDB.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest4);

        //query request
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("#0 = :v0 AND #1 > :v1");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("B"));
        exprAttrVal.put(":v1", new AttributeValue().withN("1"));
        qr.setExpressionAttributeValues(exprAttrVal);
        qr.setLimit(2);

        // query result, should return 2 items
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertTrue(dynamoResult.getCount() == 2);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.getLastEvaluatedKey();
        Assert.assertEquals("B", lastKey.get("attr_0").getS());
        Assert.assertEquals(3, Integer.parseInt(lastKey.get("attr_1").getN()));

        // provide lastEvaluatedKey as exclusiveStartKey, remaining 1 item should be returned
        qr.setLimit(null);
        qr.setExclusiveStartKey(lastKey);
        phoenixResult = phoenixDBClient.query(qr);
        dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertTrue(phoenixResult.getCount() == 1);

        // check last key
        lastKey = phoenixResult.getLastEvaluatedKey();
        Assert.assertEquals("B", lastKey.get("attr_0").getS());
        Assert.assertEquals(4, Integer.parseInt(lastKey.get("attr_1").getN()));

        // note that dynamo's last evaluated key will be null here
        Assert.assertNull(dynamoResult.getLastEvaluatedKey());

        // provide lastEvaluatedKey as exclusiveStartKey, no items should be returned
        qr.setExclusiveStartKey(lastKey);
        phoenixResult = phoenixDBClient.query(qr);
        dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertTrue(phoenixResult.getCount() == 0);
    }

    @Test
    public void queryLastEvaluatedKeyPagingWithHashKeyTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());
        PutItemRequest putItemRequest3 = new PutItemRequest(tableName, getItem3());
        PutItemRequest putItemRequest4 = new PutItemRequest(tableName, getItem4());
        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);
        phoenixDBClient.putItem(putItemRequest3);
        phoenixDBClient.putItem(putItemRequest4);
        amazonDynamoDB.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest4);

        //query request
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("B"));
        qr.setExpressionAttributeValues(exprAttrVal);
        qr.setLimit(1);
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);

        while (phoenixResult.getCount() > 0) {
            Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
            Assert.assertEquals(dynamoResult.getItems(), phoenixResult.getItems());
            Assert.assertEquals(dynamoResult.getLastEvaluatedKey(), phoenixResult.getLastEvaluatedKey());
            qr.setExclusiveStartKey(phoenixResult.getLastEvaluatedKey());
            phoenixResult = phoenixDBClient.query(qr);
            dynamoResult = amazonDynamoDB.query(qr);
        }
    }

    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("A"));
        item.put("attr_1", new AttributeValue().withN("1"));
        item.put("Id1", new AttributeValue().withN("-5"));
        item.put("Id2", new AttributeValue().withN("10.10"));
        item.put("title", new AttributeValue().withS("Title1"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Alice"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(new AttributeValue().withM(reviewMap1)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        item.put("A.B", new AttributeValue().withS("not nested field 1"));
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("B"));
        item.put("attr_1", new AttributeValue().withN("2"));
        item.put("Id1", new AttributeValue().withN("-15"));
        item.put("Id2", new AttributeValue().withN("150.10"));
        item.put("title", new AttributeValue().withS("Title2"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Bob1"));
        Map<String, AttributeValue> reviewMap2 = new HashMap<>();
        reviewMap2.put("reviewer", new AttributeValue().withS("Bob2"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(
                new AttributeValue().withM(reviewMap1),
                new AttributeValue().withM(reviewMap2)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        item.put("A.B", new AttributeValue().withS("not nested field 2"));
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("B"));
        item.put("attr_1", new AttributeValue().withN("3"));
        item.put("Id1", new AttributeValue().withN("11"));
        item.put("Id2", new AttributeValue().withN("1000.10"));
        item.put("title", new AttributeValue().withS("Title3"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Carl"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(new AttributeValue().withM(reviewMap1)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        item.put("A.B", new AttributeValue().withS("not nested field 3"));
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("B"));
        item.put("attr_1", new AttributeValue().withN("4"));
        item.put("Id1", new AttributeValue().withN("-23"));
        item.put("Id2", new AttributeValue().withN("99.10"));
        item.put("title", new AttributeValue().withS("Title40"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Drake"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(new AttributeValue().withM(reviewMap1)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        item.put("A.B", new AttributeValue().withS("not nested field 4"));
        return item;
    }
}
