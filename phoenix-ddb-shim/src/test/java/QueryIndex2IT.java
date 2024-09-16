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
    public void testPaginationWithLastKey() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put items
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

        //query request using index
        QueryRequest qr = new QueryRequest(tableName);
        qr.setIndexName(indexName);
        qr.setKeyConditionExpression("#0 = :v0 AND #1 < :v1");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "attr_1");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("str_val_1"));
        exprAttrVal.put(":v1", new AttributeValue().withN("453.23"));
        qr.setExpressionAttributeValues(exprAttrVal);
        qr.setLimit(1);

        // query result
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

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testFilterExpressionWithIndex() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest qr = new QueryRequest(tableName);
        qr.setIndexName(indexName);
        qr.setKeyConditionExpression("#0 = :v0 AND #1 < :v1");
        qr.setFilterExpression("#2 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "Title");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("str_val_1"));
        exprAttrVal.put(":v1", new AttributeValue().withN("453.23"));
        exprAttrVal.put(":v2", new AttributeValue().withS("hello"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems(), phoenixResult.getItems());

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testScanIndexForward() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest qr = new QueryRequest(tableName);
        qr.setIndexName(indexName);
        qr.setKeyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("str_val_1"));
        qr.setExpressionAttributeValues(exprAttrVal);
        qr.setScanIndexForward(false);

        // query result, should return 3 items in descending order of sort key
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        for (int i=0; i<phoenixResult.getItems().size()-1;i++) {
            double sortKeyVal1 = Double.parseDouble(phoenixResult.getItems().get(i).get("attr_1").getN());
            double sortKeyVal2 = Double.parseDouble(phoenixResult.getItems().get(i+1).get("attr_1").getN());
            Assert.assertTrue(sortKeyVal1 >= sortKeyVal2);
        }

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testIndexBeginsWith() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_1",
                        ScalarAttributeType.N, null, null);
        // create index on attr_0, attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "Title", ScalarAttributeType.S);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest qr = new QueryRequest(tableName);
        qr.setIndexName(indexName);
        qr.setKeyConditionExpression("#0 = :v0 AND begins_with(#1, :v1)");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "Title");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("str_val_1"));
        exprAttrVal.put(":v1", new AttributeValue().withS("he"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems(), phoenixResult.getItems());

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testIndexBetween() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest qr = new QueryRequest(tableName);
        qr.setIndexName(indexName);
        qr.setKeyConditionExpression("#0 = :v0 AND #1 BETWEEN :v1 AND :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "attr_1");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("str_val_1"));
        exprAttrVal.put(":v1", new AttributeValue().withN("0.0001"));
        exprAttrVal.put(":v2", new AttributeValue().withN("30.121"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems(), phoenixResult.getItems());

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testProjectionScanIndexForwardFilterLimit() throws SQLException {
        // create table with keys [title, num]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "Title",
                        ScalarAttributeType.S, "num", ScalarAttributeType.N);
        // create index on attr_0, attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "Attr_0",
                ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put items
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

        //query request using index and filter
        QueryRequest qr = new QueryRequest(tableName);
        qr.setIndexName(indexName);
        qr.setKeyConditionExpression("#0 = :v0 AND #1 > :v1");
        qr.setFilterExpression("#2 > :v2");
        qr.setProjectionExpression("Title");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "num");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("str_val_1"));
        exprAttrVal.put(":v1", new AttributeValue().withN("0.0"));
        exprAttrVal.put(":v2", new AttributeValue().withN("1.01"));
        qr.setExpressionAttributeValues(exprAttrVal);
        qr.setScanIndexForward(false);
        qr.setLimit(1);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems(), phoenixResult.getItems());

        // check last Key
        Map<String, AttributeValue> lastKey = phoenixResult.getLastEvaluatedKey();
        Assert.assertEquals(getItem1(), lastKey);

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", new AttributeValue().withS("str_val_1"));
        item.put("attr_1", new AttributeValue().withN("11295.03"));
        item.put("Title", new AttributeValue().withS("hi"));
        item.put("num", new AttributeValue().withN("1.1"));
        return item;
    }

    private Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", new AttributeValue().withS("str_val_1"));
        item.put("attr_1", new AttributeValue().withN("30.12"));
        item.put("Title", new AttributeValue().withS("hello"));
        item.put("num", new AttributeValue().withN("1.2"));
        return item;
    }

    private Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", new AttributeValue().withS("str_val_1"));
        item.put("attr_1", new AttributeValue().withN("-21.06"));
        item.put("Title", new AttributeValue().withS("hey"));
        item.put("num", new AttributeValue().withN("1.3"));
        return item;
    }

    private Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Attr_0", new AttributeValue().withS("str_val_1"));
        item.put("attr_1", new AttributeValue().withN("0.1"));
        item.put("Title", new AttributeValue().withS("hie"));
        item.put("num", new AttributeValue().withN("-0.8"));
        return item;
    }

}
