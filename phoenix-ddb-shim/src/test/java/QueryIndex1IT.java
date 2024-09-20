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
 * Tests for basic index functionality without other QueryRequest parameters.
 */
public class QueryIndex1IT {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryIndex1IT.class);

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
    public void testGlobalIndexNoSortKey1() throws SQLException {
        // create table with keys [attr_0]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        // create index on IdS
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "IdS",
                ScalarAttributeType.S, null, null);
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
        qr.setKeyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "IdS");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("101.01"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.getLastEvaluatedKey();
        Assert.assertEquals("101.01", lastKey.get("IdS").getS());

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testGlobalIndexNoSortKey2() throws SQLException {
        // create table with keys [attr_0, attr_1]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        // create index on IdS
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "IdS",
                ScalarAttributeType.S, null, null);
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
        qr.setKeyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "IdS");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("101.01"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.getLastEvaluatedKey();
        Assert.assertEquals("101.01", lastKey.get("IdS").getS());

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testGlobalIndexWithSortKey1() throws SQLException {
        // create table with keys [attr_0]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        // create index on IdS, n_attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "IdS",
                ScalarAttributeType.S, "Id2", ScalarAttributeType.N);
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
        exprAttrNames.put("#0", "IdS");
        exprAttrNames.put("#1", "Id2");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("101.01"));
        exprAttrVal.put(":v1", new AttributeValue().withN("2.1"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.getLastEvaluatedKey();
        Assert.assertEquals("101.01", lastKey.get("IdS").getS());
        Assert.assertEquals(1.1, Double.parseDouble(lastKey.get("Id2").getN()), 0);

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testGlobalIndexWithSortKey2() throws SQLException {
        // create table with keys [attr_0, attr_1]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        // create index on IdS, n_attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "IdS",
                ScalarAttributeType.S, "Id2", ScalarAttributeType.N);
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
        exprAttrNames.put("#0", "IdS");
        exprAttrNames.put("#1", "Id2");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("101.01"));
        exprAttrVal.put(":v1", new AttributeValue().withN("2.1"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.getLastEvaluatedKey();
        Assert.assertEquals("101.01", lastKey.get("IdS").getS());
        Assert.assertEquals(1.1, Double.parseDouble(lastKey.get("Id2").getN()), 0);

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    @Test
    public void testLocalIndex() throws SQLException {
        // create table with keys [attr_0, attr_1]
        final String tableName = testName.getMethodName().toUpperCase();
        final String indexName = "G_IDX_" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        // create index on attr_0, IdS
        DDLTestUtils.addIndexToRequest(false, createTableRequest, indexName, "attr_0",
                ScalarAttributeType.S, "IdS", ScalarAttributeType.S);
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
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "IdS");
        qr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", new AttributeValue().withS("str_val_1"));
        exprAttrVal.put(":v1", new AttributeValue().withS("101.02"));
        qr.setExpressionAttributeValues(exprAttrVal);

        // query result
        QueryResult phoenixResult = phoenixDBClient.query(qr);
        QueryResult dynamoResult = amazonDynamoDB.query(qr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertEquals(dynamoResult.getItems().get(0), phoenixResult.getItems().get(0));

        // check last evaluated key
        Map<String, AttributeValue> lastKey = phoenixResult.getLastEvaluatedKey();
        Assert.assertEquals("str_val_1", lastKey.get("attr_0").getS());
        Assert.assertEquals("101.01", lastKey.get("IdS").getS());

        // explain plan
        TestUtils.validateIndexUsed(qr, url);
    }

    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("str_val_1"));
        item.put("attr_1", new AttributeValue().withN("11295.03"));
        item.put("IdS", new AttributeValue().withS("101.01"));
        item.put("Id2", new AttributeValue().withN("1.1"));
        return item;
    }

    private Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("str_val_2"));
        item.put("attr_1", new AttributeValue().withN("21295.03"));
        item.put("IdS", new AttributeValue().withS("202.02"));
        item.put("Id2", new AttributeValue().withN("2.2"));
        return item;
    }

    private Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("str_val_3"));
        item.put("attr_1", new AttributeValue().withN("31295.03"));
        item.put("IdS", new AttributeValue().withS("303.03"));
        item.put("Id2", new AttributeValue().withN("3.3"));
        return item;
    }

    private Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("str_val_4"));
        item.put("attr_1", new AttributeValue().withN("41295.03"));
        item.put("IdS", new AttributeValue().withS("404.04"));
        item.put("Id2", new AttributeValue().withN("4.4"));
        return item;
    }
}
