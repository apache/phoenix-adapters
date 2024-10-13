import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnValuesOnConditionCheckFailure;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import org.bson.BsonDocument;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ConditionalPutItemIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(PutItemIT.class);

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

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
    public void simpleConditionalPutFailureTest() throws Exception {
        // create table [attr_0, attr_1]
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);

        // add index on attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName, "attr_1",
                ScalarAttributeType.N, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        // put (val1, 123)
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", new AttributeValue().withS("val1"));
        item1.put("attr_1", new AttributeValue().withN("123"));
        PutItemRequest item1PutRequest = new PutItemRequest(tableName, item1);
        phoenixDBClient.putItem(item1PutRequest);
        amazonDynamoDB.putItem(item1PutRequest);

        // conditional put (val1, 999) if attr_1 > 200
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", new AttributeValue().withS("val1"));
        item2.put("attr_1", new AttributeValue().withN("999"));
        // request
        PutItemRequest condPutRequest = new PutItemRequest(tableName, item2);
        // expression
        condPutRequest.setConditionExpression("#1 > :0");
        // expression names
        Map<String, String> exprAttrNames = new HashMap();
        exprAttrNames.put("#1", "attr_1");
        condPutRequest.setExpressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":0", new AttributeValue().withN("200"));
        condPutRequest.setExpressionAttributeValues(exprAttrVals);

        try {
            phoenixDBClient.putItem(condPutRequest);
            Assert.fail("PutItem should throw exception when condition check fails and return value is not set.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }
        try {
            amazonDynamoDB.putItem(condPutRequest);
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }

        //query from dynamo
        verifyResult1(tableName, item1);
    }

    @Test(timeout = 120000)
    public void simpleConditionalPutFailureTestWithItem() throws Exception {
        // create table [attr_0, attr_1]
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
            DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                ScalarAttributeType.S, null, null);

        // add index on attr_1
        DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName, "attr_1",
            ScalarAttributeType.N, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        // put (val1, 123)
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", new AttributeValue().withS("val1"));
        item1.put("attr_1", new AttributeValue().withN("123"));
        PutItemRequest item1PutRequest = new PutItemRequest(tableName, item1);
        phoenixDBClient.putItem(item1PutRequest);
        amazonDynamoDB.putItem(item1PutRequest);

        // conditional put (val1, 999) if attr_1 > 200
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", new AttributeValue().withS("val1"));
        item2.put("attr_1", new AttributeValue().withN("999"));
        // request
        PutItemRequest condPutRequest = new PutItemRequest(tableName, item2);
        // expression
        condPutRequest.setConditionExpression("#1 > :0");
        // expression names
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "attr_1");
        condPutRequest.setExpressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", new AttributeValue().withN("200"));
        condPutRequest.setExpressionAttributeValues(exprAttrVals);
        condPutRequest.setReturnValuesOnConditionCheckFailure(
            ReturnValuesOnConditionCheckFailure.ALL_OLD);

        try {
            phoenixDBClient.putItem(condPutRequest);
            Assert.fail(
                "PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(item1, e.getItem());
        }
        try {
            amazonDynamoDB.putItem(condPutRequest);
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(item1, e.getItem());
        }

        verifyResult1(tableName, item1);
    }

    private void verifyResult1(String tableName, Map<String, AttributeValue> item1)
        throws SQLException {
        //query from dynamo
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("attr_0 = :v");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v", new AttributeValue().withS("val1"));
        qr.setExpressionAttributeValues(exprAttrVal);
        QueryResult result = amazonDynamoDB.query(qr);
        Assert.assertEquals(1, result.getItems().size());
        Map<String, AttributeValue> dynamoItem = result.getItems().get(0);

        // check phoenix row, there should be no update
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement()
                .executeQuery("SELECT * FROM " + tableName + " WHERE \"attr_0\" = 'val1'");
            Assert.assertTrue(rs.next());
            BsonDocument rowBsonDoc = (BsonDocument) rs.getObject(2);
            Assert.assertEquals(rowBsonDoc.get("attr_1").asInt32().getValue(), 123);
            Map<String, AttributeValue> phoenixItem =
                BsonDocumentToDdbAttributes.getFullItem(rowBsonDoc);
            Assert.assertEquals(phoenixItem, dynamoItem);

            //check no update to index row [123, val1, (val1, 123)]
            rs = connection.createStatement().executeQuery("SELECT * FROM G_IDX_" + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getLong(1), Long.parseLong(item1.get("attr_1").getN()));
            Assert.assertEquals(rs.getString(2), item1.get("attr_0").getS());
            Map<String, AttributeValue> indexRowItem =
                BsonDocumentToDdbAttributes.getFullItem((BsonDocument) rs.getObject(3));
            Assert.assertEquals(indexRowItem, item1);
            Assert.assertEquals(indexRowItem, dynamoItem);
        }
    }

    @Test(timeout = 120000)
    public void simpleConditionalPutSuccessTest() throws Exception {
        // create table [attr_0, idx_attr]
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        // put (val1, 123)
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("attr_0", new AttributeValue().withS("val1"));
        item1.put("attr_1", new AttributeValue().withN("123"));
        PutItemRequest item1PutRequest = new PutItemRequest(tableName, item1);
        phoenixDBClient.putItem(item1PutRequest);
        amazonDynamoDB.putItem(item1PutRequest);

        // conditional put (val1, 999) if attr_1 != 200
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("attr_0", new AttributeValue().withS("val1"));
        item2.put("attr_1", new AttributeValue().withN("999"));
        // request
        PutItemRequest condPutRequest = new PutItemRequest(tableName, item2);
        // expression
        condPutRequest.setConditionExpression("#1 <> :0");
        // expression names
        Map<String, String> exprAttrNames = new HashMap();
        exprAttrNames.put("#1", "attr_1");
        condPutRequest.setExpressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":0", new AttributeValue().withN("200"));
        condPutRequest.setExpressionAttributeValues(exprAttrVals);

        try {
            phoenixDBClient.putItem(condPutRequest);
        } catch (ConditionalCheckFailedException e) {
            Assert.fail("PutItem should not throw exception when condition check passes.");
        }
        amazonDynamoDB.putItem(condPutRequest);

        //query from dynamo
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("attr_0 = :v");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v", new AttributeValue().withS("val1"));
        qr.setExpressionAttributeValues(exprAttrVal);
        QueryResult result = amazonDynamoDB.query(qr);
        Assert.assertEquals(1, result.getItems().size());
        Map<String, AttributeValue> dynamoItem = result.getItems().get(0);

        // check phoenix row, there should be update to the row
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE \"attr_0\" = 'val1'");
            Assert.assertTrue(rs.next());
            BsonDocument rowBsonDoc = (BsonDocument) rs.getObject(2);
            Assert.assertEquals(rowBsonDoc.get("attr_1").asInt32().getValue(), 999);
            Map<String, AttributeValue> phoenixItem = BsonDocumentToDdbAttributes.getFullItem(rowBsonDoc);
            Assert.assertEquals(phoenixItem, dynamoItem);
        }
    }

    @Test(timeout = 120000)
    public void compositeConditionalPutFailureTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item [str_val_0, item]
        Map<String, AttributeValue> item1 = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item1);
        phoenixDBClient.putItem(putItemRequest);
        amazonDynamoDB.putItem(putItemRequest);


        //conditional put
        Map<String, AttributeValue> item2 = DocumentDdbAttributesTest.getItem2();
        // request
        PutItemRequest condPutRequest = new PutItemRequest(tableName, item2);
        // expression
        condPutRequest.setConditionExpression("#0 = :0 AND #1 = :1 AND #2 = :2 AND #3 = :3 AND " +
                "attribute_not_exists(#4) AND attribute_not_exists(#5)");
        // expression names
        Map<String, String> exprAttrNames = new HashMap();
        exprAttrNames.put("#0", "attr_1");
        exprAttrNames.put("#1", "ColorBytes");
        exprAttrNames.put("#2", "Title");
        exprAttrNames.put("#3", "InPublication");
        exprAttrNames.put("#4", "NestedMap2");
        exprAttrNames.put("#5", "ISBN");
        condPutRequest.setExpressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":0", new AttributeValue().withN("1295.03"));
        exprAttrVals.put(":1", new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("Black"))));
        exprAttrVals.put(":2", new AttributeValue().withS("Book 101 Title "));
        exprAttrVals.put(":3", new AttributeValue().withBOOL(false));
        condPutRequest.setExpressionAttributeValues(exprAttrVals);

        try {
            phoenixDBClient.putItem(condPutRequest);
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }
        try {
            amazonDynamoDB.putItem(condPutRequest);
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }

        //query from dynamo
        verifyResult2(tableName, new AttributeValue().withS("str_val_0"), 2, item1);
    }

    @Test(timeout = 120000)
    public void compositeConditionalPutFailureTestWithSortKey() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
            DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                ScalarAttributeType.B, "pk2", ScalarAttributeType.B);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item [str_val_0, item]
        Map<String, AttributeValue> item1 = DocumentDdbAttributesTest.getItemWithBinary1();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item1);
        phoenixDBClient.putItem(putItemRequest);
        amazonDynamoDB.putItem(putItemRequest);

        //conditional put
        Map<String, AttributeValue> item2 = DocumentDdbAttributesTest.getItemWithBinary2();
        // request
        PutItemRequest condPutRequest = new PutItemRequest(tableName, item2);
        // expression
        condPutRequest.setConditionExpression("#0 = :0 AND #1 = :1 AND #2 = :2 AND #3 = :3 AND " +
            "attribute_not_exists(#4) AND attribute_not_exists(#5)");
        // expression names
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_1");
        exprAttrNames.put("#1", "ColorBytes");
        exprAttrNames.put("#2", "Title");
        exprAttrNames.put("#3", "InPublication");
        exprAttrNames.put("#4", "NestedMap2");
        exprAttrNames.put("#5", "ISBN");
        condPutRequest.setExpressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", new AttributeValue().withN("1295.03"));
        exprAttrVals.put(":1", new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("Black"))));
        exprAttrVals.put(":2", new AttributeValue().withS("Book 101 Title "));
        exprAttrVals.put(":3", new AttributeValue().withBOOL(false));
        condPutRequest.setExpressionAttributeValues(exprAttrVals);

        try {
            amazonDynamoDB.putItem(condPutRequest);
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }
        try {
            phoenixDBClient.putItem(condPutRequest);
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertNull(e.getItem());
        }

        verifyResult2(tableName,
            new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("str_val_0"))), 3, item1);
    }

    private void verifyResult2(String tableName, AttributeValue str_val_0, int columnIndex,
        Map<String, AttributeValue> item1) throws SQLException {
        //query from dynamo
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("attr_0 = :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", str_val_0);
        qr.setExpressionAttributeValues(exprAttrVal);
        QueryResult result = amazonDynamoDB.query(qr);
        Assert.assertEquals(1, result.getItems().size());
        Map<String, AttributeValue> dynamoItem = result.getItems().get(0);

        // check phoenix row, there should be no update to the row
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement()
                .executeQuery("SELECT * FROM " + tableName + " WHERE \"attr_0\" = 'str_val_0'");
            Assert.assertTrue(rs.next());
            Map<String, AttributeValue> phoenixItem =
                BsonDocumentToDdbAttributes.getFullItem((BsonDocument) rs.getObject(columnIndex));
            Assert.assertEquals(item1, phoenixItem);

            //TODO: uncomment when we have utility to compare sets
            //Assert.assertEquals(dynamoItem, phoenixItem);
        }
    }

    @Test(timeout = 120000)
    public void compositeConditionalPutFailureTestWithSortKeyAndReturnRow() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
            DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                ScalarAttributeType.B, "pk2", ScalarAttributeType.B);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item [str_val_0, item]
        Map<String, AttributeValue> item1 = DocumentDdbAttributesTest.getItemWithBinary1();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item1);
        phoenixDBClient.putItem(putItemRequest);
        amazonDynamoDB.putItem(putItemRequest);

        //conditional put
        Map<String, AttributeValue> item2 = DocumentDdbAttributesTest.getItemWithBinary2();
        // request
        PutItemRequest condPutRequest = new PutItemRequest(tableName, item2);
        // expression
        condPutRequest.setConditionExpression("#0 = :0 AND #1 = :1 AND #2 = :2 AND #3 = :3 AND " +
            "attribute_not_exists(#4) AND attribute_not_exists(#5)");
        // expression names
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_1");
        exprAttrNames.put("#1", "ColorBytes");
        exprAttrNames.put("#2", "Title");
        exprAttrNames.put("#3", "InPublication");
        exprAttrNames.put("#4", "NestedMap2");
        exprAttrNames.put("#5", "ISBN");
        condPutRequest.setExpressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":0", new AttributeValue().withN("1295.03"));
        exprAttrVals.put(":1", new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("Black"))));
        exprAttrVals.put(":2", new AttributeValue().withS("Book 101 Title "));
        exprAttrVals.put(":3", new AttributeValue().withBOOL(false));
        condPutRequest.setExpressionAttributeValues(exprAttrVals);
        condPutRequest.setReturnValuesOnConditionCheckFailure(
            ReturnValuesOnConditionCheckFailure.ALL_OLD);

        try {
            amazonDynamoDB.putItem(condPutRequest);
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            // TODO: uncomment when we have utility to compare sets
            // Assert.assertEquals(item1, e.getItem());
        }
        try {
            phoenixDBClient.putItem(condPutRequest);
            Assert.fail("PutItem should throw exception when condition check fails.");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(item1, e.getItem());
        }

        //query from dynamo
        verifyResult2(tableName,
            new AttributeValue().withB(ByteBuffer.wrap(Bytes.toBytes("str_val_0"))), 3, item1);
    }

    @Test(timeout = 120000)
    public void compositeConditionalPutSuccessTest() throws Exception {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        // add index on Id2
        DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName, "Id2",
                ScalarAttributeType.N, null, null);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item [str_val_0, item]
        Map<String, AttributeValue> item1 = DocumentDdbAttributesTest.getItem1();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item1);
        phoenixDBClient.putItem(putItemRequest);
        amazonDynamoDB.putItem(putItemRequest);

        //conditional put
        Map<String, AttributeValue> item2 = DocumentDdbAttributesTest.getItem2();
        // request
        PutItemRequest condPutRequest = new PutItemRequest(tableName, item2);
        // expression
        condPutRequest.setConditionExpression("(#0 = :0) AND " +
                "(((attribute_not_exists(#1) AND attribute_not_exists(#2)) OR attribute_exists(#3)) " +
                "OR (#4 = :1))");
        // expression names
        Map<String, String> exprAttrNames = new HashMap();
        exprAttrNames.put("#0", "attr_1");
        exprAttrNames.put("#1", "attr_5");
        exprAttrNames.put("#2", "attr_6");
        exprAttrNames.put("#3", "not_found");
        exprAttrNames.put("#4", "Id2");
        condPutRequest.setExpressionAttributeNames(exprAttrNames);
        // expression values
        Map<String, AttributeValue> exprAttrVals = new HashMap();
        exprAttrVals.put(":0", new AttributeValue().withN("1295.03"));
        exprAttrVals.put(":1", new AttributeValue().withN("101.01"));
        condPutRequest.setExpressionAttributeValues(exprAttrVals);

        try {
            phoenixDBClient.putItem(condPutRequest);
        } catch (ConditionalCheckFailedException e) {
            Assert.fail("PutItem should not throw exception when condition check passes.");
        }
        amazonDynamoDB.putItem(condPutRequest);

        //query from dynamo
        QueryRequest qr = new QueryRequest(tableName);
        qr.setKeyConditionExpression("attr_0 = :val");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":val", new AttributeValue().withS("str_val_0"));
        qr.setExpressionAttributeValues(exprAttrVal);
        QueryResult result = amazonDynamoDB.query(qr);
        Assert.assertEquals(1, result.getItems().size());
        Map<String, AttributeValue> dynamoItem = result.getItems().get(0);

        // check phoenix row, there should be update to the row
        try (Connection connection = DriverManager.getConnection(url)) {
            ResultSet rs = connection.createStatement().executeQuery("SELECT * FROM " + tableName + " WHERE \"attr_0\" = 'str_val_0'");
            Assert.assertTrue(rs.next());
            Map<String, AttributeValue> phoenixItem = BsonDocumentToDdbAttributes.getFullItem((BsonDocument) rs.getObject(2));
            Assert.assertEquals(phoenixItem, item2);

            // check index row is updated (Id2, attr_0, COL)
            rs = connection.createStatement().executeQuery("SELECT * FROM G_IDX_" + tableName);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getDouble(1), Double.parseDouble(item2.get("Id2").getN()), 0.0);
            Assert.assertEquals(rs.getString(2), item2.get("attr_0").getS());
            Map<String, AttributeValue> indexRowItem = BsonDocumentToDdbAttributes.getFullItem((BsonDocument) rs.getObject(3));
            Assert.assertEquals(indexRowItem, item2);

            //TODO: uncomment when we have utility to compare sets
            //Assert.assertEquals(dynamoItem, phoenixItem);
            //Assert.assertEquals(dynamoItem, indexRowItem);
        }
    }
}
