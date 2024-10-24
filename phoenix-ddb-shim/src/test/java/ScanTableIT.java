import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ScanTableIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanTableIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

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
    public void testScanAllRowsNoSortKey() {
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

        ScanRequest sr = new ScanRequest(tableName);
        sr.setProjectionExpression("title, #0.#1[0].#2");
        Map<String,String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Reviews");
        exprAttrNames.put("#1", "FiveStar");
        exprAttrNames.put("#2", "reviewer");
        sr.setExpressionAttributeNames(exprAttrNames);
        ScanResult phoenixResult = phoenixDBClient.scan(sr);
        ScanResult dynamoResult = amazonDynamoDB.scan(sr);
        // dynamo does not guarantee ordering of partition keys in Scan, so only check count
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        for (Map<String, AttributeValue> item : phoenixResult.getItems()) {
            Assert.assertNotNull(item.get("Reviews").getM().get("FiveStar").getL().get(0).getM().get("reviewer"));
            Assert.assertNotNull(item.get("title"));
        }
        Assert.assertEquals(dynamoResult.getScannedCount(), phoenixResult.getScannedCount());
    }

    @Test(timeout = 120000)
    public void testScanAllRowsWithProjection() {
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

        ScanRequest sr = new ScanRequest(tableName);
        sr.setProjectionExpression("title, #0.#1[0].#2");
        Map<String,String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Reviews");
        exprAttrNames.put("#1", "FiveStar");
        exprAttrNames.put("#2", "reviewer");
        sr.setExpressionAttributeNames(exprAttrNames);
        ScanResult phoenixResult = phoenixDBClient.scan(sr);
        ScanResult dynamoResult = amazonDynamoDB.scan(sr);
        // dynamo does not guarantee ordering of partition keys in Scan, so only check count
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        for (Map<String, AttributeValue> item : phoenixResult.getItems()) {
            Assert.assertNotNull(item.get("Reviews").getM().get("FiveStar").getL().get(0).getM().get("reviewer"));
            Assert.assertNotNull(item.get("title"));
        }

        // test attributesToGet
        sr.setProjectionExpression(null);
        sr.setExpressionAttributeNames(null);
        List<String> attrs = new ArrayList<>();
        attrs.add("title");
        attrs.add("Reviews.FiveStar[0].reviewer");
        sr.setAttributesToGet(attrs);
        phoenixResult = phoenixDBClient.scan(sr);
        dynamoResult = amazonDynamoDB.scan(sr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        for (Map<String, AttributeValue> item : phoenixResult.getItems()) {
            Assert.assertNotNull(item.get("Reviews").getM().get("FiveStar").getL().get(0).getM().get("reviewer"));
            Assert.assertNotNull(item.get("title"));
        }
        Assert.assertEquals(dynamoResult.getScannedCount(), phoenixResult.getScannedCount());
    }

    @Test(timeout = 120000)
    public void testScanWithTopLevelAttributeFilter() {
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

        ScanRequest sr = new ScanRequest(tableName);
        sr.setFilterExpression("#2 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "title");
        sr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", new AttributeValue().withS("Title3"));
        sr.setExpressionAttributeValues(exprAttrVal);
        ScanResult phoenixResult = phoenixDBClient.scan(sr);
        ScanResult dynamoResult = amazonDynamoDB.scan(sr);
        Assert.assertEquals(dynamoResult.getItems(), phoenixResult.getItems());
        Assert.assertEquals(dynamoResult.getScannedCount(), phoenixResult.getScannedCount());
    }

    @Test(timeout = 120000)
    public void testScanWithNestedAttributeFilter() {
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

        ScanRequest sr = new ScanRequest(tableName);
        sr.setFilterExpression("#1.#2[0].#3 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "reviewer");
        sr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", new AttributeValue().withS("Carl"));
        sr.setExpressionAttributeValues(exprAttrVal);
        ScanResult phoenixResult = phoenixDBClient.scan(sr);
        ScanResult dynamoResult = amazonDynamoDB.scan(sr);
        Assert.assertEquals(dynamoResult.getItems(), phoenixResult.getItems());
        Assert.assertEquals(dynamoResult.getScannedCount(), phoenixResult.getScannedCount());
    }

    /**
     * Dynamo seems to return results in the order of conditions in the filter expressions.
     * Phoenix returns results in order of PKs.
     * To test pagination, use a filter expression where the results satisfying the conditions
     * in order are also ordered by the keys.
     */
    @Test(timeout = 120000)
    public void testScanWithPagination() {
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

        ScanRequest sr = new ScanRequest(tableName);
        sr.setFilterExpression("(#4 > :v4 AND #5 < :v5) OR #1.#2[0].#3 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "reviewer");
        exprAttrNames.put("#4", "attr_0");
        exprAttrNames.put("#5", "attr_1");
        sr.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", new AttributeValue().withS("Drake"));
        exprAttrVal.put(":v4", new AttributeValue().withS("A"));
        exprAttrVal.put(":v5", new AttributeValue().withN("3"));
        sr.setExpressionAttributeValues(exprAttrVal);
        sr.setLimit(1);
        ScanResult phoenixResult, dynamoResult;
        int paginationCount = 0;
        do {
            phoenixResult = phoenixDBClient.scan(sr);
            dynamoResult = amazonDynamoDB.scan(sr);
            Assert.assertEquals(dynamoResult.getItems(), phoenixResult.getItems());
            paginationCount++;
            sr.setExclusiveStartKey(phoenixResult.getLastEvaluatedKey());
        } while (phoenixResult.getCount() > 0);
        // 1 more than total number of results expected
        Assert.assertEquals(3, paginationCount);
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
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("C"));
        item.put("attr_1", new AttributeValue().withN("3"));
        item.put("Id1", new AttributeValue().withN("11"));
        item.put("Id2", new AttributeValue().withN("1000.10"));
        item.put("title", new AttributeValue().withS("Title3"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Carl"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(new AttributeValue().withM(reviewMap1)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", new AttributeValue().withS("D"));
        item.put("attr_1", new AttributeValue().withN("4"));
        item.put("Id1", new AttributeValue().withN("-23"));
        item.put("Id2", new AttributeValue().withN("99.10"));
        item.put("title", new AttributeValue().withS("Title40"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Drake"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(new AttributeValue().withM(reviewMap1)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        return item;
    }
}
