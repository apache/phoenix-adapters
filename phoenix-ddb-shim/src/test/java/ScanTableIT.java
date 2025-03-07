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
import org.apache.phoenix.ddb.PhoenixDBClientV2;
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
import java.util.UUID;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ScanTableIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScanTableIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

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
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.projectionExpression("title, #0.#1[0].#2");
        Map<String,String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Reviews");
        exprAttrNames.put("#1", "FiveStar");
        exprAttrNames.put("#2", "reviewer");
        sr.expressionAttributeNames(exprAttrNames);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        // dynamo does not guarantee ordering of partition keys in Scan, so only check count
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (Map<String, AttributeValue> item : phoenixResult.items()) {
            Assert.assertNotNull(item.get("Reviews").m().get("FiveStar").l().get(0).m().get("reviewer"));
            Assert.assertNotNull(item.get("title"));
        }
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void testScanAllRowsWithProjection() {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.projectionExpression("title, #0.#1[0].#2");
        Map<String,String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Reviews");
        exprAttrNames.put("#1", "FiveStar");
        exprAttrNames.put("#2", "reviewer");
        sr.expressionAttributeNames(exprAttrNames);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        // dynamo does not guarantee ordering of partition keys in Scan, so only check count
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (Map<String, AttributeValue> item : phoenixResult.items()) {
            Assert.assertNotNull(item.get("Reviews").m().get("FiveStar").l().get(0).m().get("reviewer"));
            Assert.assertNotNull(item.get("title"));
        }

        // test attributesToGet
        sr.projectionExpression(null);
        sr.expressionAttributeNames(null);
        List<String> attrs = new ArrayList<>();
        attrs.add("title");
        attrs.add("Reviews.FiveStar[0].reviewer");
        sr.attributesToGet(attrs);
        phoenixResult = phoenixDBClientV2.scan(sr.build());
        dynamoResult = dynamoDbClient.scan(sr.build());
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        for (Map<String, AttributeValue> item : phoenixResult.items()) {
            Assert.assertNotNull(item.get("Reviews").m().get("FiveStar").l().get(0).m().get("reviewer"));
            Assert.assertNotNull(item.get("title"));
        }
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void testScanWithTopLevelAttributeFilter() {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.filterExpression("#2 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "title");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().s("Title3").build());
        sr.expressionAttributeValues(exprAttrVal);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    @Test(timeout = 120000)
    public void testScanWithNestedAttributeFilter() {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.filterExpression("#1.#2[0].#3 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "reviewer");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().s("Carl").build());
        sr.expressionAttributeValues(exprAttrVal);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        ScanResponse dynamoResult = dynamoDbClient.scan(sr.build());
        Assert.assertEquals(dynamoResult.items(), phoenixResult.items());
        Assert.assertEquals(dynamoResult.scannedCount(), phoenixResult.scannedCount());
    }

    /**
     * Dynamo seems to return results in the order of conditions in the filter expressions.
     * Phoenix returns results in order of PKs.
     * To test pagination, use a filter expression where the results satisfying the conditions
     * in order are also ordered by the keys.
     */
    @Test(timeout = 120000)
    public void testScanWithFilterAndPagination() {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.filterExpression("(#4 > :v4 AND #5 < :v5) OR #1.#2[0].#3 = :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "reviewer");
        exprAttrNames.put("#4", "attr_0");
        exprAttrNames.put("#5", "attr_1");
        sr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().s("Drake").build());
        exprAttrVal.put(":v4", AttributeValue.builder().s("A").build());
        exprAttrVal.put(":v5", AttributeValue.builder().n("3").build());
        sr.expressionAttributeValues(exprAttrVal);
        sr.limit(1);
        ScanResponse phoenixResult, dynamoResult;
        int paginationCount = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            dynamoResult = dynamoDbClient.scan(sr.build());
            Assert.assertEquals(dynamoResult.items(), phoenixResult.items());
            paginationCount++;
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        // 1 more than total number of results expected
        Assert.assertEquals(3, paginationCount);
    }


    @Test(timeout = 120000)
    public void testScanWithPaginationNoFilter() {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.limit(1);
        ScanResponse phoenixResult;
        int count = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            count += phoenixResult.count();
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        Assert.assertEquals(4, count);
    }

    @Test(timeout = 120000)
    public void testScanWithPaginationNoSortKeyNoFilter() {
        //create table
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put
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

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.limit(1);
        ScanResponse phoenixResult;
        int count = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            count += phoenixResult.count();
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        Assert.assertEquals(4, count);
    }

    @Test(timeout = 120000)
    public void testScanFullTable() {
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        for (int i=0; i<10;i++) {
            String randomPk = "A" + i % 4;
            String randomValue = UUID.randomUUID().toString();
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("PK1", AttributeValue.builder().s(randomPk).build());
            item.put("PK2", AttributeValue.builder().n(String.valueOf(i)).build());
            item.put("VAL", AttributeValue.builder().s(randomValue).build());
            PutItemRequest request = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();
            phoenixDBClientV2.putItem(request);
        }
        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName);
        sr.limit(1);
        ScanResponse phoenixResult;
        int count = 0;
        do {
            phoenixResult = phoenixDBClientV2.scan(sr.build());
            count += phoenixResult.count();
            sr.exclusiveStartKey(phoenixResult.lastEvaluatedKey());
        } while (phoenixResult.count() > 0);
        Assert.assertEquals(10, count);
    }

    private static Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("A").build());
        item.put("attr_1", AttributeValue.builder().n("1").build());
        item.put("Id1", AttributeValue.builder().n("-5").build());
        item.put("Id2", AttributeValue.builder().n("10.10").build());
        item.put("title", AttributeValue.builder().s("Title1").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Alice").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    private static Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("B").build());
        item.put("attr_1", AttributeValue.builder().n("2").build());
        item.put("Id1", AttributeValue.builder().n("-15").build());
        item.put("Id2", AttributeValue.builder().n("150.10").build());
        item.put("title", AttributeValue.builder().s("Title2").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Bob1").build());
        Map<String, AttributeValue> reviewMap2 = new HashMap<>();
        reviewMap2.put("reviewer", AttributeValue.builder().s("Bob2").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(
                AttributeValue.builder().m(reviewMap1).build(),
                AttributeValue.builder().m(reviewMap2).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    private static Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("C").build());
        item.put("attr_1", AttributeValue.builder().n("3").build());
        item.put("Id1", AttributeValue.builder().n("11").build());
        item.put("Id2", AttributeValue.builder().n("1000.10").build());
        item.put("title", AttributeValue.builder().s("Title3").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Carl").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    private static Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("attr_0", AttributeValue.builder().s("D").build());
        item.put("attr_1", AttributeValue.builder().n("4").build());
        item.put("Id1", AttributeValue.builder().n("-23").build());
        item.put("Id2", AttributeValue.builder().n("99.10").build());
        item.put("title", AttributeValue.builder().s("Title40").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Drake").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }
}
