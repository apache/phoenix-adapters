import java.sql.DriverManager;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
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

public class ScanTableIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanTableIT.class);

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
    public void testScanAllRowsNoSortKey() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
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
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
    public void testScanWithTopLevelAttributeFilter() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
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
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
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
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
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

    @Test(timeout = 120000)
    public void testScanWithSegments() {
        //create table
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);

        //put
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        PutItemRequest putItemRequest4 = PutItemRequest.builder().tableName(tableName).item(getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);

        ScanRequest.Builder sr = ScanRequest.builder().tableName(tableName).segment(0).totalSegments(2);
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr.build());
        Assert.assertEquals(4, phoenixResult.items().size());

        sr = ScanRequest.builder().tableName(tableName).segment(1).totalSegments(2);
        phoenixResult = phoenixDBClientV2.scan(sr.build());
        Assert.assertEquals(0, phoenixResult.items().size());
    }

    @Test(timeout = 120000)
    public void testScanWithBeginsWithFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("item1").build());
        item1.put("stringAttr", AttributeValue.builder().s("prefix_match").build());
        item1.put("binaryAttr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3, 4, 5}))
                        .build());
        item1.put("numberAttr", AttributeValue.builder().n("123").build());
        item1.put("category", AttributeValue.builder().s("electronics").build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("item2").build());
        item2.put("stringAttr", AttributeValue.builder().s("prefix_nomatch").build());
        item2.put("binaryAttr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 9, 8, 7}))
                        .build());
        item2.put("numberAttr", AttributeValue.builder().n("456").build());
        item2.put("category", AttributeValue.builder().s("books").build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("item3").build());
        item3.put("stringAttr", AttributeValue.builder().s("different_prefix").build());
        item3.put("binaryAttr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {5, 4, 3, 2, 1}))
                        .build());
        item3.put("numberAttr", AttributeValue.builder().n("789").build());
        item3.put("category", AttributeValue.builder().s("electronics").build());

        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put("pk", AttributeValue.builder().s("item4").build());
        item4.put("stringAttr", AttributeValue.builder().s("prefix_another").build());
        item4.put("binaryAttr",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3, 9, 9}))
                        .build());
        item4.put("numberAttr", AttributeValue.builder().n("101").build());
        item4.put("category", AttributeValue.builder().s("books").build());

        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(item4).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        // Test 1: begins_with with String attribute - positive case (using attribute name alias)
        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        sr1.filterExpression("begins_with(#strAttr, :prefix)");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#strAttr", "stringAttr");
        sr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":prefix", AttributeValue.builder().s("prefix_").build());
        sr1.expressionAttributeValues(exprAttrVal1);

        ScanResponse phoenixResult1 = phoenixDBClientV2.scan(sr1.build());
        ScanResponse dynamoResult1 = dynamoDbClient.scan(sr1.build());

        Assert.assertEquals(3, dynamoResult1.count().intValue());
        Assert.assertEquals(dynamoResult1.count(), phoenixResult1.count());
        Assert.assertEquals(dynamoResult1.scannedCount(), phoenixResult1.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult1.items()),
                sortItemsByPk(phoenixResult1.items()));

        // Test 2: begins_with with Binary attribute - positive case
        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        sr2.filterExpression("begins_with(binaryAttr, :binPrefix)");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":binPrefix",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        sr2.expressionAttributeValues(exprAttrVal2);

        ScanResponse phoenixResult2 = phoenixDBClientV2.scan(sr2.build());
        ScanResponse dynamoResult2 = dynamoDbClient.scan(sr2.build());

        Assert.assertEquals(3, dynamoResult2.count().intValue());
        Assert.assertEquals(dynamoResult2.count(), phoenixResult2.count());
        Assert.assertEquals(dynamoResult2.scannedCount(), phoenixResult2.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult2.items()),
                sortItemsByPk(phoenixResult2.items()));

        // Test 3: begins_with with String attribute - negative case (no matches)
        ScanRequest.Builder sr3 = ScanRequest.builder().tableName(tableName);
        sr3.filterExpression("begins_with(stringAttr, :noMatch)");
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":noMatch", AttributeValue.builder().s("nonexistent").build());
        sr3.expressionAttributeValues(exprAttrVal3);

        ScanResponse phoenixResult3 = phoenixDBClientV2.scan(sr3.build());
        ScanResponse dynamoResult3 = dynamoDbClient.scan(sr3.build());

        Assert.assertEquals(0, dynamoResult3.count().intValue());
        Assert.assertEquals(dynamoResult3.count(), phoenixResult3.count());
        Assert.assertEquals(dynamoResult3.scannedCount(), phoenixResult3.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult3.items()),
                sortItemsByPk(phoenixResult3.items()));

        // Test 4: begins_with combined with AND condition
        ScanRequest.Builder sr4 = ScanRequest.builder().tableName(tableName);
        sr4.filterExpression("begins_with(stringAttr, :prefix) AND category = :cat");
        Map<String, AttributeValue> exprAttrVal4 = new HashMap<>();
        exprAttrVal4.put(":prefix", AttributeValue.builder().s("prefix_").build());
        exprAttrVal4.put(":cat", AttributeValue.builder().s("electronics").build());
        sr4.expressionAttributeValues(exprAttrVal4);

        ScanResponse phoenixResult4 = phoenixDBClientV2.scan(sr4.build());
        ScanResponse dynamoResult4 = dynamoDbClient.scan(sr4.build());

        Assert.assertEquals(1, dynamoResult4.count().intValue());
        Assert.assertEquals(dynamoResult4.count(), phoenixResult4.count());
        Assert.assertEquals(dynamoResult4.scannedCount(), phoenixResult4.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult4.items()),
                sortItemsByPk(phoenixResult4.items()));

        // Test 5: begins_with combined with OR condition
        ScanRequest.Builder sr5 = ScanRequest.builder().tableName(tableName);
        sr5.filterExpression(
                "begins_with(stringAttr, :prefix1) OR begins_with(stringAttr, :prefix2)");
        Map<String, AttributeValue> exprAttrVal5 = new HashMap<>();
        exprAttrVal5.put(":prefix1", AttributeValue.builder().s("prefix_").build());
        exprAttrVal5.put(":prefix2", AttributeValue.builder().s("different_").build());
        sr5.expressionAttributeValues(exprAttrVal5);

        ScanResponse phoenixResult5 = phoenixDBClientV2.scan(sr5.build());
        ScanResponse dynamoResult5 = dynamoDbClient.scan(sr5.build());

        Assert.assertEquals(4, dynamoResult5.count().intValue());
        Assert.assertEquals(dynamoResult5.count(), phoenixResult5.count());
        Assert.assertEquals(dynamoResult5.scannedCount(), phoenixResult5.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult5.items()),
                sortItemsByPk(phoenixResult5.items()));

        // Test 6: begins_with with attribute names using expression attribute names
        ScanRequest.Builder sr6 = ScanRequest.builder().tableName(tableName);
        sr6.filterExpression("begins_with(#attr, :prefix)");
        Map<String, String> exprAttrNames6 = new HashMap<>();
        exprAttrNames6.put("#attr", "stringAttr");
        sr6.expressionAttributeNames(exprAttrNames6);
        Map<String, AttributeValue> exprAttrVal6 = new HashMap<>();
        exprAttrVal6.put(":prefix", AttributeValue.builder().s("prefix_").build());
        sr6.expressionAttributeValues(exprAttrVal6);

        ScanResponse phoenixResult6 = phoenixDBClientV2.scan(sr6.build());
        ScanResponse dynamoResult6 = dynamoDbClient.scan(sr6.build());

        Assert.assertEquals(3, dynamoResult6.count().intValue());
        Assert.assertEquals(dynamoResult6.count(), phoenixResult6.count());
        Assert.assertEquals(dynamoResult6.scannedCount(), phoenixResult6.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult6.items()),
                sortItemsByPk(phoenixResult6.items()));

        // Test 7: Test begins_with with unsupported data type (Number) - should fail
        ScanRequest.Builder sr7 = ScanRequest.builder().tableName(tableName);
        sr7.filterExpression("begins_with(numberAttr, :numPrefix)");
        Map<String, AttributeValue> exprAttrVal7 = new HashMap<>();
        exprAttrVal7.put(":numPrefix", AttributeValue.builder().n("12").build());
        sr7.expressionAttributeValues(exprAttrVal7);

        ScanResponse phoenixResult7;
        ScanResponse dynamoResult7;
        try {
            phoenixResult7 = phoenixDBClientV2.scan(sr7.build());
            throw new RuntimeException("Should have thrown an exception for invalid data type");
        } catch (DynamoDbException e) {
            LOGGER.info("begins_with with Number data type failed as expected: {}", e.getMessage());
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            dynamoResult7 = dynamoDbClient.scan(sr7.build());
            throw new RuntimeException("Should have thrown an exception for invalid data type");
        } catch (DynamoDbException e) {
            LOGGER.info("begins_with with Number data type failed as expected: {}", e.getMessage());
            Assert.assertEquals(400, e.statusCode());
        }

        // Test 8: begins_with with exact match (prefix equals entire string)
        ScanRequest.Builder sr8 = ScanRequest.builder().tableName(tableName);
        sr8.filterExpression("begins_with(stringAttr, :exactMatch)");
        Map<String, AttributeValue> exprAttrVal8 = new HashMap<>();
        exprAttrVal8.put(":exactMatch", AttributeValue.builder().s("prefix_match").build());
        sr8.expressionAttributeValues(exprAttrVal8);

        ScanResponse phoenixResult8 = phoenixDBClientV2.scan(sr8.build());
        ScanResponse dynamoResult8 = dynamoDbClient.scan(sr8.build());

        Assert.assertEquals(1, dynamoResult8.count().intValue());
        Assert.assertEquals(dynamoResult8.count(), phoenixResult8.count());
        Assert.assertEquals(dynamoResult8.scannedCount(), phoenixResult8.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult8.items()),
                sortItemsByPk(phoenixResult8.items()));

        // Test 9: begins_with with empty prefix
        ScanRequest.Builder sr9 = ScanRequest.builder().tableName(tableName);
        sr9.filterExpression("begins_with(stringAttr, :emptyPrefix)");
        Map<String, AttributeValue> exprAttrVal9 = new HashMap<>();
        exprAttrVal9.put(":emptyPrefix", AttributeValue.builder().s("").build());
        sr9.expressionAttributeValues(exprAttrVal9);

        ScanResponse phoenixResult9 = phoenixDBClientV2.scan(sr9.build());
        ScanResponse dynamoResult9 = dynamoDbClient.scan(sr9.build());

        Assert.assertEquals(4, dynamoResult9.count().intValue());
        Assert.assertEquals(dynamoResult9.count(), phoenixResult9.count());
        Assert.assertEquals(dynamoResult9.scannedCount(), phoenixResult9.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult9.items()),
                sortItemsByPk(phoenixResult9.items()));

        // Test 10: begins_with with binary data - more specific prefix
        ScanRequest.Builder sr10 = ScanRequest.builder().tableName(tableName);
        sr10.filterExpression("begins_with(binaryAttr, :specificBinPrefix)");
        Map<String, AttributeValue> exprAttrVal10 = new HashMap<>();
        exprAttrVal10.put(":specificBinPrefix",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build());
        sr10.expressionAttributeValues(exprAttrVal10);

        ScanResponse phoenixResult10 = phoenixDBClientV2.scan(sr10.build());
        ScanResponse dynamoResult10 = dynamoDbClient.scan(sr10.build());

        Assert.assertEquals(2, dynamoResult10.count().intValue());
        Assert.assertEquals(dynamoResult10.count(), phoenixResult10.count());
        Assert.assertEquals(dynamoResult10.scannedCount(), phoenixResult10.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult10.items()),
                sortItemsByPk(phoenixResult10.items()));

        // Test 11: begins_with for non-existent attribute
        ScanRequest.Builder sr11 = ScanRequest.builder().tableName(tableName);
        sr11.filterExpression("NOT begins_with(nonExistentAttr, :specificBinPrefix)");
        Map<String, AttributeValue> exprAttrVal11 = new HashMap<>();
        exprAttrVal11.put(":specificBinPrefix",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2, 3})).build());
        sr11.expressionAttributeValues(exprAttrVal11);

        ScanResponse phoenixResult11 = phoenixDBClientV2.scan(sr11.build());
        ScanResponse dynamoResult11 = dynamoDbClient.scan(sr11.build());

        Assert.assertEquals(4, dynamoResult11.count().intValue());
        Assert.assertEquals(dynamoResult11.count(), phoenixResult11.count());
        Assert.assertEquals(dynamoResult11.scannedCount(), phoenixResult11.scannedCount());
        Assert.assertEquals(sortItemsByPk(dynamoResult11.items()),
                sortItemsByPk(phoenixResult11.items()));
    }

    @Test(timeout = 120000)
    public void testScanWithContainsFilter() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, null,
                        null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        putItemsForScanWithContainsFilter(tableName);

        // Test 1: contains() with String attribute - substring search
        ScanRequest.Builder sr1 = ScanRequest.builder().tableName(tableName);
        sr1.filterExpression("contains(#strAttr, :substring)");
        Map<String, String> exprAttrNames1 = new HashMap<>();
        exprAttrNames1.put("#strAttr", "stringAttr");
        sr1.expressionAttributeNames(exprAttrNames1);
        Map<String, AttributeValue> exprAttrVal1 = new HashMap<>();
        exprAttrVal1.put(":substring", AttributeValue.builder().s("world").build());
        sr1.expressionAttributeValues(exprAttrVal1);

        ScanResponse phoenixResult1 = phoenixDBClientV2.scan(sr1.build());
        ScanResponse dynamoResult1 = dynamoDbClient.scan(sr1.build());

        Assert.assertEquals(4, dynamoResult1.count().intValue());
        Assert.assertEquals(dynamoResult1.count(), phoenixResult1.count());
        Assert.assertEquals(dynamoResult1.scannedCount(), phoenixResult1.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult1.items()),
                sortItemsByPk(phoenixResult1.items())));

        // Test 2: contains() with String Set - element search
        ScanRequest.Builder sr2 = ScanRequest.builder().tableName(tableName);
        sr2.filterExpression("contains(stringSet, :element)");
        Map<String, AttributeValue> exprAttrVal2 = new HashMap<>();
        exprAttrVal2.put(":element", AttributeValue.builder().s("apple").build());
        sr2.expressionAttributeValues(exprAttrVal2);

        ScanResponse phoenixResult2 = phoenixDBClientV2.scan(sr2.build());
        ScanResponse dynamoResult2 = dynamoDbClient.scan(sr2.build());

        Assert.assertEquals(4, dynamoResult2.count().intValue());
        Assert.assertEquals(dynamoResult2.count(), phoenixResult2.count());
        Assert.assertEquals(dynamoResult2.scannedCount(), phoenixResult2.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult2.items()),
                sortItemsByPk(phoenixResult2.items())));

        // Test 3: contains() with Number Set - element search
        ScanRequest.Builder sr3 = ScanRequest.builder().tableName(tableName);
        sr3.filterExpression("contains(numberSet, :number)");
        Map<String, AttributeValue> exprAttrVal3 = new HashMap<>();
        exprAttrVal3.put(":number", AttributeValue.builder().n("1").build());
        sr3.expressionAttributeValues(exprAttrVal3);

        ScanResponse phoenixResult3 = phoenixDBClientV2.scan(sr3.build());
        ScanResponse dynamoResult3 = dynamoDbClient.scan(sr3.build());

        Assert.assertEquals(4, dynamoResult3.count().intValue());
        Assert.assertEquals(dynamoResult3.count(), phoenixResult3.count());
        Assert.assertEquals(dynamoResult3.scannedCount(), phoenixResult3.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult3.items()),
                sortItemsByPk(phoenixResult3.items())));

        // Test 4: contains() with Binary Set - element search
        ScanRequest.Builder sr4 = ScanRequest.builder().tableName(tableName);
        sr4.filterExpression("contains(binarySet, :binaryElement)");
        Map<String, AttributeValue> exprAttrVal4 = new HashMap<>();
        exprAttrVal4.put(":binaryElement",
                AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        sr4.expressionAttributeValues(exprAttrVal4);

        ScanResponse phoenixResult4 = phoenixDBClientV2.scan(sr4.build());
        ScanResponse dynamoResult4 = dynamoDbClient.scan(sr4.build());

        Assert.assertEquals(4, dynamoResult4.count().intValue());
        Assert.assertEquals(dynamoResult4.count(), phoenixResult4.count());
        Assert.assertEquals(dynamoResult4.scannedCount(), phoenixResult4.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult4.items()),
                sortItemsByPk(phoenixResult4.items())));

        // Test 5: contains() with List attribute - element search (String)
        ScanRequest.Builder sr5 = ScanRequest.builder().tableName(tableName);
        sr5.filterExpression("contains(listAttr, :listElement)");
        Map<String, AttributeValue> exprAttrVal5 = new HashMap<>();
        exprAttrVal5.put(":listElement", AttributeValue.builder().s("red").build());
        sr5.expressionAttributeValues(exprAttrVal5);

        ScanResponse phoenixResult5 = phoenixDBClientV2.scan(sr5.build());
        ScanResponse dynamoResult5 = dynamoDbClient.scan(sr5.build());

        Assert.assertEquals(3, dynamoResult5.count().intValue());
        Assert.assertEquals(dynamoResult5.count(), phoenixResult5.count());
        Assert.assertEquals(dynamoResult5.scannedCount(), phoenixResult5.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult5.items()),
                sortItemsByPk(phoenixResult5.items())));

        // Test 6: contains() with List attribute - element search (Number)
        ScanRequest.Builder sr6 = ScanRequest.builder().tableName(tableName);
        sr6.filterExpression("contains(listAttr, :listElement)");
        Map<String, AttributeValue> exprAttrVal6 = new HashMap<>();
        exprAttrVal6.put(":listElement", AttributeValue.builder().n("42").build());
        sr6.expressionAttributeValues(exprAttrVal6);

        ScanResponse phoenixResult6 = phoenixDBClientV2.scan(sr6.build());
        ScanResponse dynamoResult6 = dynamoDbClient.scan(sr6.build());

        Assert.assertEquals(2, dynamoResult6.count().intValue());
        Assert.assertEquals(dynamoResult6.count(), phoenixResult6.count());
        Assert.assertEquals(dynamoResult6.scannedCount(), phoenixResult6.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult6.items()),
                sortItemsByPk(phoenixResult6.items())));

        // Test 7: contains() with String attribute - negative case (no matches)
        ScanRequest.Builder sr7 = ScanRequest.builder().tableName(tableName);
        sr7.filterExpression("contains(stringAttr, :noMatch)");
        Map<String, AttributeValue> exprAttrVal7 = new HashMap<>();
        exprAttrVal7.put(":noMatch", AttributeValue.builder().s("nonexistent").build());
        sr7.expressionAttributeValues(exprAttrVal7);

        ScanResponse phoenixResult7 = phoenixDBClientV2.scan(sr7.build());
        ScanResponse dynamoResult7 = dynamoDbClient.scan(sr7.build());

        Assert.assertEquals(0, dynamoResult7.count().intValue());
        Assert.assertEquals(dynamoResult7.count(), phoenixResult7.count());
        Assert.assertEquals(dynamoResult7.scannedCount(), phoenixResult7.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult7.items()),
                sortItemsByPk(phoenixResult7.items())));

        // Test 8: contains() combined with AND condition
        ScanRequest.Builder sr8 = ScanRequest.builder().tableName(tableName);
        sr8.filterExpression("contains(stringAttr, :substring) AND category = :cat");
        Map<String, AttributeValue> exprAttrVal8 = new HashMap<>();
        exprAttrVal8.put(":substring", AttributeValue.builder().s("world").build());
        exprAttrVal8.put(":cat", AttributeValue.builder().s("electronics").build());
        sr8.expressionAttributeValues(exprAttrVal8);

        ScanResponse phoenixResult8 = phoenixDBClientV2.scan(sr8.build());
        ScanResponse dynamoResult8 = dynamoDbClient.scan(sr8.build());

        Assert.assertEquals(1, dynamoResult8.count().intValue());
        Assert.assertEquals(dynamoResult8.count(), phoenixResult8.count());
        Assert.assertEquals(dynamoResult8.scannedCount(), phoenixResult8.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult8.items()),
                sortItemsByPk(phoenixResult8.items())));

        // Test 9: contains() combined with OR condition
        ScanRequest.Builder sr9 = ScanRequest.builder().tableName(tableName);
        sr9.filterExpression("contains(stringSet, :fruit1) OR contains(stringSet, :fruit2)");
        Map<String, AttributeValue> exprAttrVal9 = new HashMap<>();
        exprAttrVal9.put(":fruit1", AttributeValue.builder().s("apple").build());
        exprAttrVal9.put(":fruit2", AttributeValue.builder().s("orange").build());
        sr9.expressionAttributeValues(exprAttrVal9);

        ScanResponse phoenixResult9 = phoenixDBClientV2.scan(sr9.build());
        ScanResponse dynamoResult9 = dynamoDbClient.scan(sr9.build());

        Assert.assertEquals(6, dynamoResult9.count().intValue());
        Assert.assertEquals(dynamoResult9.count(), phoenixResult9.count());
        Assert.assertEquals(dynamoResult9.scannedCount(), phoenixResult9.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult9.items()),
                sortItemsByPk(phoenixResult9.items())));

        // Test 10: contains() with expression attribute names
        ScanRequest.Builder sr10 = ScanRequest.builder().tableName(tableName);
        sr10.filterExpression("contains(#attr, :substring)");
        Map<String, String> exprAttrNames10 = new HashMap<>();
        exprAttrNames10.put("#attr", "stringAttr");
        sr10.expressionAttributeNames(exprAttrNames10);
        Map<String, AttributeValue> exprAttrVal10 = new HashMap<>();
        exprAttrVal10.put(":substring", AttributeValue.builder().s("test").build());
        sr10.expressionAttributeValues(exprAttrVal10);

        ScanResponse phoenixResult10 = phoenixDBClientV2.scan(sr10.build());
        ScanResponse dynamoResult10 = dynamoDbClient.scan(sr10.build());

        Assert.assertEquals(4, dynamoResult10.count().intValue());
        Assert.assertEquals(dynamoResult10.count(), phoenixResult10.count());
        Assert.assertEquals(dynamoResult10.scannedCount(), phoenixResult10.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult10.items()),
                sortItemsByPk(phoenixResult10.items())));

        // Test 11: contains() with case-sensitive string matching
        ScanRequest.Builder sr11 = ScanRequest.builder().tableName(tableName);
        sr11.filterExpression("contains(stringAttr, :substring)");
        Map<String, AttributeValue> exprAttrVal11 = new HashMap<>();
        exprAttrVal11.put(":substring", AttributeValue.builder().s("WORLD").build()); // uppercase
        sr11.expressionAttributeValues(exprAttrVal11);

        ScanResponse phoenixResult11 = phoenixDBClientV2.scan(sr11.build());
        ScanResponse dynamoResult11 = dynamoDbClient.scan(sr11.build());

        Assert.assertEquals(0, dynamoResult11.count().intValue()); // case-sensitive, no matches
        Assert.assertEquals(dynamoResult11.count(), phoenixResult11.count());
        Assert.assertEquals(dynamoResult11.scannedCount(), phoenixResult11.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult11.items()),
                sortItemsByPk(phoenixResult11.items())));

        // Test 12: contains() with Set - element not found
        ScanRequest.Builder sr12 = ScanRequest.builder().tableName(tableName);
        sr12.filterExpression("contains(stringSet, :element)");
        Map<String, AttributeValue> exprAttrVal12 = new HashMap<>();
        exprAttrVal12.put(":element", AttributeValue.builder().s("nonexistent").build());
        sr12.expressionAttributeValues(exprAttrVal12);

        ScanResponse phoenixResult12 = phoenixDBClientV2.scan(sr12.build());
        ScanResponse dynamoResult12 = dynamoDbClient.scan(sr12.build());

        Assert.assertEquals(0, dynamoResult12.count().intValue());
        Assert.assertEquals(dynamoResult12.count(), phoenixResult12.count());
        Assert.assertEquals(dynamoResult12.scannedCount(), phoenixResult12.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult12.items()),
                sortItemsByPk(phoenixResult12.items())));

        // Test 13: contains() with Number Set - partial number match (should fail)
        ScanRequest.Builder sr13 = ScanRequest.builder().tableName(tableName);
        sr13.filterExpression("contains(numberSet, :partialNumber)");
        Map<String, AttributeValue> exprAttrVal13 = new HashMap<>();
        exprAttrVal13.put(":partialNumber",
                AttributeValue.builder().n("10").build()); // Looking for exact match
        sr13.expressionAttributeValues(exprAttrVal13);

        ScanResponse phoenixResult13 = phoenixDBClientV2.scan(sr13.build());
        ScanResponse dynamoResult13 = dynamoDbClient.scan(sr13.build());

        Assert.assertEquals(2,
                dynamoResult13.count().intValue()); // item4 and item6 have "10" in numberSet
        Assert.assertEquals(dynamoResult13.count(), phoenixResult13.count());
        Assert.assertEquals(dynamoResult13.scannedCount(), phoenixResult13.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult13.items()),
                sortItemsByPk(phoenixResult13.items())));

        // Test 14: contains() with unsupported data type (Number) on String attribute - should fail
        ScanRequest.Builder sr14 = ScanRequest.builder().tableName(tableName);
        sr14.filterExpression("contains(numberAttr, :substring)");
        Map<String, AttributeValue> exprAttrVal14 = new HashMap<>();
        exprAttrVal14.put(":substring", AttributeValue.builder().s("10").build());
        sr14.expressionAttributeValues(exprAttrVal14);

        ScanResponse phoenixResult14 = phoenixDBClientV2.scan(sr14.build());
        ScanResponse dynamoResult14 = dynamoDbClient.scan(sr14.build());

        Assert.assertEquals(0, dynamoResult14.count().intValue());
        Assert.assertEquals(dynamoResult14.count(), phoenixResult14.count());
        Assert.assertEquals(dynamoResult14.scannedCount(), phoenixResult14.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult14.items()),
                sortItemsByPk(phoenixResult14.items())));

        // Test 15: contains() with NOT operator
        ScanRequest.Builder sr15 = ScanRequest.builder().tableName(tableName);
        sr15.filterExpression("NOT contains(stringAttr, :substring)");
        Map<String, AttributeValue> exprAttrVal15 = new HashMap<>();
        exprAttrVal15.put(":substring", AttributeValue.builder().s("world").build());
        sr15.expressionAttributeValues(exprAttrVal15);

        ScanResponse phoenixResult15 = phoenixDBClientV2.scan(sr15.build());
        ScanResponse dynamoResult15 = dynamoDbClient.scan(sr15.build());

        Assert.assertEquals(4,
                dynamoResult15.count().intValue()); // items that don't contain "world"
        Assert.assertEquals(dynamoResult15.count(), phoenixResult15.count());
        Assert.assertEquals(dynamoResult15.scannedCount(), phoenixResult15.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult15.items()),
                sortItemsByPk(phoenixResult15.items())));

        // Test 16: contains() with empty string
        ScanRequest.Builder sr16 = ScanRequest.builder().tableName(tableName);
        sr16.filterExpression("contains(stringAttr, :emptyString)");
        Map<String, AttributeValue> exprAttrVal16 = new HashMap<>();
        exprAttrVal16.put(":emptyString", AttributeValue.builder().s("").build());
        sr16.expressionAttributeValues(exprAttrVal16);

        ScanResponse phoenixResult16 = phoenixDBClientV2.scan(sr16.build());
        ScanResponse dynamoResult16 = dynamoDbClient.scan(sr16.build());

        Assert.assertEquals(8,
                dynamoResult16.count().intValue()); // empty string is contained in all strings
        Assert.assertEquals(dynamoResult16.count(), phoenixResult16.count());
        Assert.assertEquals(dynamoResult16.scannedCount(), phoenixResult16.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult16.items()),
                sortItemsByPk(phoenixResult16.items())));

        // Test 17: contains() with multiple conditions using parentheses
        ScanRequest.Builder sr17 = ScanRequest.builder().tableName(tableName);
        sr17.filterExpression(
                "(contains(stringAttr, :sub1) OR contains(stringAttr, :sub2)) AND category = :cat");
        Map<String, AttributeValue> exprAttrVal17 = new HashMap<>();
        exprAttrVal17.put(":sub1", AttributeValue.builder().s("hello").build());
        exprAttrVal17.put(":sub2", AttributeValue.builder().s("testing").build());
        exprAttrVal17.put(":cat", AttributeValue.builder().s("electronics").build());
        sr17.expressionAttributeValues(exprAttrVal17);

        ScanResponse phoenixResult17 = phoenixDBClientV2.scan(sr17.build());
        ScanResponse dynamoResult17 = dynamoDbClient.scan(sr17.build());

        Assert.assertEquals(3, dynamoResult17.count().intValue());
        Assert.assertEquals(dynamoResult17.count(), phoenixResult17.count());
        Assert.assertEquals(dynamoResult17.scannedCount(), phoenixResult17.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult17.items()),
                sortItemsByPk(phoenixResult17.items())));

        // Test 18: contains() for non-existent attribute
        ScanRequest.Builder sr18 = ScanRequest.builder().tableName(tableName);
        sr18.filterExpression("NOT contains(nonExistentAttr, :value)");
        Map<String, AttributeValue> exprAttrVal18 = new HashMap<>();
        exprAttrVal18.put(":value", AttributeValue.builder().s("test").build());
        sr18.expressionAttributeValues(exprAttrVal18);

        ScanResponse phoenixResult18 = phoenixDBClientV2.scan(sr18.build());
        ScanResponse dynamoResult18 = dynamoDbClient.scan(sr18.build());

        Assert.assertEquals(8, dynamoResult18.count()
                .intValue()); // non-existent attribute doesn't contain anything
        Assert.assertEquals(dynamoResult18.count(), phoenixResult18.count());
        Assert.assertEquals(dynamoResult18.scannedCount(), phoenixResult18.scannedCount());
        Assert.assertTrue(ItemComparator.areItemsEqual(sortItemsByPk(dynamoResult18.items()),
                sortItemsByPk(phoenixResult18.items())));
    }

    private void putItemsForScanWithContainsFilter(String tableName) {
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("pk", AttributeValue.builder().s("item1").build());
        item1.put("stringAttr", AttributeValue.builder().s("hello world test").build());
        item1.put("stringSet", AttributeValue.builder().ss("apple", "banana", "cherry").build());
        item1.put("numberSet", AttributeValue.builder().ns("1", "2", "3").build());
        item1.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {1, 2}),
                        SdkBytes.fromByteArray(new byte[] {3, 4}),
                        SdkBytes.fromByteArray(new byte[] {5, 6})).build());
        item1.put("listAttr", AttributeValue.builder().l(AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("green").build(),
                AttributeValue.builder().s("blue").build()).build());
        item1.put("category", AttributeValue.builder().s("electronics").build());
        item1.put("numberAttr", AttributeValue.builder().n("100").build());

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("pk", AttributeValue.builder().s("item2").build());
        item2.put("stringAttr", AttributeValue.builder().s("goodbye world").build());
        item2.put("stringSet", AttributeValue.builder().ss("orange", "grape", "kiwi").build());
        item2.put("numberSet", AttributeValue.builder().ns("4", "5", "6").build());
        item2.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {7, 8}),
                        SdkBytes.fromByteArray(new byte[] {9, 10})).build());
        item2.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("yellow").build(),
                        AttributeValue.builder().s("purple").build()).build());
        item2.put("category", AttributeValue.builder().s("books").build());
        item2.put("numberAttr", AttributeValue.builder().n("200").build());

        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("pk", AttributeValue.builder().s("item3").build());
        item3.put("stringAttr", AttributeValue.builder().s("testing contains function").build());
        item3.put("stringSet", AttributeValue.builder().ss("apple", "orange", "mango").build());
        item3.put("numberSet", AttributeValue.builder().ns("1", "7", "8").build());
        item3.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {1, 2}),
                        SdkBytes.fromByteArray(new byte[] {11, 12})).build());
        item3.put("listAttr", AttributeValue.builder().l(AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("orange").build(),
                AttributeValue.builder().n("42").build()).build());
        item3.put("category", AttributeValue.builder().s("electronics").build());
        item3.put("numberAttr", AttributeValue.builder().n("300").build());

        Map<String, AttributeValue> item4 = new HashMap<>();
        item4.put("pk", AttributeValue.builder().s("item4").build());
        item4.put("stringAttr", AttributeValue.builder().s("no match here").build());
        item4.put("stringSet", AttributeValue.builder().ss("watermelon", "pineapple").build());
        item4.put("numberSet", AttributeValue.builder().ns("9", "10", "11").build());
        item4.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {13, 14})).build());
        item4.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("black").build(),
                        AttributeValue.builder().s("white").build()).build());
        item4.put("category", AttributeValue.builder().s("books").build());
        item4.put("numberAttr", AttributeValue.builder().n("400").build());

        Map<String, AttributeValue> item5 = new HashMap<>();
        item5.put("pk", AttributeValue.builder().s("item5").build());
        item5.put("stringAttr", AttributeValue.builder().s("world of wonders").build());
        item5.put("stringSet", AttributeValue.builder().ss("apple", "banana").build());
        item5.put("numberSet", AttributeValue.builder().ns("1", "2").build());
        item5.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {1, 2})).build());
        item5.put("listAttr", AttributeValue.builder().l(AttributeValue.builder().s("red").build(),
                AttributeValue.builder().s("pink").build()).build());
        item5.put("category", AttributeValue.builder().s("toys").build());
        item5.put("numberAttr", AttributeValue.builder().n("500").build());

        Map<String, AttributeValue> item6 = new HashMap<>();
        item6.put("pk", AttributeValue.builder().s("item6").build());
        item6.put("stringAttr", AttributeValue.builder().s("hello universe").build());
        item6.put("stringSet", AttributeValue.builder().ss("orange", "mango").build());
        item6.put("numberSet", AttributeValue.builder().ns("10", "12").build());
        item6.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {15, 16})).build());
        item6.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("green").build(),
                        AttributeValue.builder().s("blue").build()).build());
        item6.put("category", AttributeValue.builder().s("electronics").build());
        item6.put("numberAttr", AttributeValue.builder().n("600").build());

        Map<String, AttributeValue> item7 = new HashMap<>();
        item7.put("pk", AttributeValue.builder().s("item7").build());
        item7.put("stringAttr", AttributeValue.builder().s("testing world").build());
        item7.put("stringSet", AttributeValue.builder().ss("grape", "apple").build());
        item7.put("numberSet", AttributeValue.builder().ns("1", "9").build());
        item7.put("binarySet", AttributeValue.builder()
                .bs(SdkBytes.fromByteArray(new byte[] {1, 2}),
                        SdkBytes.fromByteArray(new byte[] {7, 8})).build());
        item7.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("black").build(),
                        AttributeValue.builder().n("42").build()).build());
        item7.put("category", AttributeValue.builder().s("books").build());
        item7.put("numberAttr", AttributeValue.builder().n("700").build());

        Map<String, AttributeValue> item8 = new HashMap<>();
        item8.put("pk", AttributeValue.builder().s("item8").build());
        item8.put("stringAttr", AttributeValue.builder().s("sample test").build());
        item8.put("stringSet", AttributeValue.builder().ss("cherry", "kiwi").build());
        item8.put("numberSet", AttributeValue.builder().ns("5", "6").build());
        item8.put("binarySet",
                AttributeValue.builder().bs(SdkBytes.fromByteArray(new byte[] {9, 10})).build());
        item8.put("listAttr", AttributeValue.builder()
                .l(AttributeValue.builder().s("white").build(),
                        AttributeValue.builder().s("gray").build()).build());
        item8.put("category", AttributeValue.builder().s("toys").build());
        item8.put("numberAttr", AttributeValue.builder().n("800").build());

        // Insert test data
        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(item3).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(item4).build();
        PutItemRequest putItemRequest5 =
                PutItemRequest.builder().tableName(tableName).item(item5).build();
        PutItemRequest putItemRequest6 =
                PutItemRequest.builder().tableName(tableName).item(item6).build();
        PutItemRequest putItemRequest7 =
                PutItemRequest.builder().tableName(tableName).item(item7).build();
        PutItemRequest putItemRequest8 =
                PutItemRequest.builder().tableName(tableName).item(item8).build();

        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        phoenixDBClientV2.putItem(putItemRequest5);
        phoenixDBClientV2.putItem(putItemRequest6);
        phoenixDBClientV2.putItem(putItemRequest7);
        phoenixDBClientV2.putItem(putItemRequest8);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest5);
        dynamoDbClient.putItem(putItemRequest6);
        dynamoDbClient.putItem(putItemRequest7);
        dynamoDbClient.putItem(putItemRequest8);
    }

    private List<Map<String, AttributeValue>> sortItemsByPk(
            List<Map<String, AttributeValue>> items) {
        return items.stream()
                .sorted(Comparator.comparing(item -> item.get("pk").s()))
                .collect(Collectors.toList());
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
