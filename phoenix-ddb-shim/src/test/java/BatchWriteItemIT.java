import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
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

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class BatchWriteItemIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchWriteItemIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

    private static PhoenixDBClientV2 phoenixDBClientV2 = null;

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
        phoenixDBClientV2 = new PhoenixDBClientV2(url);
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

    @Test
    public void testBatchWritesOneTable() {
        String tableName = testName.getMethodName().toUpperCase();
        createTable1(tableName);
        putItem(tableName, getItem1());
        putItem(tableName, getItem2());
        List<WriteRequest> writeReqs = new ArrayList<>();
        writeReqs.add(WriteRequest.builder().putRequest(PutRequest.builder().item(getItem3()).build()).build());
        writeReqs.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(getKey(getItem1(), new String[]{"PK1", "PK2"})).build()).build());
        writeReqs.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(getKey(getItem2(), new String[]{"PK1", "PK2"})).build()).build());
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName,  writeReqs);
        BatchWriteItemRequest request = BatchWriteItemRequest.builder().requestItems(requestItems).build();;

        BatchWriteItemResponse dynamoResult = dynamoDbClient.batchWriteItem(request);
        BatchWriteItemResponse phoenixResult = phoenixDBClientV2.batchWriteItem(request);
        Assert.assertEquals(dynamoResult.unprocessedItems(), phoenixResult.unprocessedItems());

        validateTableScan(tableName);
    }

    @Test
    public void testBatchWritesTwoTables() {
        String testname = testName.getMethodName().toUpperCase();
        String tableName1 = testname + "_1";
        String tableName2 = testname + "_2";
        createTable1(tableName1);
        createTable2(tableName2);

        //table1
        putItem(tableName1, getItem4());
        List<WriteRequest> writeReqs1 = new ArrayList<>();
        writeReqs1.add(WriteRequest.builder().putRequest(PutRequest.builder().item(getItem2()).build()).build());
        writeReqs1.add(WriteRequest.builder().putRequest(PutRequest.builder().item(getItem3()).build()).build());
        writeReqs1.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(getKey(getItem4(), new String[]{"PK1", "PK2"})).build()).build());

        //table2
        putItem(tableName2, getItem2());
        putItem(tableName2, getItem3());
        putItem(tableName2, getItem4());
        List<WriteRequest> writeReqs2 = new ArrayList<>();
        writeReqs2.add(WriteRequest.builder().putRequest(PutRequest.builder().item(getItem1()).build()).build());
        writeReqs2.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(getKey(getItem2(), new String[]{"COL1", "COL2"})).build()).build());
        writeReqs2.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(getKey(getItem3(), new String[]{"COL1", "COL2"})).build()).build());

        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName1,  writeReqs1);
        requestItems.put(tableName2,  writeReqs2);
        BatchWriteItemRequest request = BatchWriteItemRequest.builder().requestItems(requestItems).build();;

        BatchWriteItemResponse dynamoResult = dynamoDbClient.batchWriteItem(request);
        BatchWriteItemResponse phoenixResult = phoenixDBClientV2.batchWriteItem(request);
        Assert.assertEquals(dynamoResult.unprocessedItems(), phoenixResult.unprocessedItems());

        validateTableScan(tableName1);
        validateTableScan(tableName2);
    }

    @Test
    public void testUnprocessedKeys() {
        String testname = testName.getMethodName().toUpperCase();
        String tableName1 = testname + "_1";
        String tableName2 = testname + "_2";
        createTable1(tableName1);
        createTable2(tableName2);

        //table1 - 25puts, 1 delete, 1 put
        List<WriteRequest> writeReqs1 = new ArrayList<>();
        for (int i=0; i<25; i++) {
            writeReqs1.add(WriteRequest.builder().putRequest(PutRequest.builder().item(getNewItem1(i)).build()).build());
        }
        writeReqs1.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(getKey(getItem4(), new String[]{"PK1", "PK2"})).build()).build());
        writeReqs1.add(WriteRequest.builder().putRequest(PutRequest.builder().item(getNewItem1(27)).build()).build());

        //table2 - 24 puts, 1 delete
        List<WriteRequest> writeReqs2 = new ArrayList<>();
        for (int i=0; i<24; i++) {
            writeReqs2.add(WriteRequest.builder().putRequest(PutRequest.builder().item(getNewItem2(i)).build()).build());
        }
        writeReqs2.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key(getKey(getItem4(), new String[]{"COL1", "COL2"})).build()).build());

        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName1,  writeReqs1);
        requestItems.put(tableName2,  writeReqs2);
        BatchWriteItemRequest request = BatchWriteItemRequest.builder().requestItems(requestItems).build();;

        BatchWriteItemResponse phoenixResult = phoenixDBClientV2.batchWriteItem(request);
        Assert.assertTrue(phoenixResult.unprocessedItems().containsKey(tableName1));
        Assert.assertFalse(phoenixResult.unprocessedItems().containsKey(tableName2));
        Assert.assertEquals(2, phoenixResult.unprocessedItems().get(tableName1).size());

        request = request.toBuilder().requestItems(phoenixResult.unprocessedItems()).build();
        phoenixResult = phoenixDBClientV2.batchWriteItem(request);
        Assert.assertTrue(phoenixResult.unprocessedItems().isEmpty());
    }

    private void createTable1(String tableName) {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
    }

    private void createTable2(String tableName) {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "COL1",
                        ScalarAttributeType.N, "COL2", ScalarAttributeType.S);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
    }

    private void putItem(String tableName, Map<String, AttributeValue> item) {
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);
    }

    private void validateTableScan(String tableName) {
        ScanRequest sr = ScanRequest.builder().tableName(tableName).build();
        ScanResponse phoenixResult = phoenixDBClientV2.scan(sr);
        ScanResponse dynamoResult = dynamoDbClient.scan(sr);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertTrue(dynamoResult.items().containsAll(phoenixResult.items()));
        Assert.assertTrue(phoenixResult.items().containsAll(dynamoResult.items()));
    }


    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("A").build());
        item.put("PK2", AttributeValue.builder().n("1").build());
        item.put("COL1", AttributeValue.builder().n("1").build());
        item.put("COL2", AttributeValue.builder().s("Title1").build());
        return item;
    }

    private Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("B").build());
        item.put("PK2", AttributeValue.builder().n("2").build());
        item.put("COL1", AttributeValue.builder().n("3").build());
        item.put("COL2", AttributeValue.builder().s("Title2").build());
        return item;
    }

    private Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("C").build());
        item.put("PK2", AttributeValue.builder().n("3").build());
        item.put("COL1", AttributeValue.builder().n("4").build());
        item.put("COL2", AttributeValue.builder().s("Title3").build());
        return item;
    }

    private Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("D").build());
        item.put("PK2", AttributeValue.builder().n("4").build());
        item.put("COL1", AttributeValue.builder().n("5").build());
        item.put("COL2", AttributeValue.builder().s("Title4").build());
        return item;
    }

    private Map<String, AttributeValue> getNewItem1(int i) {
        Map<String, AttributeValue> item = getItem1();
        Integer pk2 = Integer.parseInt(item.get("PK2").n()) * i;
        item.put("PK2",AttributeValue.builder().n(pk2.toString()).build());
        return item;
    }

    private Map<String, AttributeValue> getNewItem2(int i) {
        Map<String, AttributeValue> item = getItem2();
        Integer pk2 = Integer.parseInt(item.get("COL1").n()) * i;
        item.put("PK2",AttributeValue.builder().n(pk2.toString()).build());
        return item;
    }

    private Map<String, AttributeValue> getKey(Map<String, AttributeValue> item, String[] keyCols) {
        Map<String, AttributeValue> key = new HashMap<>();
        for (String keyCol : keyCols) {
            key.put(keyCol, item.get(keyCol));
        }
        return key;
    }
}
