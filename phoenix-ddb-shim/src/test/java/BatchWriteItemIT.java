import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
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

public class BatchWriteItemIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchWriteItemIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static PhoenixDBClient phoenixDBClient = null;

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
        phoenixDBClient = new PhoenixDBClient(url);
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
        writeReqs.add(new WriteRequest(new PutRequest(getItem3())));
        writeReqs.add(new WriteRequest(new DeleteRequest(getKey(getItem1(), new String[]{"PK1", "PK2"}))));
        writeReqs.add(new WriteRequest(new DeleteRequest(getKey(getItem2(), new String[]{"PK1", "PK2"}))));
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName,  writeReqs);
        BatchWriteItemRequest request = new BatchWriteItemRequest(requestItems);

        BatchWriteItemResult dynamoResult = amazonDynamoDB.batchWriteItem(request);
        BatchWriteItemResult phoenixResult = phoenixDBClient.batchWriteItem(request);
        Assert.assertEquals(dynamoResult.getUnprocessedItems(), phoenixResult.getUnprocessedItems());

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
        writeReqs1.add(new WriteRequest(new PutRequest(getItem2())));
        writeReqs1.add(new WriteRequest(new PutRequest(getItem3())));
        writeReqs1.add(new WriteRequest(new DeleteRequest(getKey(getItem4(), new String[]{"PK1", "PK2"}))));

        //table2
        putItem(tableName2, getItem2());
        putItem(tableName2, getItem3());
        putItem(tableName2, getItem4());
        List<WriteRequest> writeReqs2 = new ArrayList<>();
        writeReqs2.add(new WriteRequest(new PutRequest(getItem1())));
        writeReqs2.add(new WriteRequest(new DeleteRequest(getKey(getItem2(), new String[]{"COL1", "COL2"}))));
        writeReqs2.add(new WriteRequest(new DeleteRequest(getKey(getItem3(), new String[]{"COL1", "COL2"}))));

        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName1,  writeReqs1);
        requestItems.put(tableName2,  writeReqs2);
        BatchWriteItemRequest request = new BatchWriteItemRequest(requestItems);

        BatchWriteItemResult dynamoResult = amazonDynamoDB.batchWriteItem(request);
        BatchWriteItemResult phoenixResult = phoenixDBClient.batchWriteItem(request);
        Assert.assertEquals(dynamoResult.getUnprocessedItems(), phoenixResult.getUnprocessedItems());

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
            writeReqs1.add(new WriteRequest(new PutRequest(getNewItem1(i))));
        }
        writeReqs1.add(new WriteRequest(new DeleteRequest(getKey(getItem4(), new String[]{"PK1", "PK2"}))));
        writeReqs1.add(new WriteRequest(new PutRequest(getNewItem1(27))));

        //table2 - 24 puts, 1 delete
        List<WriteRequest> writeReqs2 = new ArrayList<>();
        for (int i=0; i<24; i++) {
            writeReqs2.add(new WriteRequest(new PutRequest(getNewItem2(i))));
        }
        writeReqs2.add(new WriteRequest(new DeleteRequest(getKey(getItem4(), new String[]{"COL1", "COL2"}))));

        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName1,  writeReqs1);
        requestItems.put(tableName2,  writeReqs2);
        BatchWriteItemRequest request = new BatchWriteItemRequest(requestItems);

        BatchWriteItemResult phoenixResult = phoenixDBClient.batchWriteItem(request);
        Assert.assertTrue(phoenixResult.getUnprocessedItems().containsKey(tableName1));
        Assert.assertFalse(phoenixResult.getUnprocessedItems().containsKey(tableName2));
        Assert.assertEquals(2, phoenixResult.getUnprocessedItems().get(tableName1).size());

        request.setRequestItems(phoenixResult.getUnprocessedItems());
        phoenixResult = phoenixDBClient.batchWriteItem(request);
        Assert.assertTrue(phoenixResult.getUnprocessedItems().isEmpty());
    }

    private void createTable1(String tableName) {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);
    }

    private void createTable2(String tableName) {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "COL1",
                        ScalarAttributeType.N, "COL2", ScalarAttributeType.S);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);
    }

    private void putItem(String tableName, Map<String, AttributeValue> item) {
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        phoenixDBClient.putItem(putItemRequest);
        amazonDynamoDB.putItem(putItemRequest);
    }

    private void validateTableScan(String tableName) {
        ScanRequest sr = new ScanRequest(tableName);
        ScanResult phoenixResult = phoenixDBClient.scan(sr);
        ScanResult dynamoResult = amazonDynamoDB.scan(sr);
        Assert.assertEquals(dynamoResult.getCount(), phoenixResult.getCount());
        Assert.assertTrue(dynamoResult.getItems().containsAll(phoenixResult.getItems()));
        Assert.assertTrue(phoenixResult.getItems().containsAll(dynamoResult.getItems()));
    }


    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("A"));
        item.put("PK2", new AttributeValue().withN("1"));
        item.put("COL1", new AttributeValue().withN("1"));
        item.put("COL2", new AttributeValue().withS("Title1"));
        return item;
    }

    private Map<String, AttributeValue> getItem2() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("B"));
        item.put("PK2", new AttributeValue().withN("2"));
        item.put("COL1", new AttributeValue().withN("3"));
        item.put("COL2", new AttributeValue().withS("Title2"));
        return item;
    }

    private Map<String, AttributeValue> getItem3() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("C"));
        item.put("PK2", new AttributeValue().withN("3"));
        item.put("COL1", new AttributeValue().withN("4"));
        item.put("COL2", new AttributeValue().withS("Title3"));
        return item;
    }

    private Map<String, AttributeValue> getItem4() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("D"));
        item.put("PK2", new AttributeValue().withN("4"));
        item.put("COL1", new AttributeValue().withN("5"));
        item.put("COL2", new AttributeValue().withS("Title4"));
        return item;
    }

    private Map<String, AttributeValue> getNewItem1(int i) {
        Map<String, AttributeValue> item = getItem1();
        Integer pk2 = Integer.parseInt(item.get("PK2").getN()) * i;
        item.put("PK2",new AttributeValue().withN(pk2.toString()));
        return item;
    }

    private Map<String, AttributeValue> getNewItem2(int i) {
        Map<String, AttributeValue> item = getItem2();
        Integer pk2 = Integer.parseInt(item.get("COL1").getN()) * i;
        item.put("PK2",new AttributeValue().withN(pk2.toString()));
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
