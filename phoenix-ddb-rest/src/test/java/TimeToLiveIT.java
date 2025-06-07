import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveResponse;

import java.sql.DriverManager;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/*
TODO: Once we move to eventually consistent TTL,
    we will no longer be able to mask items that expire until they are removed by Major Compaction.
 */
public class TimeToLiveIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeToLiveIT.class);

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
    public void updateTimeToLiveTest() {
        final String tableName = "1298-.Node.Stream._OverflowStack";
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);

        // enable
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        UpdateTimeToLiveResponse ddbResponse = dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        UpdateTimeToLiveResponse phoenixResponse = phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        Assert.assertEquals(ddbResponse.timeToLiveSpecification(), phoenixResponse.timeToLiveSpecification());

        // disable
        spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(false).build();
        ddbResponse = dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        phoenixResponse = phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        Assert.assertEquals(ddbResponse.timeToLiveSpecification(), phoenixResponse.timeToLiveSpecification());
    }

    @Test(timeout = 120000)
    public void describeTimeToLiveTest() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);

        // enable
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        DescribeTimeToLiveRequest dTtlReq = DescribeTimeToLiveRequest.builder().tableName(tableName).build();
        DescribeTimeToLiveResponse ddbResponse = dynamoDbClient.describeTimeToLive(dTtlReq);
        DescribeTimeToLiveResponse phoenixResponse = phoenixDBClientV2.describeTimeToLive(dTtlReq);
        Assert.assertEquals(ddbResponse.timeToLiveDescription(), phoenixResponse. timeToLiveDescription());

        // disable
        spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(false).build();
        dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        ddbResponse = dynamoDbClient.describeTimeToLive(dTtlReq);
        phoenixResponse = phoenixDBClientV2.describeTimeToLive(dTtlReq);
        Assert.assertEquals(ddbResponse.timeToLiveDescription(), phoenixResponse. timeToLiveDescription());
    }

    @Test(timeout = 120000)
    public void ttlExpressionTest() throws InterruptedException {
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.S, "PK2", ScalarAttributeType.S);

        phoenixDBClientV2.createTable(createTableRequest);

        // enable TTL
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        // item with expiry = now
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("pk1").build());
        item.put("PK2", AttributeValue.builder().s("pk2").build());
        item.put("ttlAttr", AttributeValue.builder().n(Long.toString(Instant.now().getEpochSecond())).build());
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(pir);

        // item2 with expiry = now + 10s
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("PK1", AttributeValue.builder().s("pk11").build());
        item2.put("PK2", AttributeValue.builder().s("pk22").build());
        item2.put("ttlAttr", AttributeValue.builder().n(Long.toString(Instant.now().getEpochSecond() + 10)).build());
        pir = PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(pir);

        // scan should show only item2
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse scanResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(1, scanResponse.items().size());
        Assert.assertEquals("pk11", scanResponse.items().get(0).get("PK1").s());
        Assert.assertEquals("pk22", scanResponse.items().get(0).get("PK2").s());

        // item3 without the ttl attribute
        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("PK1", AttributeValue.builder().s("pk111").build());
        item3.put("PK2", AttributeValue.builder().s("pk222").build());
        item3.put("otherAttr", AttributeValue.builder().n(Long.toString(System.currentTimeMillis())).build());
        pir = PutItemRequest.builder().tableName(tableName).item(item3).build();
        phoenixDBClientV2.putItem(pir);

        //scan should show item2 and item3
        scanResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(2, scanResponse.items().size());
        Assert.assertFalse(scanResponse.items().contains(item));

        //sleep for >10s, item2 should also expire, only item3 remains
        Thread.sleep(10100);
        scanResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(1, scanResponse.items().size());
        Assert.assertTrue(scanResponse.items().contains(item3));

        // disable TTL
        uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(false).build();
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        //scan should show all items since we did not run compaction
        scanResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(3, scanResponse.items().size());
    }
}
