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

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class TimeToLiveIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeToLiveIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

    private static String url;

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
    public void updateTimeToLiveTest() {
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        dynamoDbClient.createTable(createTableRequest);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
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
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        dynamoDbClient.createTable(createTableRequest);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
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
    public void ttlExpressionTest() {
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.S, "PK2", ScalarAttributeType.S);

        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);

        // enable TTL
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        // should expire
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("pk1").build());
        item.put("PK2", AttributeValue.builder().s("pk2").build());
        item.put("ttlAttr", AttributeValue.builder().n(Long.toString(System.currentTimeMillis())).build());
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(pir);

        //should not expire
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("PK1", AttributeValue.builder().s("pk11").build());
        item2.put("PK2", AttributeValue.builder().s("pk22").build());
        item2.put("ttlAttr", AttributeValue.builder().n(Long.toString(System.currentTimeMillis() + 1000000)).build());
        pir = PutItemRequest.builder().tableName(tableName).item(item2).build();
        phoenixDBClientV2.putItem(pir);

        //scan should show only the second item
        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName).build();
        ScanResponse scanResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(1, scanResponse.items().size());
        Assert.assertEquals("pk11", scanResponse.items().get(0).get("PK1").s());
        Assert.assertEquals("pk22", scanResponse.items().get(0).get("PK2").s());

        // disable TTL
        uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(false).build();
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        //scan should show both items since we did not run compaction
        scanRequest = ScanRequest.builder().tableName(tableName).build();
        scanResponse = phoenixDBClientV2.scan(scanRequest);
        Assert.assertEquals(2, scanResponse.items().size());
    }
}
