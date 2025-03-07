import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.PhoenixDBClientV2;
import org.apache.phoenix.ddb.PhoenixDBStreamsClientV2;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class DescribeStreamIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeStreamIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

    private final DynamoDbStreamsClient dynamoDbStreamsClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2StreamsClient();

    private static String url;

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                Long.toString(0));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(2000));
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpConfigForMiniCluster(conf, new ReadOnlyProps(props.entrySet().iterator()));

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        DriverManager.registerDriver(new PhoenixTestDriver());
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
    public void testDescribeStreamWithSplit() throws Exception {
        String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey",
                        ScalarAttributeType.S, "sortKey", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "OLD_IMAGE");

        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        PhoenixDBStreamsClientV2 phoenixDBStreamsClientV2 = new PhoenixDBStreamsClientV2(url);
        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);

        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        ListStreamsResponse dynamoStreams = dynamoDbStreamsClient.listStreams(lsr);
        Assert.assertEquals(dynamoStreams.streams().size(), phoenixStreams.streams().size());
        Assert.assertEquals(1, phoenixStreams.streams().size());

        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn = dynamoStreams.streams().get(0).streamArn();
        DescribeStreamRequest phoenixRequest = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();

        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(phoenixRequest).streamDescription();

        // stream would be in ENABLING state
        Assert.assertEquals(StreamStatus.ENABLING, phoenixStreamDesc.streamStatus());
        Assert.assertTrue(phoenixStreamDesc.shards().isEmpty());

        // wait for stream to be enabled
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(phoenixRequest).streamDescription();

        // stream would be in ENABLED state and api should return shards
        Assert.assertEquals(StreamStatus.ENABLED, phoenixStreamDesc.streamStatus());
        StreamDescription dynamoStreamDesc = dynamoDbStreamsClient.describeStream(
                DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build()).streamDescription();
        LOGGER.info("DescribeStream in Phoenix: " + phoenixStreamDesc);
        LOGGER.info("DescribeStream in DDB: " + dynamoStreamDesc);
        Assert.assertNotNull(phoenixStreamDesc.shards());
        Assert.assertEquals(1, phoenixStreamDesc.shards().size());
        Assert.assertEquals(dynamoStreamDesc.streamViewType(), phoenixStreamDesc.streamViewType());
        Assert.assertEquals(dynamoStreamDesc.keySchema(), phoenixStreamDesc.keySchema());
        Assert.assertEquals(dynamoStreamDesc.tableName(), phoenixStreamDesc.tableName());
        Assert.assertEquals(dynamoStreamDesc.streamStatus(), phoenixStreamDesc.streamStatus());

        // split table
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtils.splitTable(connection, tableName, Bytes.toBytes("foo"));
        }

        //local dynamodb does not support multiple shards so we will only verify phoenix here
        phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(phoenixRequest).streamDescription();
        LOGGER.info("DescribeStream in Phoenix after Split: " + phoenixStreamDesc);
        Assert.assertEquals(dynamoStreamDesc.streamViewType(), phoenixStreamDesc.streamViewType());
        Assert.assertEquals(dynamoStreamDesc.keySchema(), phoenixStreamDesc.keySchema());
        Assert.assertEquals(dynamoStreamDesc.tableName(), phoenixStreamDesc.tableName());
        Assert.assertEquals(dynamoStreamDesc.streamStatus(), phoenixStreamDesc.streamStatus());

        Assert.assertNotNull(phoenixStreamDesc.shards());
        Assert.assertEquals(3, phoenixStreamDesc.shards().size());
        String parentId = null;
        for (Shard shard : phoenixStreamDesc.shards()) {
            Assert.assertNotNull(shard.sequenceNumberRange());
            Assert.assertTrue(shard.sequenceNumberRange().startingSequenceNumber().endsWith("00000"));
            // parent which split, should have end sequence number
            if (shard.parentShardId() == null) {
                parentId = shard.shardId();
                Assert.assertTrue(shard.sequenceNumberRange().endingSequenceNumber().endsWith("99999"));
            }
        }
        for (Shard shard : phoenixStreamDesc.shards()) {
            if (shard.parentShardId() != null) {
                Assert.assertEquals(parentId, shard.parentShardId());
            }
        }
    }
}
