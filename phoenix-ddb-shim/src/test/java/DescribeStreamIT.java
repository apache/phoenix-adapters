import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.ddb.PhoenixDBStreamsClient;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.ConnectionQueryServices;
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

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private final AmazonDynamoDBStreams amazonDynamoDBStreams =
            LocalDynamoDbTestBase.localDynamoDb().createV1StreamsClient();

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

        DDLTestUtils.addStreamSpecToRequest(createTableRequest, "OLD_IMAGE");

        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        PhoenixDBStreamsClient phoenixDBStreamsClient = new PhoenixDBStreamsClient(url);
        amazonDynamoDB.createTable(createTableRequest);
        phoenixDBClient.createTable(createTableRequest);

        ListStreamsRequest lsr = new ListStreamsRequest().withTableName(tableName);
        ListStreamsResult phoenixStreams = phoenixDBStreamsClient.listStreams(lsr);
        ListStreamsResult dynamoStreams = amazonDynamoDBStreams.listStreams(lsr);
        Assert.assertEquals(dynamoStreams.getStreams().size(), phoenixStreams.getStreams().size());
        Assert.assertEquals(1, phoenixStreams.getStreams().size());

        String phoenixStreamArn = phoenixStreams.getStreams().get(0).getStreamArn();
        String dynamoStreamArn = dynamoStreams.getStreams().get(0).getStreamArn();
        DescribeStreamRequest phoenixRequest = new DescribeStreamRequest().withStreamArn(phoenixStreamArn);

        StreamDescription phoenixStreamDesc = phoenixDBStreamsClient.describeStream(phoenixRequest).getStreamDescription();

        // stream would be in ENABLING state
        Assert.assertEquals(CDCUtil.CdcStreamStatus.ENABLING.getSerializedValue(), phoenixStreamDesc.getStreamStatus());
        Assert.assertNull(phoenixStreamDesc.getShards());

        // wait for stream to be enabled
        TestUtils.waitForStream(phoenixDBStreamsClient, phoenixStreamArn);
        phoenixStreamDesc = phoenixDBStreamsClient.describeStream(phoenixRequest).getStreamDescription();

        // stream would be in ENABLED state and api should return shards
        Assert.assertEquals(CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue(), phoenixStreamDesc.getStreamStatus());
        StreamDescription dynamoStreamDesc = amazonDynamoDBStreams.describeStream(
                new DescribeStreamRequest().withStreamArn(dynamoStreamArn)).getStreamDescription();
        LOGGER.info("DescribeStream in Phoenix: " + phoenixStreamDesc);
        LOGGER.info("DescribeStream in DDB: " + dynamoStreamDesc);
        Assert.assertNotNull(phoenixStreamDesc.getShards());
        Assert.assertEquals(1, phoenixStreamDesc.getShards().size());
        Assert.assertEquals(dynamoStreamDesc.getStreamViewType(), phoenixStreamDesc.getStreamViewType());
        Assert.assertEquals(dynamoStreamDesc.getKeySchema(), phoenixStreamDesc.getKeySchema());
        Assert.assertEquals(dynamoStreamDesc.getTableName(), phoenixStreamDesc.getTableName());
        Assert.assertEquals(dynamoStreamDesc.getStreamStatus(), phoenixStreamDesc.getStreamStatus());

        // split table
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtils.splitTable(connection, tableName, Bytes.toBytes("foo"));
        }

        //local dynamodb does not support multiple shards so we will only verify phoenix here
        phoenixStreamDesc = phoenixDBStreamsClient.describeStream(phoenixRequest).getStreamDescription();
        LOGGER.info("DescribeStream in Phoenix after Split: " + phoenixStreamDesc);
        Assert.assertEquals(dynamoStreamDesc.getStreamViewType(), phoenixStreamDesc.getStreamViewType());
        Assert.assertEquals(dynamoStreamDesc.getKeySchema(), phoenixStreamDesc.getKeySchema());
        Assert.assertEquals(dynamoStreamDesc.getTableName(), phoenixStreamDesc.getTableName());
        Assert.assertEquals(dynamoStreamDesc.getStreamStatus(), phoenixStreamDesc.getStreamStatus());

        Assert.assertNotNull(phoenixStreamDesc.getShards());
        Assert.assertEquals(3, phoenixStreamDesc.getShards().size());
        String parentId = null;
        for (Shard shard : phoenixStreamDesc.getShards()) {
            Assert.assertNotNull(shard.getSequenceNumberRange());
            Assert.assertTrue(shard.getSequenceNumberRange().getStartingSequenceNumber().endsWith("00000"));
            // parent which split, should have end sequence number
            if (shard.getParentShardId() == null) {
                parentId = shard.getShardId();
                Assert.assertTrue(shard.getSequenceNumberRange().getEndingSequenceNumber().endsWith("99999"));
            }
        }
        for (Shard shard : phoenixStreamDesc.getShards()) {
            if (shard.getParentShardId() != null) {
                Assert.assertEquals(parentId, shard.getParentShardId());
            }
        }
    }
}
