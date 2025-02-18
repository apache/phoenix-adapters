import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.ddb.PhoenixDBStreamsClient;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.EnvironmentEdgeManager;
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

import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON;
import static org.apache.phoenix.ddb.utils.DDBShimCDCUtils.MAX_NUM_CHANGES_AT_TIMESTAMP;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class GetShardIteratorIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetShardIteratorIT.class);

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
    public void testGetShardIterator() throws Exception {
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

        // wait for stream to be enabled
        TestUtils.waitForStream(phoenixDBStreamsClient, phoenixStreamArn);
        DescribeStreamRequest dsr = new DescribeStreamRequest().withStreamArn(phoenixStreamArn);
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClient.describeStream(dsr).getStreamDescription();

        // stream would be in ENABLED state and api should return shards
        Assert.assertEquals(CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue(), phoenixStreamDesc.getStreamStatus());
        Assert.assertEquals(1, phoenixStreamDesc.getShards().size());

        //print dynamo shard iterator for info
        StreamDescription dynamoStreamDesc = amazonDynamoDBStreams.describeStream(
                new DescribeStreamRequest().withStreamArn(dynamoStreamArn)).getStreamDescription();
        GetShardIteratorRequest request = new GetShardIteratorRequest();
        request.setStreamArn(dynamoStreamDesc.getStreamArn());
        request.setShardId(dynamoStreamDesc.getShards().get(0).getShardId());
        request.setShardIteratorType(TRIM_HORIZON);
        LOGGER.info("Dynamo Shard Iterator with trim horizon: " + amazonDynamoDBStreams.getShardIterator(request).getShardIterator());
        request.setShardIteratorType(LATEST);
        LOGGER.info("Dynamo Shard Iterator with latest: " + amazonDynamoDBStreams.getShardIterator(request).getShardIterator());

        // test phoenix shard iterator
        String shardStartSeqNum = phoenixStreamDesc.getShards().get(0).getSequenceNumberRange().getStartingSequenceNumber();
        String shardId = phoenixStreamDesc.getShards().get(0).getShardId();
        request = new GetShardIteratorRequest();
        request.setStreamArn(phoenixStreamDesc.getStreamArn());
        request.setShardId(shardId);

        String testSeqNum = "173837205000000420"; // 1738372050000+00420
        String testSeqNumPlusOne = String.valueOf(Long.parseLong(testSeqNum) + 1); // 1738372050000+00421
        //AT_SEQUENCE_NUMBER
        request.setSequenceNumber(testSeqNum);
        request.setShardIteratorType(AT_SEQUENCE_NUMBER);
        GetShardIteratorResult result = phoenixDBStreamsClient.getShardIterator(request);
        validateShardIterator(result.getShardIterator(), tableName, "CDC_"+tableName, "OLD_IMAGE", shardId);
        Assert.assertTrue(result.getShardIterator().contains(testSeqNum));

        //AFTER_SEQUENCE_NUMBER
        request.setShardIteratorType(AFTER_SEQUENCE_NUMBER);
        result = phoenixDBStreamsClient.getShardIterator(request);
        validateShardIterator(result.getShardIterator(), tableName, "CDC_"+tableName, "OLD_IMAGE", shardId);
        Assert.assertTrue(result.getShardIterator().contains(testSeqNumPlusOne));

        //TRIM_HORIZON
        request.setSequenceNumber(null);
        request.setShardIteratorType(TRIM_HORIZON);
        result = phoenixDBStreamsClient.getShardIterator(request);
        validateShardIterator(result.getShardIterator(), tableName, "CDC_"+tableName, "OLD_IMAGE", shardId);
        Assert.assertTrue(result.getShardIterator().contains(shardStartSeqNum));

        //LATEST
        request.setSequenceNumber(null);
        long currentTime = EnvironmentEdgeManager.currentTimeMillis() - 1000;
        request.setShardIteratorType(LATEST);
        result = phoenixDBStreamsClient.getShardIterator(request);
        validateShardIterator(result.getShardIterator(), tableName, "CDC_"+tableName, "OLD_IMAGE", shardId);
        String shardIter[] = result.getShardIterator().split("-");
        // shard iterator would be created after the current time we recorded here
        Assert.assertTrue(Long.parseLong(shardIter[shardIter.length-1]) > currentTime * MAX_NUM_CHANGES_AT_TIMESTAMP);
    }

    private void validateShardIterator(String shardIter, String tableName, String cdcObj,
                                       String streamType, String shardId) {
        LOGGER.info("Shard Iterator: " + shardIter);
        Assert.assertTrue(shardIter.contains(tableName));
        Assert.assertTrue(shardIter.contains(cdcObj));
        Assert.assertTrue(shardIter.contains(streamType));
        Assert.assertTrue(shardIter.contains(shardId));
    }
}
