import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.PhoenixDBClientV2;
import org.apache.phoenix.ddb.PhoenixDBStreamsClientV2;
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
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Map;

import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.LATEST;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;
import static org.apache.phoenix.ddb.utils.DDBShimCDCUtils.MAX_NUM_CHANGES_AT_TIMESTAMP;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class GetShardIteratorIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetShardIteratorIT.class);

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
    public void testGetShardIterator() throws Exception {
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

        // wait for stream to be enabled
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();

        // stream would be in ENABLED state and api should return shards
        Assert.assertEquals(StreamStatus.ENABLED, phoenixStreamDesc.streamStatus());
        Assert.assertEquals(1, phoenixStreamDesc.shards().size());

        //print dynamo shard iterator for info
        StreamDescription dynamoStreamDesc = dynamoDbStreamsClient.describeStream(
                DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build()).streamDescription();
        GetShardIteratorRequest request1 = GetShardIteratorRequest.builder()
                .streamArn(dynamoStreamDesc.streamArn())
                .shardId(dynamoStreamDesc.shards().get(0).shardId())
                .shardIteratorType(TRIM_HORIZON).build();
        LOGGER.info("Dynamo Shard Iterator with trim horizon: " + dynamoDbStreamsClient.getShardIterator(request1).shardIterator());
        request1 = request1.toBuilder().shardIteratorType(LATEST).build();
        LOGGER.info("Dynamo Shard Iterator with latest: " + dynamoDbStreamsClient.getShardIterator(request1).shardIterator());

        // test phoenix shard iterator
        String shardStartSeqNum = phoenixStreamDesc.shards().get(0).sequenceNumberRange().startingSequenceNumber();
        String shardId = phoenixStreamDesc.shards().get(0).shardId();
        GetShardIteratorRequest.Builder request = GetShardIteratorRequest.builder()
                .streamArn(phoenixStreamDesc.streamArn())
                .shardId(shardId);

        String testSeqNum = "173837205000000420"; // 1738372050000+00420
        String testSeqNumPlusOne = String.valueOf(Long.parseLong(testSeqNum) + 1); // 1738372050000+00421
        //AT_SEQUENCE_NUMBER
        request.sequenceNumber(testSeqNum);
        request.shardIteratorType(AT_SEQUENCE_NUMBER);
        GetShardIteratorResponse result = phoenixDBStreamsClientV2.getShardIterator(request.build());
        validateShardIterator(result.shardIterator(), tableName, "CDC_"+tableName, "OLD_IMAGE", shardId);
        Assert.assertTrue(result.shardIterator().contains(testSeqNum));

        //AFTER_SEQUENCE_NUMBER
        request.shardIteratorType(AFTER_SEQUENCE_NUMBER);
        result = phoenixDBStreamsClientV2.getShardIterator(request.build());
        validateShardIterator(result.shardIterator(), tableName, "CDC_"+tableName, "OLD_IMAGE", shardId);
        Assert.assertTrue(result.shardIterator().contains(testSeqNumPlusOne));

        //TRIM_HORIZON
        request.sequenceNumber(null);
        request.shardIteratorType(TRIM_HORIZON);
        result = phoenixDBStreamsClientV2.getShardIterator(request.build());
        validateShardIterator(result.shardIterator(), tableName, "CDC_"+tableName, "OLD_IMAGE", shardId);
        Assert.assertTrue(result.shardIterator().contains(shardStartSeqNum));

        //LATEST
        request.sequenceNumber(null);
        long currentTime = EnvironmentEdgeManager.currentTimeMillis() - 1000;
        request.shardIteratorType(LATEST);
        result = phoenixDBStreamsClientV2.getShardIterator(request.build());
        validateShardIterator(result.shardIterator(), tableName, "CDC_"+tableName, "OLD_IMAGE", shardId);
        String shardIter[] = result.shardIterator().split("-");
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
