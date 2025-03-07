import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Test to verify sequence numbers are increasing and unique in the shard even when multiple
 * writes have the same phoenix timestamp.
 */
public class GetRecordsSequenceNumberIT extends GetRecordsBaseTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsSequenceNumberIT.class);

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

    @Test
    public void testSequenceNumbers() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "KEYS_ONLY");
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
        PhoenixDBStreamsClientV2 phoenixDBStreamsClientV2 = new PhoenixDBStreamsClientV2(url);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn = dynamoDbStreamsClient.listStreams(lsr).streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);

        //put 2 items
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);

        //put 3 items, delete 1 item using batch api
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeReqs = new ArrayList<>();
        writeReqs.add(WriteRequest.builder().putRequest(PutRequest.builder().item((getItem3())).build()).build());
        writeReqs.add(WriteRequest.builder().putRequest(PutRequest.builder().item((getItem4())).build()).build());
        writeReqs.add(WriteRequest.builder().putRequest(PutRequest.builder().item((getItem5())).build()).build());
        writeReqs.add(WriteRequest.builder().deleteRequest(DeleteRequest.builder().key((getKey1())).build()).build());
        requestItems.put(tableName, writeReqs);
        BatchWriteItemRequest request = BatchWriteItemRequest.builder().requestItems(requestItems).build();
        phoenixDBClientV2.batchWriteItem(request);
        dynamoDbClient.batchWriteItem(request);

        //put 1 item
        PutItemRequest putItemRequest3 = PutItemRequest.builder().tableName(tableName).item(getItem6()).build();
        phoenixDBClientV2.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest3);

        //get shard id
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        String shardId = phoenixStreamDesc.shards().get(0).shardId();

        // get all records and confirm sequence numbers are increasing and unique
        List<Record> phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), shardId, TRIM_HORIZON, null, null);
        Assert.assertEquals(7, phoenixRecords.size());
        Set<String> seqNums = new HashSet<>();
        seqNums.add(phoenixRecords.get(0).dynamodb().sequenceNumber());
        for (int i=1; i < phoenixRecords.size(); i++) {
            String prevSeqNum = phoenixRecords.get(i-1).dynamodb().sequenceNumber();
            String currSeqNum = phoenixRecords.get(i).dynamodb().sequenceNumber();
            if (prevSeqNum.compareTo(currSeqNum) >= 0) {
                Assert.fail("Sequence numbers should be monotonically increasing: " + prevSeqNum + " " + currSeqNum);
            }
            seqNums.add(phoenixRecords.get(i).dynamodb().sequenceNumber());
        }
        Assert.assertEquals(7, seqNums.size());

        // get all records with different limits to test query offset logic
        for (int i=1; i<=7; i++) {
            phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                    phoenixStreamDesc.streamArn(), shardId, TRIM_HORIZON, null, i);
            Assert.assertEquals(7, phoenixRecords.size());
        }

        /**
         * Compare with Dynamo records
         */
        dsr = DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build();
        StreamDescription dynamoStreamDesc = dynamoDbStreamsClient.describeStream(dsr).streamDescription();
        shardId = dynamoStreamDesc.shards().get(0).shardId();
        List<Record> dynamoRecords = TestUtils.getRecordsFromShardWithLimit(dynamoDbStreamsClient,
                dynamoStreamDesc.streamArn(), shardId, TRIM_HORIZON, null, 10);
        // records with same ts are returned in PK order in phoenix
        // ordering can be different in ddb, just compare number of change records
        Assert.assertEquals(dynamoRecords.size(), phoenixRecords.size());
    }
}
