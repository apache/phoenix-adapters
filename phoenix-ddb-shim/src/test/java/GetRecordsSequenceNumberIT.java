import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.ddb.PhoenixDBStreamsClient;
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

import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON;
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

    @Test
    public void testSequenceNumbers() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);

        DDLTestUtils.addStreamSpecToRequest(createTableRequest, "KEYS_ONLY");
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);
        PhoenixDBStreamsClient phoenixDBStreamsClient = new PhoenixDBStreamsClient(url);
        ListStreamsRequest lsr = new ListStreamsRequest().withTableName(tableName);
        ListStreamsResult phoenixStreams = phoenixDBStreamsClient.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.getStreams().get(0).getStreamArn();
        String dynamoStreamArn = amazonDynamoDBStreams.listStreams(lsr).getStreams().get(0).getStreamArn();
        TestUtils.waitForStream(phoenixDBStreamsClient, phoenixStreamArn);

        //put 2 items
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());
        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);
        amazonDynamoDB.putItem(putItemRequest1);
        amazonDynamoDB.putItem(putItemRequest2);

        //put 3 items, delete 1 item using batch api
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        List<WriteRequest> writeReqs = new ArrayList<>();
        writeReqs.add(new WriteRequest(new PutRequest(getItem3())));
        writeReqs.add(new WriteRequest(new PutRequest(getItem4())));
        writeReqs.add(new WriteRequest(new PutRequest(getItem5())));
        writeReqs.add(new WriteRequest(new DeleteRequest(getKey1())));
        requestItems.put(tableName, writeReqs);
        BatchWriteItemRequest request = new BatchWriteItemRequest(requestItems);
        phoenixDBClient.batchWriteItem(request);
        amazonDynamoDB.batchWriteItem(request);

        //put 1 item
        PutItemRequest putItemRequest3 = new PutItemRequest(tableName, getItem6());
        phoenixDBClient.putItem(putItemRequest3);
        amazonDynamoDB.putItem(putItemRequest3);

        //get shard id
        DescribeStreamRequest dsr = new DescribeStreamRequest().withStreamArn(phoenixStreamArn);
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClient.describeStream(dsr).getStreamDescription();
        String shardId = phoenixStreamDesc.getShards().get(0).getShardId();

        // get all records and confirm sequence numbers are increasing and unique
        List<Record> phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClient,
                phoenixStreamDesc.getStreamArn(), shardId, TRIM_HORIZON, null, null);
        Assert.assertEquals(7, phoenixRecords.size());
        Set<String> seqNums = new HashSet<>();
        seqNums.add(phoenixRecords.get(0).getDynamodb().getSequenceNumber());
        for (int i=1; i < phoenixRecords.size(); i++) {
            String prevSeqNum = phoenixRecords.get(i-1).getDynamodb().getSequenceNumber();
            String currSeqNum = phoenixRecords.get(i).getDynamodb().getSequenceNumber();
            if (prevSeqNum.compareTo(currSeqNum) >= 0) {
                Assert.fail("Sequence numbers should be monotonically increasing: " + prevSeqNum + " " + currSeqNum);
            }
            seqNums.add(phoenixRecords.get(i).getDynamodb().getSequenceNumber());
        }
        Assert.assertEquals(7, seqNums.size());

        // get all records with different limits to test query offset logic
        for (int i=1; i<=7; i++) {
            phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClient,
                    phoenixStreamDesc.getStreamArn(), shardId, TRIM_HORIZON, null, i);
            Assert.assertEquals(7, phoenixRecords.size());
        }

        /**
         * Compare with Dynamo records
         */
        dsr = new DescribeStreamRequest().withStreamArn(dynamoStreamArn);
        StreamDescription dynamoStreamDesc = amazonDynamoDBStreams.describeStream(dsr).getStreamDescription();
        shardId = dynamoStreamDesc.getShards().get(0).getShardId();
        List<Record> dynamoRecords = TestUtils.getRecordsFromShardWithLimit(amazonDynamoDBStreams,
                dynamoStreamDesc.getStreamArn(), shardId, TRIM_HORIZON, null, 10);
        // records with same ts are returned in PK order in phoenix
        // ordering can be different in ddb, just compare number of change records
        Assert.assertEquals(dynamoRecords.size(), phoenixRecords.size());
    }
}
