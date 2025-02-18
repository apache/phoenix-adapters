import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST;
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Test different types of shard iterators when retrieving change records.
 */
public class GetRecordsShardIteratorTypeIT extends GetRecordsBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsShardIteratorTypeIT.class);

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
    public void testShardIteratorTypes() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);

        DDLTestUtils.addStreamSpecToRequest(createTableRequest, "KEYS_ONLY");
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        PhoenixDBStreamsClient phoenixDBStreamsClient = new PhoenixDBStreamsClient(url);
        ListStreamsRequest lsr = new ListStreamsRequest().withTableName(tableName);
        ListStreamsResult phoenixStreams = phoenixDBStreamsClient.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.getStreams().get(0).getStreamArn();
        TestUtils.waitForStream(phoenixDBStreamsClient, phoenixStreamArn);

        //put 2 items
        PutItemRequest putItemRequest1 = new PutItemRequest(tableName, getItem1());
        PutItemRequest putItemRequest2 = new PutItemRequest(tableName, getItem2());
        phoenixDBClient.putItem(putItemRequest1);
        phoenixDBClient.putItem(putItemRequest2);

        //update item2
        Map<String, AttributeValue> key = getKey2();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("SET #2 = #2 + :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "Id2");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", new AttributeValue().withN("32"));
        uir.setExpressionAttributeValues(exprAttrVal);
        phoenixDBClient.updateItem(uir);

        //delete item1
        key = getKey1();
        DeleteItemRequest dir = new DeleteItemRequest(tableName, key);
        phoenixDBClient.deleteItem(dir);

        //get shard id
        DescribeStreamRequest dsr = new DescribeStreamRequest().withStreamArn(phoenixStreamArn);
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClient.describeStream(dsr).getStreamDescription();
        String shardId = phoenixStreamDesc.getShards().get(0).getShardId();

        // TRIM_HORIZON, all records returned
        List<Record> phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClient,
                phoenixStreamDesc.getStreamArn(), shardId, TRIM_HORIZON, null, null);
        Assert.assertEquals(4, phoenixRecords.size());

        // AT_SEQUENCE_NUMBER, use second sequence number, 3 records returned
        String testSeqNum = phoenixRecords.get(1).getDynamodb().getSequenceNumber();
        phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClient,
                phoenixStreamDesc.getStreamArn(), shardId, AT_SEQUENCE_NUMBER, testSeqNum, null);
        Assert.assertEquals(3, phoenixRecords.size());

        // AFTER_SEQUENCE_NUMBER, use second sequence number, 2 records returned
        phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClient,
                phoenixStreamDesc.getStreamArn(), shardId, AFTER_SEQUENCE_NUMBER, testSeqNum, null);
        Assert.assertEquals(2, phoenixRecords.size());

        // LATEST, no records returned
        phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClient,
                phoenixStreamDesc.getStreamArn(), shardId, LATEST, null, null);
        Assert.assertEquals(0, phoenixRecords.size());
        // do another change and then retrieve using latest shard iterator
        GetShardIteratorRequest gsir = new GetShardIteratorRequest();
        gsir.setStreamArn(phoenixStreamDesc.getStreamArn());
        gsir.setShardId(shardId);
        gsir.setShardIteratorType(LATEST);
        gsir.setSequenceNumber(null);
        String shardIter = phoenixDBStreamsClient.getShardIterator(gsir).getShardIterator();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, getItem3());
        phoenixDBClient.putItem(putItemRequest);
        GetRecordsRequest grr = new GetRecordsRequest().withShardIterator(shardIter);
        phoenixRecords = phoenixDBStreamsClient.getRecords(grr).getRecords();
        Assert.assertEquals(1, phoenixRecords.size());
    }
}
