import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.AFTER_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.AT_SEQUENCE_NUMBER;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.LATEST;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;
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
    public void testShardIteratorTypes() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "KEYS_ONLY");
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        PhoenixDBStreamsClientV2 phoenixDBStreamsClientV2 = new PhoenixDBStreamsClientV2(url);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);

        //put 2 items
        PutItemRequest putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(getItem1()).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(getItem2()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);

        //update item2
        Map<String, AttributeValue> key = getKey2();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #2 = #2 + :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "Id2");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().n("32").build());
        uir.expressionAttributeValues(exprAttrVal);
        phoenixDBClientV2.updateItem(uir.build());

        //delete item1
        key = getKey1();
        DeleteItemRequest dir = DeleteItemRequest.builder().tableName(tableName).key(key).build();
        phoenixDBClientV2.deleteItem(dir);

        //get shard id
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        String shardId = phoenixStreamDesc.shards().get(0).shardId();

        // TRIM_HORIZON, all records returned
        List<Record> phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), shardId, TRIM_HORIZON, null, null);
        Assert.assertEquals(4, phoenixRecords.size());

        // AT_SEQUENCE_NUMBER, use second sequence number, 3 records returned
        String testSeqNum = phoenixRecords.get(1).dynamodb().sequenceNumber();
        phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), shardId, AT_SEQUENCE_NUMBER, testSeqNum, null);
        Assert.assertEquals(3, phoenixRecords.size());

        // AFTER_SEQUENCE_NUMBER, use second sequence number, 2 records returned
        phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), shardId, AFTER_SEQUENCE_NUMBER, testSeqNum, null);
        Assert.assertEquals(2, phoenixRecords.size());

        // LATEST, no records returned
        phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), shardId, LATEST, null, null);
        Assert.assertEquals(0, phoenixRecords.size());
        // do another change and then retrieve using latest shard iterator
        GetShardIteratorRequest gsir
                = GetShardIteratorRequest.builder()
                .streamArn(phoenixStreamDesc.streamArn())
                .shardId(shardId)
                .shardIteratorType(LATEST)
                .sequenceNumber(null).build();
        String shardIter = phoenixDBStreamsClientV2.getShardIterator(gsir).shardIterator();
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(getItem3()).build();
        phoenixDBClientV2.putItem(putItemRequest);
        GetRecordsRequest grr = GetRecordsRequest.builder().shardIterator(shardIter).build();
        phoenixRecords = phoenixDBStreamsClientV2.getRecords(grr).records();
        Assert.assertEquals(1, phoenixRecords.size());
    }
}
