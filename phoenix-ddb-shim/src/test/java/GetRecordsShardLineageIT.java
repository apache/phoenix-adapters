import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Test GetRecords while following shard lineage when a partition has split.
 */
public class GetRecordsShardLineageIT extends GetRecordsBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsShardLineageIT.class);

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
        props.put(BaseScannerRegionObserverConstants.PHOENIX_MAX_LOOKBACK_AGE_CONF_KEY, Integer.toString(24*60*60));
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
    public void testGetRecordsWithPartitionSplit() throws Exception {
        final String tableName = "MYTABLE";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_AND_OLD_IMAGES");
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
        dynamoDbClient.updateItem(uir.build());

        //split table between the 2 records
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtils.splitTable(connection, tableName, Bytes.toBytes("LMN"));
        }

        //update item1 --> change should go to left daughter
        key = getKey1();
        uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #2 = :v2");
        exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "title");
        uir.expressionAttributeNames(exprAttrNames);
        exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().s("newTitle").build());
        uir.expressionAttributeValues(exprAttrVal);
        phoenixDBClientV2.updateItem(uir.build());
        dynamoDbClient.updateItem(uir.build());

        //update item2 --> change should go to right daughter
        key = getKey2();
        uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #2 = #2 + :v2");
        exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "Id1");
        uir.expressionAttributeNames(exprAttrNames);
        exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().n("10").build());
        uir.expressionAttributeValues(exprAttrVal);
        phoenixDBClientV2.updateItem(uir.build());
        dynamoDbClient.updateItem(uir.build());

        /**
         * Phoenix
         */
        // get shard iterator
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        String parentShard = null;
        List<String> daughterShards = new ArrayList<>();
        for (Shard shard : phoenixStreamDesc.shards()) {
            if (shard.sequenceNumberRange().endingSequenceNumber() != null) {
                parentShard = shard.shardId();
            } else {
                daughterShards.add(shard.shardId());
            }
        }
        Assert.assertNotNull(parentShard);
        Assert.assertEquals(2, daughterShards.size());
        List<Record> phoenixRecords = new ArrayList<>();
        // get records from parent shard
        phoenixRecords.addAll(TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), parentShard, TRIM_HORIZON, null,3));
        // get records from each daughter shard
        for (String daughter : daughterShards) {
            phoenixRecords.addAll(TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                    phoenixStreamDesc.streamArn(), daughter, TRIM_HORIZON, null, 1));
        }

        /**
         * Dynamo
         */
        dsr = DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build();
        StreamDescription dynamoStreamDesc = dynamoDbStreamsClient.describeStream(dsr).streamDescription();
        String shardId = dynamoStreamDesc.shards().get(0).shardId();
        // get records
        List<Record> dynamoRecords = TestUtils.getRecordsFromShardWithLimit(dynamoDbStreamsClient,
                dynamoStreamDesc.streamArn(), shardId, TRIM_HORIZON, null, 1);


        //sort based on timestamp for comparison since records from daughter
        // regions can be in a different order from dynamodb
        phoenixRecords.sort(Comparator.comparing(r -> r.dynamodb().approximateCreationDateTime()));
        dynamoRecords.sort(Comparator.comparing(r -> r.dynamodb().approximateCreationDateTime()));
        TestUtils.validateRecords(phoenixRecords, dynamoRecords);
    }
}
