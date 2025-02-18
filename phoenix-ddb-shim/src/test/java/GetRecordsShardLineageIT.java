import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.Shard;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
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
import org.apache.phoenix.util.TestUtil;
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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.TRIM_HORIZON;
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

        DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_AND_OLD_IMAGES");
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
        amazonDynamoDB.updateItem(uir);

        //split table between the 2 records
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtils.splitTable(connection, tableName, Bytes.toBytes("LMN"));
        }

        //update item1 --> change should go to left daughter
        key = getKey1();
        uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("SET #2 = :v2");
        exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "title");
        uir.setExpressionAttributeNames(exprAttrNames);
        exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", new AttributeValue().withS("newTitle"));
        uir.setExpressionAttributeValues(exprAttrVal);
        phoenixDBClient.updateItem(uir);
        amazonDynamoDB.updateItem(uir);

        //update item2 --> change should go to right daughter
        key = getKey2();
        uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("SET #2 = #2 + :v2");
        exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "Id1");
        uir.setExpressionAttributeNames(exprAttrNames);
        exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", new AttributeValue().withN("10"));
        uir.setExpressionAttributeValues(exprAttrVal);
        phoenixDBClient.updateItem(uir);
        amazonDynamoDB.updateItem(uir);

        /**
         * Phoenix
         */
        // get shard iterator
        DescribeStreamRequest dsr = new DescribeStreamRequest().withStreamArn(phoenixStreamArn);
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClient.describeStream(dsr).getStreamDescription();
        String parentShard = null;
        List<String> daughterShards = new ArrayList<>();
        for (Shard shard : phoenixStreamDesc.getShards()) {
            if (shard.getSequenceNumberRange().getEndingSequenceNumber() != null) {
                parentShard = shard.getShardId();
            } else {
                daughterShards.add(shard.getShardId());
            }
        }
        Assert.assertNotNull(parentShard);
        Assert.assertEquals(2, daughterShards.size());
        List<Record> phoenixRecords = new ArrayList<>();
        // get records from parent shard
        phoenixRecords.addAll(TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClient,
                phoenixStreamDesc.getStreamArn(), parentShard, TRIM_HORIZON, null,3));
        // get records from each daughter shard
        for (String daughter : daughterShards) {
            phoenixRecords.addAll(TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClient,
                    phoenixStreamDesc.getStreamArn(), daughter, TRIM_HORIZON, null, 1));
        }

        /**
         * Dynamo
         */
        dsr = new DescribeStreamRequest().withStreamArn(dynamoStreamArn);
        StreamDescription dynamoStreamDesc = amazonDynamoDBStreams.describeStream(dsr).getStreamDescription();
        String shardId = dynamoStreamDesc.getShards().get(0).getShardId();
        // get records
        List<Record> dynamoRecords = TestUtils.getRecordsFromShardWithLimit(amazonDynamoDBStreams,
                dynamoStreamDesc.getStreamArn(), shardId, TRIM_HORIZON, null, 1);


        //sort based on timestamp for comparison since records from daughter
        // regions can be in a different order from dynamodb
        phoenixRecords.sort(Comparator.comparing(r -> r.getDynamodb().getApproximateCreationDateTime()));
        dynamoRecords.sort(Comparator.comparing(r -> r.getDynamodb().getApproximateCreationDateTime()));
        TestUtils.validateRecords(phoenixRecords, dynamoRecords);
    }
}
