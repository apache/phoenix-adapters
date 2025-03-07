import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
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
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Test different stream types.
 * Each test does 2 puts, 1 update and 1 delete.
 * Also test nextShardIterator usage using different limits in GetRecords API
 */
@RunWith(Parameterized.class)
public class GetRecordsStreamTypeIT extends GetRecordsBaseTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsStreamTypeIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

    private final DynamoDbStreamsClient dynamoDbStreamsClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2StreamsClient();

    private static String url;

    private String streamType;
    private Integer limit;

    @Parameterized.Parameters(name="StreamType_{0}_{1}")
    public static synchronized Collection<String[]> data() {
        return Arrays.asList(new String[][] {
                { "OLD_IMAGE", "0" },
                { "NEW_IMAGE", "1" },
                { "NEW_AND_OLD_IMAGES", "2" },
                { "KEYS_ONLY", "3" },
                { "NEW_AND_OLD_IMAGES", "4" },
        });
    }

    public GetRecordsStreamTypeIT(String streamType, String limit) {
        Integer l = Integer.parseInt(limit);
        if (l == 0) l = null;
        this.limit = l;
        this.streamType = streamType;
    }

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
    public void testGetRecords() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, this.streamType);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
        PhoenixDBStreamsClientV2 phoenixDBStreamsClientV2 = new PhoenixDBStreamsClientV2(url);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        ListStreamsResponse dynamoStreams = dynamoDbStreamsClient.listStreams(lsr);
        String dynamoStreamArn = dynamoStreams.streams().get(0).streamArn();
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
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

        //delete item1
        key = getKey1();
        DeleteItemRequest dir = DeleteItemRequest.builder().tableName(tableName).key(key).build();
        phoenixDBClientV2.deleteItem(dir);
        dynamoDbClient.deleteItem(dir);

        /**
         * Phoenix
         */
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        String shardId = phoenixStreamDesc.shards().get(0).shardId();
        // get records
        List<Record> phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), shardId, TRIM_HORIZON, null, this.limit);

        /**
         * Dynamo
         */
        dsr = DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build();
        StreamDescription dynamoStreamDesc = dynamoDbStreamsClient.describeStream(dsr).streamDescription();
        shardId = dynamoStreamDesc.shards().get(0).shardId();
        // get records
        List<Record> dynamoRecords = TestUtils.getRecordsFromShardWithLimit(dynamoDbStreamsClient,
                dynamoStreamDesc.streamArn(), shardId, TRIM_HORIZON, null, this.limit);

        TestUtils.validateRecords(phoenixRecords, dynamoRecords);
    }
}
