import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.LATEST;

@RunWith(Parameterized.class)
public class GetRecordsMultipleProducers {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsMultipleProducers.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    private static final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

    private static final DynamoDbStreamsClient dynamoDbStreamsClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2StreamsClient();

    private static PhoenixDBClientV2 phoenixDBClientV2 = null;
    private static PhoenixDBStreamsClientV2 phoenixDBStreamsClientV2 = null;

    private static String url;

    private static final Random random = new Random();

    private int threadCount;
    private int numItemsPerThread;
    private int limit;
    private double batchWriteProb;

    @Parameterized.Parameters(name="testGetRecordsMultipleProducers_NUMTHREADS_{0}_ITEMS_{1}_LIMIT_{2}_BATCHING_PROB_{3}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { 10, 10, 9, 0 },
                { 20, 1000, 9123, 0 },
                { 12, 4, 7, 60 },
                { 50, 25, 99, 20 }
        });
    }

    public GetRecordsMultipleProducers(int numThreads, int items, int limit, int batchProb) {
        this.threadCount = numThreads;
        this.numItemsPerThread = items;
        this.limit = limit;
        this.batchWriteProb = batchProb*1.0/100;
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

    @Test
    public void testGetRecordsMultipleProducers() throws Exception {
        String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "").replaceAll(",", "");
        String latestShardIterator = setupStreamAndGetLatestShardIterator(tableName);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        IntStream.range(0, threadCount).forEach(i ->
                executorService.submit(() ->
                        GetRecordsMultipleProducers.putRandomItems(tableName, numItemsPerThread, batchWriteProb))
        );

        GetRecordsRequest grr = GetRecordsRequest.builder().shardIterator(latestShardIterator).limit(limit).build();
        Set<Record> records = new HashSet<>();
        GetRecordsResponse result;
        do {
            Thread.sleep(100); //give some time before consuming
            result = phoenixDBStreamsClientV2.getRecords(grr);
            records.addAll(result.records());
            grr = grr.toBuilder().shardIterator(result.nextShardIterator()).build();
        } while (result.nextShardIterator() != null && !result.records().isEmpty());
        executorService.shutdown();
        Assert.assertEquals(threadCount * numItemsPerThread, records.size());
        Set<String> seqNums = new HashSet<>();
        for (Record r : records) {
            seqNums.add(r.dynamodb().sequenceNumber());
        }
        Assert.assertEquals(threadCount * numItemsPerThread, seqNums.size());
    }

    private String setupStreamAndGetLatestShardIterator(String tableName) throws InterruptedException {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_IMAGE");
        phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);
        phoenixDBStreamsClientV2 = new PhoenixDBStreamsClientV2(url);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        GetShardIteratorRequest gsir = GetShardIteratorRequest.builder()
                .streamArn(phoenixStreamArn)
                .shardId(phoenixStreamDesc.shards().get(0).shardId())
                .shardIteratorType(LATEST)
                .build();
        return phoenixDBStreamsClientV2.getShardIterator(gsir).shardIterator();
    }

    private static void putRandomItems(String tableName, int numItemsPerThread, double batchWriteProb) {
        // batch write, max 25 at a time
        if (random.nextDouble() < batchWriteProb) {
            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            List<WriteRequest> writeReqs = new ArrayList<>();
            for (int i = 0; i< numItemsPerThread; i++) {
                writeReqs.add(WriteRequest.builder().putRequest(PutRequest.builder().item((buildRandomItem())).build()).build());
            }
            requestItems.put(tableName, writeReqs);
            BatchWriteItemRequest request = BatchWriteItemRequest.builder().requestItems(requestItems).build();
            phoenixDBClientV2.batchWriteItem(request);
        }
        else { // individual writes
            for (int i = 0; i< numItemsPerThread; i++) {
                PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(buildRandomItem()).build();
                phoenixDBClientV2.putItem(request);
            }
        }
    }

    private static Map<String, AttributeValue> buildRandomItem() {
        String randomPk = UUID.randomUUID().toString();
        int randomId = random.nextInt(1000);
        String randomValue = UUID.randomUUID().toString();

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s(randomPk).build());
        item.put("PK2", AttributeValue.builder().n(String.valueOf(randomId)).build());
        item.put("VAL", AttributeValue.builder().s(randomValue).build());
        return item;
    }
}
