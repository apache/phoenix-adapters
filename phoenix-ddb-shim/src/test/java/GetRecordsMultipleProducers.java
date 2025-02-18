import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
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
import static com.amazonaws.services.dynamodbv2.model.ShardIteratorType.LATEST;

@RunWith(Parameterized.class)
public class GetRecordsMultipleProducers {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsMultipleProducers.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    private static final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static final AmazonDynamoDBStreams amazonDynamoDBStreams =
            LocalDynamoDbTestBase.localDynamoDb().createV1StreamsClient();

    private static PhoenixDBClient phoenixDBClient = null;
    private static PhoenixDBStreamsClient phoenixDBStreamsClient = null;

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

        GetRecordsRequest grr = new GetRecordsRequest().withShardIterator(latestShardIterator).withLimit(limit);
        Set<Record> records = new HashSet<>();
        GetRecordsResult result;
        do {
            result = phoenixDBStreamsClient.getRecords(grr);
            records.addAll(result.getRecords());
            grr.setShardIterator(result.getNextShardIterator());
            Thread.sleep(100);
        } while (result.getNextShardIterator() != null && !result.getRecords().isEmpty());
        executorService.shutdown();
        Assert.assertEquals(threadCount * numItemsPerThread, records.size());
        Set<String> seqNums = new HashSet<>();
        for (Record r : records) {
            seqNums.add(r.getDynamodb().getSequenceNumber());
        }
        Assert.assertEquals(threadCount * numItemsPerThread, seqNums.size());
    }

    private String setupStreamAndGetLatestShardIterator(String tableName) throws InterruptedException {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_IMAGE");
        phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        phoenixDBStreamsClient = new PhoenixDBStreamsClient(url);
        ListStreamsRequest lsr = new ListStreamsRequest().withTableName(tableName);
        ListStreamsResult phoenixStreams = phoenixDBStreamsClient.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.getStreams().get(0).getStreamArn();
        TestUtils.waitForStream(phoenixDBStreamsClient, phoenixStreamArn);
        DescribeStreamRequest dsr = new DescribeStreamRequest().withStreamArn(phoenixStreamArn);
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClient.describeStream(dsr).getStreamDescription();
        GetShardIteratorRequest gsir = new GetShardIteratorRequest();
        gsir.setStreamArn(phoenixStreamArn);
        gsir.setShardId(phoenixStreamDesc.getShards().get(0).getShardId());
        gsir.setShardIteratorType(LATEST);
        return phoenixDBStreamsClient.getShardIterator(gsir).getShardIterator();
    }

    private static void putRandomItems(String tableName, int numItemsPerThread, double batchWriteProb) {
        // batch write, max 25 at a time
        if (random.nextDouble() < batchWriteProb) {
            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            List<WriteRequest> writeReqs = new ArrayList<>();
            for (int i = 0; i< numItemsPerThread; i++) {
                writeReqs.add(new WriteRequest(new PutRequest(buildRandomItem())));
            }
            requestItems.put(tableName, writeReqs);
            BatchWriteItemRequest request = new BatchWriteItemRequest(requestItems);
            phoenixDBClient.batchWriteItem(request);
        }
        else { // individual writes
            for (int i = 0; i< numItemsPerThread; i++) {
                PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(buildRandomItem());
                phoenixDBClient.putItem(request);
            }
        }
    }

    private static Map<String, AttributeValue> buildRandomItem() {
        String randomPk = UUID.randomUUID().toString();
        int randomId = random.nextInt(1000);
        String randomValue = UUID.randomUUID().toString();

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS(randomPk));
        item.put("PK2", new AttributeValue().withN(String.valueOf(randomId)));
        item.put("VAL", new AttributeValue().withS(randomValue));
        return item;
    }
}
