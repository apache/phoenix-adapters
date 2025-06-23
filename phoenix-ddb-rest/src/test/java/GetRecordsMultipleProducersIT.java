import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.coprocessorclient.BaseScannerRegionObserverConstants;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
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
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

@RunWith(Parameterized.class)
public class GetRecordsMultipleProducersIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsMultipleProducersIT.class);
    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbStreamsClient dynamoDbStreamsClient;
    private static DynamoDbClient phoenixDBClientV2;
    private static DynamoDbStreamsClient phoenixDBStreamsClientV2;

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    private static final Random random = new Random(42);

    private final int threadCount;
    private final int numItemsPerThread;
    private final int limit;
    private final double batchWriteProb;

    @Parameterized.Parameters(name="testGetRecordsMultipleProducers_NUMTHREADS_{0}_ITEMS_{1}_LIMIT_{2}_BATCHING_PROB_{3}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { 4, 10, 9, 0 },
                { 10, 2000, 912, 0 },
                { 12, 4, 7, 60 },
                { 50, 25, 99, 20 }
        });
    }

    public GetRecordsMultipleProducersIT(int numThreads, int items, int limit, int batchProb) {
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
                Long.toString(1000));
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpConfigForMiniCluster(conf, new ReadOnlyProps(props.entrySet().iterator()));

        utility.startMiniCluster();
        DriverManager.registerDriver(new PhoenixTestDriver());

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        phoenixDBStreamsClientV2 = LocalDynamoDB.createV2StreamsClient("http://" + restServer.getServerAddress());
        dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();
        dynamoDbStreamsClient = LocalDynamoDbTestBase.localDynamoDb().createV2StreamsClient();;
    }

    @AfterClass
    public static void stopLocalDynamoDb() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().stop();
        if (restServer != null) {
            restServer.stop();
        }
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
        List<String> latestShardIterators = setupStreamAndGetLatestShardIterators(tableName);
        String ddbShardIterator = latestShardIterators.get(0);
        String phoenixShardIterator = latestShardIterators.get(1);
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = IntStream.range(0, threadCount)
                .mapToObj(j -> executorService.submit(() ->
                        GetRecordsMultipleProducersIT.putRandomItems(tableName, numItemsPerThread, batchWriteProb)))
                .collect(Collectors.toList());
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
        // ddb
        GetRecordsRequest grr = GetRecordsRequest.builder().shardIterator(ddbShardIterator).limit(limit).build();
        List<Record> ddbRecords = new ArrayList<>();
        GetRecordsResponse ddbResponse;
        do {
            ddbResponse = dynamoDbStreamsClient.getRecords(grr);
            ddbRecords.addAll(ddbResponse.records());
            grr = grr.toBuilder().shardIterator(ddbResponse.nextShardIterator()).build();
        } while (ddbResponse.nextShardIterator() != null && !ddbResponse.records().isEmpty());
        Assert.assertEquals(threadCount * numItemsPerThread, ddbRecords.size());

        // phoenix
        grr = GetRecordsRequest.builder().shardIterator(phoenixShardIterator).limit(limit).build();
        List<Record> phoenixRecords = new ArrayList<>();
        GetRecordsResponse phoenixResponse;
        do {
            phoenixResponse = phoenixDBStreamsClientV2.getRecords(grr);
            phoenixRecords.addAll(phoenixResponse.records());
            grr = grr.toBuilder().shardIterator(phoenixResponse.nextShardIterator()).build();
        } while (phoenixResponse.nextShardIterator() != null && !phoenixResponse.records().isEmpty());
        executorService.shutdown();
        Assert.assertEquals(threadCount * numItemsPerThread, phoenixRecords.size());
        List<String> seqNums = new ArrayList<>();
        for (Record r : phoenixRecords) {
            seqNums.add(r.dynamodb().sequenceNumber());
        }
        Assert.assertEquals(threadCount * numItemsPerThread, seqNums.size());
    }

    private List<String> setupStreamAndGetLatestShardIterators(String tableName) throws InterruptedException {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_IMAGE");
        phoenixDBClientV2.createTable(createTableRequest);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        GetShardIteratorRequest gsir = GetShardIteratorRequest.builder()
                .streamArn(phoenixStreamArn)
                .shardId(phoenixStreamDesc.shards().get(0).shardId())
                .shardIteratorType("LATEST")
                .build();
        String phoenixShardIter = phoenixDBStreamsClientV2.getShardIterator(gsir).shardIterator();

        dynamoDbClient.createTable(createTableRequest);
        String ddbStreamArn = dynamoDbStreamsClient.listStreams(lsr).streams().get(0).streamArn();
        TestUtils.waitForStream(dynamoDbStreamsClient, ddbStreamArn);
        dsr = DescribeStreamRequest.builder().streamArn(ddbStreamArn).build();
        StreamDescription ddbStreamDesc = dynamoDbStreamsClient.describeStream(dsr).streamDescription();
        gsir = GetShardIteratorRequest.builder()
                .streamArn(ddbStreamArn)
                .shardId(ddbStreamDesc.shards().get(0).shardId())
                .shardIteratorType("LATEST")
                .build();
        String ddbShardIter = dynamoDbStreamsClient.getShardIterator(gsir).shardIterator();

        return Arrays.asList(ddbShardIter, phoenixShardIter);
    }

    private static void putRandomItems(String tableName, int numItemsPerThread, double batchWriteProb) {
        // batch write, max 25 at a time
        if (batchWriteProb > 0.0 && random.nextDouble() < batchWriteProb) {
            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            List<WriteRequest> writeReqs = new ArrayList<>();
            for (int i = 0; i< numItemsPerThread; i++) {
                writeReqs.add(WriteRequest.builder().putRequest(PutRequest.builder().item((buildRandomItem())).build()).build());
            }
            requestItems.put(tableName, writeReqs);
            BatchWriteItemRequest request = BatchWriteItemRequest.builder().requestItems(requestItems).build();
            phoenixDBClientV2.batchWriteItem(request);
            dynamoDbClient.batchWriteItem(request);
        }
        else { // individual writes
            for (int i = 0; i< numItemsPerThread; i++) {
                PutItemRequest request = PutItemRequest.builder().tableName(tableName).item(buildRandomItem()).build();
                phoenixDBClientV2.putItem(request);
                dynamoDbClient.putItem(request);
            }
        }
    }

    private static Map<String, AttributeValue> buildRandomItem() {
        String randomPk = UUID.randomUUID().toString();
        int randomId = random.nextInt();
        String randomValue = UUID.randomUUID().toString();

        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s(randomPk).build());
        item.put("PK2", AttributeValue.builder().n(String.valueOf(randomId)).build());
        item.put("VAL", AttributeValue.builder().s(randomValue).build());
        return item;
    }

}
