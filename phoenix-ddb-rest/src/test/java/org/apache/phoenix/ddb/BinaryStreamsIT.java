package org.apache.phoenix.ddb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.rest.RESTServer;
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
import org.junit.Test;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
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
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

public class BinaryStreamsIT {

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private final DynamoDbStreamsClient dynamoDbStreamsClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2StreamsClient();
    private static DynamoDbClient phoenixDBClientV2;
    private static DynamoDbStreamsClient phoenixDBStreamsClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    private static String TABLE_NAME = "Binary.PK_STREAMs_Test-Table";
    Random random = new Random(42);

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB,
                Long.toString(0));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB,
                Long.toString(1000));
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpConfigForMiniCluster(conf, new ReadOnlyProps(props.entrySet().iterator()));
        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());
        TestUtils.awaitPhoenixReady(url);

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        phoenixDBStreamsClientV2 = LocalDynamoDB.createV2StreamsClient("http://" + restServer.getServerAddress());
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
    public void getStreamRecords() throws InterruptedException {
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(TABLE_NAME, "hk", ScalarAttributeType.B,
                        "sk", ScalarAttributeType.B);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
        UpdateTableRequest utr =   UpdateTableRequest.builder()
                .tableName(TABLE_NAME)
                .streamSpecification(StreamSpecification.builder().streamEnabled(true).streamViewType("NEW_AND_OLD_IMAGES").build())
                .build();
        dynamoDbClient.updateTable(utr);
        phoenixDBClientV2.updateTable(utr);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(TABLE_NAME).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn = dynamoDbStreamsClient.listStreams(lsr).streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        TestUtils.waitForStream(dynamoDbStreamsClient, dynamoStreamArn);

        List<Map<String, AttributeValue>> keys = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Map<String, AttributeValue> item = BinaryEndToEndIT.getItem(i);
            keys.add(BinaryEndToEndIT.getKey(item));
            PutItemRequest pir = PutItemRequest.builder().tableName(TABLE_NAME).item(item).build();
            dynamoDbClient.putItem(pir);
            phoenixDBClientV2.putItem(pir);
        }
        for (int i = 0; i < 20; i+=2) {
            byte[] index_hk = new byte[11];
            random.nextBytes(index_hk);
            Map<String, AttributeValue> exprAttrVals = new HashMap<>();
            exprAttrVals.put(":val1", AttributeValue.builder().b(SdkBytes.fromByteArray(index_hk)).build());
            UpdateItemRequest uir = UpdateItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(keys.get(i))
                    .updateExpression("SET index_hk = :val1")
                    .expressionAttributeValues(exprAttrVals)
                    .build();
            dynamoDbClient.updateItem(uir);
            phoenixDBClientV2.updateItem(uir);
        }
        for (int i = 1; i < 20; i+=2) {
            DeleteItemRequest delr = DeleteItemRequest.builder()
                    .tableName(TABLE_NAME)
                    .key(keys.get(i))
                    .build();
            dynamoDbClient.deleteItem(delr);
            phoenixDBClientV2.deleteItem(delr);
        }

        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        String phoenixShardId = phoenixStreamDesc.shards().get(0).shardId();
        List<Record> phoenixRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamArn, phoenixShardId, TRIM_HORIZON, null, 11);

        dsr = DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build();
        String ddbShardId = dynamoDbStreamsClient.describeStream(dsr).streamDescription().shards().get(0).shardId();
        List<Record> ddbRecords = TestUtils.getRecordsFromShardWithLimit(dynamoDbStreamsClient,
                dynamoStreamArn, ddbShardId, TRIM_HORIZON, null, 7);

        Assert.assertEquals(ddbRecords.size(), phoenixRecords.size());
        for (int i = 0; i < phoenixRecords.size(); i++) {
            StreamRecord ddbRecord = ddbRecords.get(i).dynamodb();
            StreamRecord phoenixRecord = phoenixRecords.get(i).dynamodb();
            Assert.assertEquals(ddbRecord.oldImage(), phoenixRecord.oldImage());
            Assert.assertEquals(ddbRecord.newImage(), phoenixRecord.newImage());
        }
    }
}
