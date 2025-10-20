/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.ddb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
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
import org.apache.phoenix.ddb.utils.PhoenixUtils;

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
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

@RunWith(Parameterized.class)
public class GetRecordsIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsIT.class);

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

    @Rule
    public final TestName testName = new TestName();
    public boolean hasSortKey = false;
    public String streamType;

    @Parameterized.Parameters(name = "testGetRecords_sortKey_{0}_streamType_{1}")
    public static synchronized Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {{true, "OLD_IMAGE"}, {true, "NEW_AND_OLD_IMAGES"},
                {true, "KEYS_ONLY"}, {false, "NEW_IMAGE"}, {false, "NEW_AND_OLD_IMAGES"}});
    }

    public GetRecordsIT(boolean hasSortKey, String streamType) {
        this.hasSortKey = hasSortKey;
        this.streamType = streamType;
    }

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
        props.put(QueryServices.TASK_HANDLING_INTERVAL_MS_ATTRIB, Long.toString(0));
        props.put(QueryServices.TASK_HANDLING_INITIAL_DELAY_MS_ATTRIB, Long.toString(1000));
        props.put("hbase.coprocessor.master.classes", PhoenixMasterObserver.class.getName());
        setUpConfigForMiniCluster(conf, new ReadOnlyProps(props.entrySet().iterator()));

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        phoenixDBStreamsClientV2 =
                LocalDynamoDB.createV2StreamsClient("http://" + restServer.getServerAddress());
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

    /**
     * puts/upserts/deletes
     * getRecords using TRIM_HORIZON
     * puts/upserts/deletes
     * getRecords using AFTER_SEQUENCE_NUMBER from last record's seqNum
     * puts/upserts/deletes
     * getRecords using AT_SEQUENCE_NUMBER from last record's seqNum
     * getRecords using AFTER_SEQUENCE_NUMBER from last record's seqNum -> empty result
     */
    @Test(timeout = 120000)
    public void testGetRecords() throws Exception {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_");
        List<String> arns = setupAndGetStreamArns(tableName);
        String phoenixStreamArn = arns.get(0);
        String ddbStreamArn = arns.get(1);

        // puts, updates, deletes
        for (int i = 0; i < 25; i++) {
            PutItemRequest pir =
                    PutItemRequest.builder().tableName(tableName).item(getItem(i)).build();
            phoenixDBClientV2.putItem(pir);
            dynamoDbClient.putItem(pir);
        }
        for (int i = 0; i < 5; i++) {
            Map<String, AttributeValue> exprAttrVals = new HashMap<>();
            exprAttrVals.put(":v", AttributeValue.builder().n(String.valueOf((i + 1) * 3)).build());
            UpdateItemRequest uir = UpdateItemRequest.builder().tableName(tableName).key(getKey(i))
                    .updateExpression("SET VAL = :v").expressionAttributeValues(exprAttrVals)
                    .build();
            phoenixDBClientV2.updateItem(uir);
            dynamoDbClient.updateItem(uir);
        }
        for (int i = 5; i < 10; i++) {
            DeleteItemRequest del =
                    DeleteItemRequest.builder().tableName(tableName).key(getKey(i)).build();
            phoenixDBClientV2.deleteItem(del);
            dynamoDbClient.deleteItem(del);
        }

        // get shardIds
        DescribeStreamRequest dsr =
                DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        String phoenixShardId =
                phoenixDBStreamsClientV2.describeStream(dsr).streamDescription().shards().get(0)
                        .shardId();
        dsr = DescribeStreamRequest.builder().streamArn(ddbStreamArn).build();
        String ddbShardId =
                dynamoDbStreamsClient.describeStream(dsr).streamDescription().shards().get(0)
                        .shardId();

        // shardIterator TRIM_HORIZON
        GetShardIteratorRequest gsir = GetShardIteratorRequest.builder().streamArn(phoenixStreamArn)
                .shardId(phoenixShardId).shardIteratorType(ShardIteratorType.TRIM_HORIZON).build();
        String phoenixShardIterator =
                phoenixDBStreamsClientV2.getShardIterator(gsir).shardIterator();
        gsir = GetShardIteratorRequest.builder().streamArn(ddbStreamArn).shardId(ddbShardId)
                .shardIteratorType(ShardIteratorType.TRIM_HORIZON).build();
        String ddbShardIterator = dynamoDbStreamsClient.getShardIterator(gsir).shardIterator();

        // get records
        List<Record> phoenixRecords = new ArrayList<>(), ddbRecords = new ArrayList<>();
        GetRecordsResponse phoenixResponse, ddbResponse;
        GetRecordsRequest.Builder phoenixGRR =
                GetRecordsRequest.builder().shardIterator(phoenixShardIterator).limit(7);
        GetRecordsRequest.Builder ddbGRR =
                GetRecordsRequest.builder().shardIterator(ddbShardIterator).limit(7);
        do {
            phoenixResponse = phoenixDBStreamsClientV2.getRecords(phoenixGRR.build());
            ddbResponse = dynamoDbStreamsClient.getRecords(ddbGRR.build());
            Assert.assertEquals(ddbResponse.records().size(), phoenixResponse.records().size());
            for (int i = 0; i < phoenixResponse.records().size(); i++) {
                assertRecords(ddbResponse.records().get(i), phoenixResponse.records().get(i));
            }
            phoenixRecords.addAll(phoenixResponse.records());
            ddbRecords.addAll(ddbResponse.records());
            phoenixGRR.shardIterator(phoenixResponse.nextShardIterator());
            ddbGRR.shardIterator(ddbResponse.nextShardIterator());
        } while (phoenixResponse.nextShardIterator() != null && !phoenixResponse.records().isEmpty()
                && ddbResponse.nextShardIterator() != null && !ddbResponse.records().isEmpty());
        Assert.assertEquals(ddbRecords.size(), phoenixRecords.size());

        String phoenixLastSeqNum =
                phoenixRecords.get(phoenixRecords.size() - 1).dynamodb().sequenceNumber();
        String ddbLastSeqNum = ddbRecords.get(ddbRecords.size() - 1).dynamodb().sequenceNumber();

        // puts, updates, deletes
        for (int i = 25; i < 30; i++) {
            PutItemRequest pir =
                    PutItemRequest.builder().tableName(tableName).item(getItem(i)).build();
            phoenixDBClientV2.putItem(pir);
            dynamoDbClient.putItem(pir);
        }
        for (int i = 10; i < 15; i++) {
            Map<String, AttributeValue> exprAttrVals = new HashMap<>();
            exprAttrVals.put(":v", AttributeValue.builder().n(String.valueOf((i + 1) * 3)).build());
            UpdateItemRequest uir = UpdateItemRequest.builder().tableName(tableName).key(getKey(i))
                    .updateExpression("SET VAL = :v").expressionAttributeValues(exprAttrVals)
                    .build();
            phoenixDBClientV2.updateItem(uir);
            dynamoDbClient.updateItem(uir);
        }
        for (int i = 15; i < 20; i++) {
            DeleteItemRequest del =
                    DeleteItemRequest.builder().tableName(tableName).key(getKey(i)).build();
            phoenixDBClientV2.deleteItem(del);
            dynamoDbClient.deleteItem(del);
        }

        // shardIterator AFTER_SEQUENCE_NUMBER
        gsir = GetShardIteratorRequest.builder().streamArn(phoenixStreamArn).shardId(phoenixShardId)
                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .sequenceNumber(phoenixLastSeqNum).build();
        phoenixShardIterator = phoenixDBStreamsClientV2.getShardIterator(gsir).shardIterator();
        gsir = GetShardIteratorRequest.builder().streamArn(ddbStreamArn).shardId(ddbShardId)
                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .sequenceNumber(ddbLastSeqNum).build();
        ddbShardIterator = dynamoDbStreamsClient.getShardIterator(gsir).shardIterator();

        // get records
        phoenixGRR = GetRecordsRequest.builder().shardIterator(phoenixShardIterator).limit(3);
        ddbGRR = GetRecordsRequest.builder().shardIterator(ddbShardIterator).limit(3);
        do {
            phoenixResponse = phoenixDBStreamsClientV2.getRecords(phoenixGRR.build());
            ddbResponse = dynamoDbStreamsClient.getRecords(ddbGRR.build());
            Assert.assertEquals(ddbResponse.records().size(), phoenixResponse.records().size());
            for (int i = 0; i < phoenixResponse.records().size(); i++) {
                assertRecords(ddbResponse.records().get(i), phoenixResponse.records().get(i));
            }
            phoenixRecords.addAll(phoenixResponse.records());
            ddbRecords.addAll(ddbResponse.records());
            phoenixGRR.shardIterator(phoenixResponse.nextShardIterator());
            ddbGRR.shardIterator(ddbResponse.nextShardIterator());
        } while (phoenixResponse.nextShardIterator() != null && !phoenixResponse.records().isEmpty()
                && ddbResponse.nextShardIterator() != null && !ddbResponse.records().isEmpty());

        phoenixLastSeqNum =
                phoenixRecords.get(phoenixRecords.size() - 1).dynamodb().sequenceNumber();
        ddbLastSeqNum = ddbRecords.get(ddbRecords.size() - 1).dynamodb().sequenceNumber();

        // puts, update, delete
        for (int i = 30; i < 35; i++) {
            PutItemRequest pir =
                    PutItemRequest.builder().tableName(tableName).item(getItem(i)).build();
            phoenixDBClientV2.putItem(pir);
            dynamoDbClient.putItem(pir);
        }
        for (int i = 20; i < 25; i++) {
            Map<String, AttributeValue> exprAttrVals = new HashMap<>();
            exprAttrVals.put(":v", AttributeValue.builder().n(String.valueOf((i + 1) * 3)).build());
            UpdateItemRequest uir = UpdateItemRequest.builder().tableName(tableName).key(getKey(i))
                    .updateExpression("SET VAL = :v").expressionAttributeValues(exprAttrVals)
                    .build();
            phoenixDBClientV2.updateItem(uir);
            dynamoDbClient.updateItem(uir);
        }
        for (int i = 25; i < 30; i++) {
            DeleteItemRequest del =
                    DeleteItemRequest.builder().tableName(tableName).key(getKey(i)).build();
            phoenixDBClientV2.deleteItem(del);
            dynamoDbClient.deleteItem(del);
        }

        // shardIterator AT_SEQUENCE_NUMBER
        gsir = GetShardIteratorRequest.builder().streamArn(phoenixStreamArn).shardId(phoenixShardId)
                .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .sequenceNumber(phoenixLastSeqNum).build();
        phoenixShardIterator = phoenixDBStreamsClientV2.getShardIterator(gsir).shardIterator();
        gsir = GetShardIteratorRequest.builder().streamArn(ddbStreamArn).shardId(ddbShardId)
                .shardIteratorType(ShardIteratorType.AT_SEQUENCE_NUMBER)
                .sequenceNumber(ddbLastSeqNum).build();
        ddbShardIterator = dynamoDbStreamsClient.getShardIterator(gsir).shardIterator();

        // get records
        phoenixGRR = GetRecordsRequest.builder().shardIterator(phoenixShardIterator).limit(5);
        ddbGRR = GetRecordsRequest.builder().shardIterator(ddbShardIterator).limit(5);
        boolean checkLastSeqNum = true;
        do {
            phoenixResponse = phoenixDBStreamsClientV2.getRecords(phoenixGRR.build());
            ddbResponse = dynamoDbStreamsClient.getRecords(ddbGRR.build());
            Assert.assertEquals(ddbResponse.records().size(), phoenixResponse.records().size());
            for (int i = 0; i < phoenixResponse.records().size(); i++) {
                assertRecords(ddbResponse.records().get(i), phoenixResponse.records().get(i));
            }
            if (checkLastSeqNum) {
                Assert.assertEquals(phoenixLastSeqNum,
                        phoenixResponse.records().get(0).dynamodb().sequenceNumber());
                Assert.assertEquals(ddbLastSeqNum,
                        ddbResponse.records().get(0).dynamodb().sequenceNumber());
                checkLastSeqNum = false;
            }
            phoenixRecords.addAll(phoenixResponse.records());
            ddbRecords.addAll(ddbResponse.records());
            phoenixGRR.shardIterator(phoenixResponse.nextShardIterator());
            ddbGRR.shardIterator(ddbResponse.nextShardIterator());
        } while (phoenixResponse.nextShardIterator() != null && !phoenixResponse.records().isEmpty()
                && ddbResponse.nextShardIterator() != null && !ddbResponse.records().isEmpty());
        Assert.assertEquals(ddbRecords.size(), phoenixRecords.size());

        phoenixLastSeqNum =
                phoenixRecords.get(phoenixRecords.size() - 1).dynamodb().sequenceNumber();
        ddbLastSeqNum = ddbRecords.get(ddbRecords.size() - 1).dynamodb().sequenceNumber();

        // shardIterator AFTER_SEQUENCE_NUMBER
        gsir = GetShardIteratorRequest.builder().streamArn(phoenixStreamArn).shardId(phoenixShardId)
                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .sequenceNumber(phoenixLastSeqNum).build();
        phoenixShardIterator = phoenixDBStreamsClientV2.getShardIterator(gsir).shardIterator();
        gsir = GetShardIteratorRequest.builder().streamArn(ddbStreamArn).shardId(ddbShardId)
                .shardIteratorType(ShardIteratorType.AFTER_SEQUENCE_NUMBER)
                .sequenceNumber(ddbLastSeqNum).build();
        ddbShardIterator = dynamoDbStreamsClient.getShardIterator(gsir).shardIterator();

        // get records
        phoenixGRR = GetRecordsRequest.builder().shardIterator(phoenixShardIterator);
        ddbGRR = GetRecordsRequest.builder().shardIterator(ddbShardIterator);
        Assert.assertTrue(dynamoDbStreamsClient.getRecords(ddbGRR.build()).records().isEmpty());
        Assert.assertTrue(
                phoenixDBStreamsClientV2.getRecords(phoenixGRR.build()).records().isEmpty());
    }

    /**
     * puts 5k rows
     * splits table
     * updates 5k rows
     * splits table twice
     * deletes 2.5k rows
     *
     * @throws Exception
     */
    @Test(timeout = 600000)
    public void testGetRecordsWithPartitionSplits() throws Exception {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_");
        List<String> arns = setupAndGetStreamArns(tableName);
        String phoenixStreamArn = arns.get(0);
        String ddbStreamArn = arns.get(1);
        for (int i = 0; i < 5000; i++) {
            PutItemRequest pir =
                    PutItemRequest.builder().tableName(tableName).item(getItem(i)).build();
            phoenixDBClientV2.putItem(pir);
            dynamoDbClient.putItem(pir);
        }
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtils.splitTable(connection, PhoenixUtils.getFullTableName(tableName, false),
                    Bytes.toBytes(5));
        }
        for (int i = 0; i < 5000; i++) {
            Map<String, AttributeValue> exprAttrVals = new HashMap<>();
            exprAttrVals.put(":v", AttributeValue.builder().n(String.valueOf((i + 1) * 3)).build());
            UpdateItemRequest uir = UpdateItemRequest.builder().tableName(tableName).key(getKey(i))
                    .updateExpression("SET VAL = :v").expressionAttributeValues(exprAttrVals)
                    .build();
            phoenixDBClientV2.updateItem(uir);
            dynamoDbClient.updateItem(uir);
        }
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtils.splitTable(connection, PhoenixUtils.getFullTableName(tableName, false),
                    Bytes.toBytes(3));
            TestUtils.splitTable(connection, PhoenixUtils.getFullTableName(tableName, false),
                    Bytes.toBytes(7));
        }
        for (int i = 0; i < 5000; i = i + 2) {
            DeleteItemRequest del =
                    DeleteItemRequest.builder().tableName(tableName).key(getKey(i)).build();
            phoenixDBClientV2.deleteItem(del);
            dynamoDbClient.deleteItem(del);
        }

        // collect ddb records, sorted by timestamp
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(ddbStreamArn).build();
        String ddbShardId =
                dynamoDbStreamsClient.describeStream(dsr).streamDescription().shards().get(0)
                        .shardId();
        List<Record> ddbRecords =
                TestUtils.getRecordsFromShardWithLimit(dynamoDbStreamsClient, ddbStreamArn,
                        ddbShardId, ShardIteratorType.TRIM_HORIZON, null, 1000);
        ddbRecords.sort(Comparator.comparing(r -> r.dynamodb().approximateCreationDateTime()));

        //collect phoenix records
        dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        List<Shard> phoenixShards =
                phoenixDBStreamsClientV2.describeStream(dsr).streamDescription().shards();
        List<Record> phoenixRecords = new ArrayList<>();
        for (Shard shard : phoenixShards) {
            phoenixRecords.addAll(TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                    phoenixStreamArn, shard.shardId(), ShardIteratorType.TRIM_HORIZON, null, 999));
        }
        phoenixRecords.sort(Comparator.comparing(r -> r.dynamodb().approximateCreationDateTime()));

        TestUtils.validateRecords(phoenixRecords, ddbRecords);
    }

    private List<String> setupAndGetStreamArns(String tableName) throws InterruptedException {
        CreateTableRequest createTableRequest;
        if (hasSortKey) {
            createTableRequest =
                    DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.N,
                            "PK2", ScalarAttributeType.N);
        } else {
            createTableRequest =
                    DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.N,
                            null, null);
        }
        createTableRequest =
                DDLTestUtils.addStreamSpecToRequest(createTableRequest, this.streamType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        String ddbStreamArn = dynamoDbStreamsClient.listStreams(lsr).streams().get(0).streamArn();
        TestUtils.waitForStream(dynamoDbStreamsClient, ddbStreamArn);
        return Arrays.asList(phoenixStreamArn, ddbStreamArn);
    }

    private Map<String, AttributeValue> getItem(int i) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().n(String.valueOf(i)).build());
        if (hasSortKey)
            item.put("PK2", AttributeValue.builder().n(String.valueOf(i + 1)).build());
        item.put("VAL", AttributeValue.builder().n(String.valueOf(i * 2)).build());
        return item;
    }

    private Map<String, AttributeValue> getKey(int i) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK1", AttributeValue.builder().n(String.valueOf(i)).build());
        if (hasSortKey)
            key.put("PK2", AttributeValue.builder().n(String.valueOf(i + 1)).build());
        return key;
    }

    private void assertRecords(Record ddbRecord, Record phoenixRecord) {
        Assert.assertEquals(ddbRecord.eventName(), phoenixRecord.eventName());
        Assert.assertEquals(ddbRecord.dynamodb().oldImage(), phoenixRecord.dynamodb().oldImage());
        Assert.assertEquals(ddbRecord.dynamodb().newImage(), phoenixRecord.dynamodb().newImage());
        Assert.assertTrue(ddbRecord.dynamodb().sizeBytes() > 0);
        Assert.assertTrue(phoenixRecord.dynamodb().sizeBytes() > 0);
    }
}
