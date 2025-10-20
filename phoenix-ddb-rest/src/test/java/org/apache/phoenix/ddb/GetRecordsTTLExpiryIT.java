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

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.OperationType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.SchemaUtil;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TestUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

public class GetRecordsTTLExpiryIT extends GetRecordsBaseTest  {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsTTLExpiryIT.class);

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
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
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

    /**
     * Put 2 items that expire in 4h. Update item2.
     * Split the table in the middle.
     * Update item1, item2.
     * Validate change records from both phoenix and ddb.
     * Store last seqNum for both daughters.
     *
     * Increment clock so that all data table ttl, maxlookback and system table ttl pass.
     * Perform major compaction to purge expired rows.
     *
     * Verify:
     * 1. DescribeStream does not return parent shard.
     * 2. GetRecords on previously stored parent shardId returns no records.
     * 3. Both daughter shards see TTL_DELETE event after last seqNums.
     */
    @Test
    public void testTtlExpiry1() throws Exception {
        // create table
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_");
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_AND_OLD_IMAGES");
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn = dynamoDbStreamsClient.listStreams(lsr).streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        TestUtils.waitForStream(dynamoDbStreamsClient, dynamoStreamArn);

        // update table with TTL
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification
                spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        //put 2 items, expiring 4h in the future
        long expiry = (System.currentTimeMillis() + TimeUnit.HOURS.toMillis(4)) / 1000;
        Map<String, AttributeValue> item1 = getItem1();
        item1.put("ttlAttr", AttributeValue.builder().n(Long.toString(expiry)).build());
        Map<String, AttributeValue> item2 = getItem2();
        item2.put("ttlAttr", AttributeValue.builder().n(Long.toString(expiry)).build());
        PutItemRequest
                putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putItemRequest2 = PutItemRequest.builder().tableName(tableName).item(item2).build();
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
            TestUtils.splitTable(connection, PhoenixUtils.getFullTableName(tableName, false), Bytes.toBytes("LMN"));
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

        // validate change records
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
        List<Record> parentRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), parentShard, TRIM_HORIZON, null,3);
        phoenixRecords.addAll(parentRecords);
        String parentShardFirstSeqNum = parentRecords.get(0).dynamodb().sequenceNumber();
        // get records from each daughter shard
        Map<String, String> daughterSeqNumMap = new HashMap<>();
        for (String daughter : daughterShards) {
            List<Record> daughterRecords = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                    phoenixStreamDesc.streamArn(), daughter, TRIM_HORIZON, null, 1);
            phoenixRecords.addAll(daughterRecords);
            daughterSeqNumMap.put(daughter, daughterRecords.get(daughterRecords.size()-1).dynamodb().sequenceNumber());
        }

        dsr = DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build();
        StreamDescription dynamoStreamDesc = dynamoDbStreamsClient.describeStream(dsr).streamDescription();
        String shardId = dynamoStreamDesc.shards().get(0).shardId();
        List<Record> dynamoRecords = TestUtils.getRecordsFromShardWithLimit(dynamoDbStreamsClient,
                dynamoStreamDesc.streamArn(), shardId, TRIM_HORIZON, null, 1);


        phoenixRecords.sort(Comparator.comparing(r -> r.dynamodb().approximateCreationDateTime()));
        dynamoRecords.sort(Comparator.comparing(r -> r.dynamodb().approximateCreationDateTime()));
        TestUtils.validateRecords(phoenixRecords, dynamoRecords);

        // increment clock by 28h
        ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
        long t = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(28);
        t = (t / 1000) * 1000;
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(t);

        // major compact table
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtil.doMajorCompaction(connection, SchemaUtil.getEscapedArgument(PhoenixUtils.getFullTableName(tableName, false)));
        }

        // there should be no change records in parent shard
        List<Record> parentRecordsAfterCompaction
                = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2,
                phoenixStreamDesc.streamArn(), parentShard, TRIM_HORIZON, null,null);
        Assert.assertTrue(parentRecordsAfterCompaction.isEmpty());

        //daughter shard should see TTL DELETE event after the last seen seqNum
        for (String daughter : daughterSeqNumMap.keySet()) {
            List<Record> daughterRecordsAfterCompaction = TestUtils.getRecordsFromShardWithLimit(
                    phoenixDBStreamsClientV2, phoenixStreamDesc.streamArn(), daughter, TRIM_HORIZON,
                    null, null);
            Assert.assertEquals(1, daughterRecordsAfterCompaction.size());
            Assert.assertNotNull(daughterRecordsAfterCompaction.get(0).userIdentity());
            Assert.assertEquals("phoenix/hbase", daughterRecordsAfterCompaction.get(0).userIdentity().principalId());
            Assert.assertEquals("Service", daughterRecordsAfterCompaction.get(0).userIdentity().type());
        }

        // increment clock by 3 more hours
        injectEdge.incrementValue(TimeUnit.HOURS.toMillis(3));
        // describeStream should only return the child shards in Phoenix
        // parentShardId has the shardId of the parent partition which got closed and expired
        dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        Assert.assertEquals(2, phoenixStreamDesc.shards().size());
        for (Shard shard : phoenixStreamDesc.shards()) {
            Assert.assertNotEquals(shard.shardId(), parentShard);
        }
        EnvironmentEdgeManager.reset();
    }

    /**
     * Put an item with TTL of 3 days.
     * max lookback is 27h
     * Compaction after 4 days should not create ttl_delete event.
     * Compaction after 4 days + 3hours + few seconds should create ttl_delete event.
     */
    @Test
    public void testTtlExpiry2() throws Exception {
        // create table
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_");
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_AND_OLD_IMAGES");
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn = dynamoDbStreamsClient.listStreams(lsr).streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        TestUtils.waitForStream(dynamoDbStreamsClient, dynamoStreamArn);

        // update table with TTL
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification
                spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        // TTL = 3days
        long expiry = (System.currentTimeMillis() + TimeUnit.DAYS.toMillis(3)) / 1000;
        Map<String, AttributeValue> item1 = getItem1();
        item1.put("ttlAttr", AttributeValue.builder().n(Long.toString(expiry)).build());
        PutItemRequest
                putItemRequest1 = PutItemRequest.builder().tableName(tableName).item(item1).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest1);

        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(dsr).streamDescription();
        String parentShard = phoenixStreamDesc.shards().get(0).shardId();
        List<Record> records = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2, phoenixStreamArn, parentShard, TRIM_HORIZON, null,null);
        Assert.assertEquals(1, records.size());
        String lastSeqNum = records.get(0).dynamodb().sequenceNumber();

        // increment clock by ttl=3days and update the row after it expires
        ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
        long t = System.currentTimeMillis() + TimeUnit.DAYS.toMillis(3);
        t = ((t / 1000) + 60) * 1000;
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(t);
        UpdateItemRequest uir = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(getKey1())
                .updateExpression("REMOVE title")
                .returnValues(ReturnValue.ALL_NEW)
                .build();
        UpdateItemResponse phoenixResponse = phoenixDBClientV2.updateItem(uir);
        UpdateItemResponse ddbResponse = dynamoDbClient.updateItem(uir);
        Assert.assertEquals(ddbResponse.attributes(), phoenixResponse.attributes());

        // increment clock by 1 more day and major compact table and index
        injectEdge.incrementValue(TimeUnit.DAYS.toMillis(1));
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtil.doMajorCompaction(connection, SchemaUtil.getEscapedArgument(PhoenixUtils.getFullTableName(tableName, false)));
            TestUtil.doMajorCompaction(connection, SchemaUtil.getEscapedArgument(PhoenixUtils.getFullTableName("PHOENIX_CDC_INDEX_CDC_" +tableName, false)));
        }

        // there should be no ttl_delete event since max lookback is 27h
        records = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2, phoenixStreamArn, parentShard, TRIM_HORIZON, null,null);
        Assert.assertEquals(1, records.size());
        Assert.assertEquals(OperationType.MODIFY, records.get(0).eventName());

        // increment clock by 3h + few seconds and major compact
        injectEdge.incrementValue(TimeUnit.HOURS.toMillis(3) + TimeUnit.SECONDS.toMillis(10));
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtil.doMajorCompaction(connection, SchemaUtil.getEscapedArgument(PhoenixUtils.getFullTableName(tableName, false)));
        }

        // ttl_delete event is generated
        records = TestUtils.getRecordsFromShardWithLimit(phoenixDBStreamsClientV2, phoenixStreamArn, parentShard, TRIM_HORIZON, null,null);
        Assert.assertEquals(1, records.size());
        Assert.assertNotNull(records.get(0).userIdentity());
        Assert.assertEquals("phoenix/hbase", records.get(0).userIdentity().principalId());
        Assert.assertEquals("Service", records.get(0).userIdentity().type());
        EnvironmentEdgeManager.reset();
    }
}
