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

import org.apache.commons.lang3.StringUtils;
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
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class DescribeStreamIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeStreamIT.class);

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
        Map<String, String> props = Maps.newHashMapWithExpectedSize(3);
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

    @Test(timeout = 120000)
    public void testDescribeStreamWithLimitAndPagination() throws Exception {
        String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey",
                        ScalarAttributeType.S, "sortKey", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_IMAGE");
        phoenixDBClientV2.createTable(createTableRequest);

        // wait for stream to be enabled
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        String fullTableName = PhoenixUtils.getFullTableName(tableName, false);
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("m"));
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("g"));
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("d"));
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("j"));
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("t"));
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("p"));
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("w"));
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("l"));
            TestUtils.splitTable(connection, fullTableName, Bytes.toBytes("r"));
        }

        List<Shard> phoenixShards = new ArrayList<>();
        String lastEvaluatedShardId = null;
        do {
            StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(
                    DescribeStreamRequest.builder().streamArn(phoenixStreamArn)
                            .exclusiveStartShardId(lastEvaluatedShardId).limit(3).build()).streamDescription();
            phoenixShards.addAll(phoenixStreamDesc.shards());
            lastEvaluatedShardId = phoenixStreamDesc.lastEvaluatedShardId();
        } while (lastEvaluatedShardId != null);
        Assert.assertEquals(19, phoenixShards.size());

        int open = 0, closed = 0;
        for (Shard shard : phoenixShards) {
            if (StringUtils.isEmpty(shard.sequenceNumberRange().endingSequenceNumber())) {
                open++;
            } else {
                closed++;
            }
        }
        Assert.assertEquals(10, open);
        Assert.assertEquals(9, closed);
    }

    @Test(timeout = 120000)
    public void testDescribeStreamWithSplit() throws Exception {
        String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey",
                        ScalarAttributeType.S, "sortKey", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "OLD_IMAGE");

        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);

        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        ListStreamsResponse dynamoStreams = dynamoDbStreamsClient.listStreams(lsr);
        Assert.assertEquals(dynamoStreams.streams().size(), phoenixStreams.streams().size());
        Assert.assertEquals(1, phoenixStreams.streams().size());

        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn = dynamoStreams.streams().get(0).streamArn();
        DescribeStreamRequest phoenixRequest = DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();

        StreamDescription phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(phoenixRequest).streamDescription();
        LOGGER.info("DescribeStream in Phoenix before enabling: " + phoenixStreamDesc);

        // stream would be in ENABLING state
        Assert.assertEquals(StreamStatus.ENABLING, phoenixStreamDesc.streamStatus());
        Assert.assertTrue(phoenixStreamDesc.shards().isEmpty());

        // wait for stream to be enabled
        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);

        List<Shard> phoenixShards = new ArrayList<>();
        List<Shard> ddbShards = new ArrayList<>();

        String lastEvaluatedShardId = null;
        do {
            phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(
                DescribeStreamRequest.builder().streamArn(phoenixStreamArn)
                    .exclusiveStartShardId(lastEvaluatedShardId).build()).streamDescription();
            phoenixShards.addAll(phoenixStreamDesc.shards());
            lastEvaluatedShardId = phoenixStreamDesc.lastEvaluatedShardId();
        } while (lastEvaluatedShardId != null);

        StreamDescription dynamoStreamDesc;
        lastEvaluatedShardId = null;
        do {
            dynamoStreamDesc = dynamoDbStreamsClient.describeStream(
                DescribeStreamRequest.builder().streamArn(dynamoStreamArn)
                    .exclusiveStartShardId(lastEvaluatedShardId).build()).streamDescription();
            ddbShards.addAll(dynamoStreamDesc.shards());
            lastEvaluatedShardId = dynamoStreamDesc.lastEvaluatedShardId();
        } while (lastEvaluatedShardId != null);

        // stream would be in ENABLED state and api should return shards
        Assert.assertEquals(StreamStatus.ENABLED, phoenixStreamDesc.streamStatus());
        LOGGER.info("DescribeStream in Phoenix: " + phoenixStreamDesc);
        LOGGER.info("DescribeStream in DDB: " + dynamoStreamDesc);
        Assert.assertNotNull(phoenixShards);
        Assert.assertEquals(1, phoenixShards.size());
        Assert.assertEquals(dynamoStreamDesc.streamViewType(), phoenixStreamDesc.streamViewType());
        Assert.assertEquals(dynamoStreamDesc.keySchema(), phoenixStreamDesc.keySchema());
        Assert.assertEquals(dynamoStreamDesc.tableName(), phoenixStreamDesc.tableName());
        Assert.assertEquals(dynamoStreamDesc.streamStatus(), phoenixStreamDesc.streamStatus());

        // split table
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtils.splitTable(connection, PhoenixUtils.getFullTableName(tableName, false), Bytes.toBytes("foo"));
        }

        phoenixShards.clear();
        lastEvaluatedShardId = null;
        do {
            phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(
                DescribeStreamRequest.builder().streamArn(phoenixStreamArn)
                    .exclusiveStartShardId(lastEvaluatedShardId).limit(1).build()).streamDescription();
            phoenixShards.addAll(phoenixStreamDesc.shards());
            lastEvaluatedShardId = phoenixStreamDesc.lastEvaluatedShardId();
        } while (lastEvaluatedShardId != null);

        //local dynamodb does not support multiple shards so we will only verify phoenix here
        phoenixStreamDesc = phoenixDBStreamsClientV2.describeStream(phoenixRequest).streamDescription();
        LOGGER.info("DescribeStream in Phoenix after Split: " + phoenixStreamDesc);
        Assert.assertEquals(dynamoStreamDesc.streamViewType(), phoenixStreamDesc.streamViewType());
        Assert.assertEquals(dynamoStreamDesc.keySchema(), phoenixStreamDesc.keySchema());
        Assert.assertEquals(dynamoStreamDesc.tableName(), phoenixStreamDesc.tableName());
        Assert.assertEquals(dynamoStreamDesc.streamStatus(), phoenixStreamDesc.streamStatus());

        Assert.assertNotNull(phoenixShards);
        Assert.assertEquals(3, phoenixShards.size());
        String parentId = null;
        for (Shard shard : phoenixShards) {
            Assert.assertNotNull(shard.sequenceNumberRange());
            Assert.assertTrue(
                shard.sequenceNumberRange().startingSequenceNumber().endsWith("00000"));
            // parent which split, should have end sequence number
            if (shard.parentShardId() == null) {
                parentId = shard.shardId();
                Assert.assertTrue(
                    shard.sequenceNumberRange().endingSequenceNumber().endsWith("99999"));
            }
        }
        for (Shard shard : phoenixShards) {
            if (shard.parentShardId() != null) {
                Assert.assertEquals(parentId, shard.parentShardId());
            }
        }
    }

    @Test(timeout = 120000)
    public void testCreationRequestDateTimeAsNumeric() throws Exception {
        String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey", ScalarAttributeType.S,
                        "sortKey", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "OLD_IMAGE");

        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);

        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams = phoenixDBStreamsClientV2.listStreams(lsr);
        Assert.assertEquals(1, phoenixStreams.streams().size());

        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();

        String restEndpoint = "http://" + restServer.getServerAddress();
        URL url = new URL(restEndpoint);

        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/x-amz-json-1.0");
        conn.setRequestProperty("X-Amz-Target", "DynamoDBStreams_20120810.DescribeStream");

        Map<String, String> requestBody = new HashMap<>();
        requestBody.put("StreamArn", phoenixStreamArn);

        conn.setDoOutput(true);
        try (java.io.OutputStream os = conn.getOutputStream()) {
            byte[] input = new ObjectMapper().writeValueAsBytes(requestBody);
            os.write(input, 0, input.length);
        }

        StringBuilder response = new StringBuilder();
        try (java.io.BufferedReader reader = new java.io.BufferedReader(
                new java.io.InputStreamReader(conn.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
        }

        ObjectMapper mapper = new ObjectMapper();
        Map<String, Object> jsonResponse = mapper.readValue(response.toString(), Map.class);
        Map<String, Object> streamDescription =
                (Map<String, Object>) jsonResponse.get("StreamDescription");

        Object creationRequestDateTime = streamDescription.get("CreationRequestDateTime");
        Assert.assertNotNull("CreationRequestDateTime should not be null", creationRequestDateTime);

        double timestamp = ((Number) creationRequestDateTime).doubleValue();
        Assert.assertTrue("Timestamp should be positive", timestamp > 0);

        Thread.sleep(500);
        long now = System.currentTimeMillis();
        LOGGER.info("CreationRequestDateTime value: {} , now: {}", timestamp, now);
        Assert.assertTrue("Timestamp should not be in the future", timestamp <= now);

    }
}
