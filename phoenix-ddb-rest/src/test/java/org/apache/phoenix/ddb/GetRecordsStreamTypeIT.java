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
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

/**
 * Test different stream types.
 * Each test does 2 puts, 1 update and 1 delete.
 * Also test nextShardIterator usage using different limits in GetRecords API
 */
@RunWith(Parameterized.class)
public class GetRecordsStreamTypeIT extends GetRecordsBaseTest  {
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsStreamTypeIT.class);

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
        TestUtils.awaitPhoenixReady(url);

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
    public void testGetRecords() throws Exception {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.S, "PK2", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, this.streamType);

        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);
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
