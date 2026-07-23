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
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;
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
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.Stream;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.sql.DriverManager;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ListStreamsIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListStreamsIT.class);

    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbStreamsClient dynamoDbStreamsClient;
    private static DynamoDbClient phoenixDBClientV2;
    private static DynamoDbStreamsClient phoenixDBStreamsClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    private static List<String> tableNames = new ArrayList<>();

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        TestUtils.awaitPhoenixReady(url);

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        phoenixDBStreamsClientV2 = LocalDynamoDB.createV2StreamsClient("http://" + restServer.getServerAddress());
        dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();
        dynamoDbStreamsClient = LocalDynamoDbTestBase.localDynamoDb().createV2StreamsClient();;

        for (int i=0; i<10; i++) {
            String tableName = generateUniqueName();
            tableNames.add(tableName);
            CreateTableRequest createTableRequest =
                    DDLTestUtils.getCreateTableRequest(tableName, "hashKey",
                            ScalarAttributeType.B, "sortKey", ScalarAttributeType.N);
            createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_IMAGE");
            phoenixDBClientV2.createTable(createTableRequest);
            dynamoDbClient.createTable(createTableRequest);
        }
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
    public void testListStreamsForTable() throws ParseException {
        String tableName = tableNames.get(0);
        ListStreamsRequest lsr = ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixResult = phoenixDBStreamsClientV2.listStreams(lsr);
        ListStreamsResponse dynamoResult = dynamoDbStreamsClient.listStreams(lsr);
        LOGGER.info("ListStreamsResponse in Phoenix: " + phoenixResult);
        LOGGER.info("ListStreamsResponse in DDB: " + dynamoResult);

        Assert.assertEquals(dynamoResult.streams().size(), phoenixResult.streams().size());
        Stream phoenixStream = phoenixResult.streams().get(0);
        Assert.assertEquals(tableName, phoenixStream.tableName());
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");
        df.setTimeZone(TimeZone.getTimeZone("UTC"));
        df.parse(phoenixStream.streamLabel());
        Assert.assertTrue(phoenixStream.streamArn().endsWith("/stream/" + phoenixStream.streamLabel()));
    }

    @Test(timeout = 120000)
    public void testListStreamsAllTables() {
        ListStreamsRequest lsr = ListStreamsRequest.builder().build();
        ListStreamsResponse phoenixResult = phoenixDBStreamsClientV2.listStreams(lsr);
        ListStreamsResponse dynamoResult = dynamoDbStreamsClient.listStreams(lsr);
        LOGGER.info("ListStreamsResponse in Phoenix: " + phoenixResult);
        LOGGER.info("ListStreamsResponse in DDB: " + dynamoResult);
        Assert.assertEquals(dynamoResult.streams().size(), phoenixResult.streams().size());
    }

    @Test(timeout = 120000)
    public void testListStreamsAllTablesWithPagination() {
        ListStreamsRequest.Builder lsr = ListStreamsRequest.builder().limit(3);
        ListStreamsResponse ddbResponse;
        List<Stream> ddbStreams = new ArrayList<>();
        do {
            ddbResponse = dynamoDbStreamsClient.listStreams(lsr.build());
            ddbStreams.addAll(ddbResponse.streams());
            lsr.exclusiveStartStreamArn(ddbResponse.lastEvaluatedStreamArn());
        } while (!ddbResponse.streams().isEmpty() && ddbResponse.lastEvaluatedStreamArn() != null);
        Assert.assertEquals(10, ddbStreams.size());

        lsr.exclusiveStartStreamArn(null);
        ListStreamsResponse phoenixResponse;
        List<Stream> phoenixStreams = new ArrayList<>();
        do {
            phoenixResponse = phoenixDBStreamsClientV2.listStreams(lsr.build());
            phoenixStreams.addAll(phoenixResponse.streams());
            lsr.exclusiveStartStreamArn(phoenixResponse.lastEvaluatedStreamArn());
        } while (!phoenixResponse.streams().isEmpty() && phoenixResponse.lastEvaluatedStreamArn() != null);
        Assert.assertEquals(10, phoenixStreams.size());
    }
}
