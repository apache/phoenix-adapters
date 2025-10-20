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
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.util.EnvironmentEdgeManager;
import org.apache.phoenix.util.ManualEnvironmentEdge;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;
import org.apache.phoenix.util.TestUtil;

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
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class TimeToLiveIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeToLiveIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

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
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
        DriverManager.registerDriver(new PhoenixTestDriver());

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
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
    public void updateTimeToLiveTest() {
        final String tableName = "1298-.Node.Stream._OverflowStack";
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);

        // enable
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        UpdateTimeToLiveResponse ddbResponse = dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        UpdateTimeToLiveResponse phoenixResponse = phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        Assert.assertEquals(ddbResponse.timeToLiveSpecification(), phoenixResponse.timeToLiveSpecification());

        // disable
        spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(false).build();
        ddbResponse = dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        phoenixResponse = phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        Assert.assertEquals(ddbResponse.timeToLiveSpecification(), phoenixResponse.timeToLiveSpecification());
    }

    @Test(timeout = 120000)
    public void describeTimeToLiveTest() {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);

        // enable
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        DescribeTimeToLiveRequest dTtlReq = DescribeTimeToLiveRequest.builder().tableName(tableName).build();
        DescribeTimeToLiveResponse ddbResponse = dynamoDbClient.describeTimeToLive(dTtlReq);
        DescribeTimeToLiveResponse phoenixResponse = phoenixDBClientV2.describeTimeToLive(dTtlReq);
        Assert.assertEquals(ddbResponse.timeToLiveDescription(), phoenixResponse. timeToLiveDescription());

        // disable
        spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(false).build();
        dynamoDbClient.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());
        ddbResponse = dynamoDbClient.describeTimeToLive(dTtlReq);
        phoenixResponse = phoenixDBClientV2.describeTimeToLive(dTtlReq);
        Assert.assertEquals(ddbResponse.timeToLiveDescription(), phoenixResponse. timeToLiveDescription());
    }

    @Test(timeout = 120000)
    public void ttlExpressionTest() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "hk", ScalarAttributeType.S, "sk", ScalarAttributeType.S);

        phoenixDBClientV2.createTable(createTableRequest);

        // enable TTL
        UpdateTimeToLiveRequest.Builder uTtlReq = UpdateTimeToLiveRequest.builder().tableName(tableName);
        TimeToLiveSpecification spec = TimeToLiveSpecification.builder().attributeName("ttlAttr").enabled(true).build();
        phoenixDBClientV2.updateTimeToLive(uTtlReq.timeToLiveSpecification(spec).build());

        long now = Instant.now().getEpochSecond();
        // item with expiry now
        Map<String, AttributeValue> item = getItem("pk1", "pk11", now);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(pir);

        // item with expiry 1d
        item = getItem("pk2", "pk22", now + TimeUnit.DAYS.toSeconds(1));
        pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(pir);

        // item with expiry 2d
        item = getItem("pk3", "pk33", now + TimeUnit.DAYS.toSeconds(2));
        pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(pir);

        // item with no ttl attribute
        item = getItem("pk4", "pk44", null);
        pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(pir);

        // scan should show all items since we did not run compaction
        ScanRequest sr = ScanRequest.builder().tableName(tableName).build();
        Assert.assertEquals(4, phoenixDBClientV2.scan(sr).items().size());

        // increment clock by 26h50min and major compact
        ManualEnvironmentEdge injectEdge = new ManualEnvironmentEdge();
        long t = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(26) + TimeUnit.MINUTES.toMillis(50);
        t = ((t / 1000) + 60) * 1000;
        EnvironmentEdgeManager.injectEdge(injectEdge);
        injectEdge.setValue(t);
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtil.doMajorCompaction(connection, PhoenixUtils.getFullTableName(tableName, false));
        }

        // max lookback is 27h so all rows should be visible
        Assert.assertEquals(4, phoenixDBClientV2.scan(sr).items().size());

        // increment clock by 15 mins and major compact
        injectEdge.incrementValue(TimeUnit.MINUTES.toMillis(15));

        // first 2 items do not show in scan after major compaction
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtil.doMajorCompaction(connection, PhoenixUtils.getFullTableName(tableName, false));
        }
        ScanResponse sres = phoenixDBClientV2.scan(sr);
        Assert.assertEquals(2, sres.items().size());
        Assert.assertEquals("pk3", sres.items().get(0).get("hk").s());
        Assert.assertEquals("pk4", sres.items().get(1).get("hk").s());

        // increment clock by 1 day, third item should not show up in scan after compaction
        injectEdge.incrementValue(TimeUnit.DAYS.toMillis(1));
        Assert.assertEquals(2, phoenixDBClientV2.scan(sr).items().size());
        try (Connection connection = DriverManager.getConnection(url)) {
            TestUtil.doMajorCompaction(connection, PhoenixUtils.getFullTableName(tableName, false));
        }
        sres = phoenixDBClientV2.scan(sr);
        Assert.assertEquals(1, sres.items().size());
        Assert.assertEquals("pk4", sres.items().get(0).get("hk").s());

        EnvironmentEdgeManager.reset();
    }

    private Map<String, AttributeValue> getItem(String pk1, String pk2, Long expiryTS) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("hk", AttributeValue.builder().s(pk1).build());
        item.put("sk", AttributeValue.builder().s(pk2).build());
        if (expiryTS != null) {
            item.put("ttlAttr", AttributeValue.builder().n(expiryTS.toString()).build());
        }
        return item;
    }
}
