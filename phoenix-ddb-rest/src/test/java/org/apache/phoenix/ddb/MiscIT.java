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

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeAction;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.AttributeValueUpdate;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.ExpectedAttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ReturnValue;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.coprocessor.PhoenixMasterObserver;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.thirdparty.com.google.common.collect.Maps;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ReadOnlyProps;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class MiscIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(MiscIT.class);

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

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
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

    @Test
    public void testMixWorkflows1() throws Exception {
        Misc1Util.test1(dynamoDbClient, phoenixDBClientV2, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);
    }

    @Test
    public void testMixWorkflows2() throws Exception {
        Misc1Util.test2(dynamoDbClient, phoenixDBClientV2, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);
    }

    @Test
    public void testMixWorkflows4() throws Exception {
        Misc1Util.test3(dynamoDbClient, phoenixDBClientV2, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);
    }

    @Test(timeout = 120000)
    public void testMixWorkflows3() throws Exception {
        final String tableName = "tests";

        createTestTable(tableName);

        Map<String, AttributeValue> updateKey = new HashMap<>();
        updateKey.put("sk", AttributeValue.builder().s("Data").build());
        updateKey.put("pk",
                AttributeValue.builder().s("TestRun#65156626-2a8c-4f5d-9970-8ddf6394a48b").build());

        Map<String, AttributeValueUpdate> attributeUpdates = new HashMap<>();
        attributeUpdates.put("fireAndForget",
                AttributeValueUpdate.builder().value(AttributeValue.builder().n("0").build())
                        .action(AttributeAction.PUT).build());
        attributeUpdates.put("executionStatus",
                AttributeValueUpdate.builder().value(AttributeValue.builder().s("COMPLETE").build())
                        .action(AttributeAction.PUT).build());
        attributeUpdates.put("itemVersion",
                AttributeValueUpdate.builder().value(AttributeValue.builder().n("1").build())
                        .action(AttributeAction.PUT).build());
        attributeUpdates.put("tp_tg", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s("foundation").build()).action(AttributeAction.PUT)
                .build());
        attributeUpdates.put("started", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s("2025-08-22T04:12:21.028Z").build())
                .action(AttributeAction.PUT).build());
        attributeUpdates.put("id", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s("65156626-2a8c-4f5d-9970-8ddf6394a48b").build())
                .action(AttributeAction.PUT).build());
        attributeUpdates.put("tgt", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().s("st12").build())
                .action(AttributeAction.PUT).build());
        attributeUpdates.put("expireAt", AttributeValueUpdate.builder()
                .value(AttributeValue.builder().n("1850443956").build()).action(AttributeAction.PUT)
                .build());
        attributeUpdates.put("status",
                AttributeValueUpdate.builder().value(AttributeValue.builder().s("NO_TESTS").build())
                        .action(AttributeAction.PUT).build());
        attributeUpdates.put("master",
                AttributeValueUpdate.builder().value(AttributeValue.builder().n("1").build())
                        .action(AttributeAction.PUT).build());

        Map<String, ExpectedAttributeValue> updateExpected = new HashMap<>();
        updateExpected.put("itemVersion", ExpectedAttributeValue.builder().exists(false).build());

        UpdateItemRequest updateRequest =
                UpdateItemRequest.builder().tableName(tableName).key(updateKey)
                        .attributeUpdates(attributeUpdates).expected(updateExpected)
                        .returnValues(ReturnValue.ALL_NEW).build();

        UpdateItemResponse dynamoUpdateResponse = dynamoDbClient.updateItem(updateRequest);
        UpdateItemResponse phoenixUpdateResponse = phoenixDBClientV2.updateItem(updateRequest);

        Assert.assertEquals("Update attributes should match", dynamoUpdateResponse.attributes(),
                phoenixUpdateResponse.attributes());

        Map<String, AttributeValue> putItem = new HashMap<>();
        putItem.put("fireAndForget", AttributeValue.builder().n("0").build());
        putItem.put("executionStatus", AttributeValue.builder().s("COMPLETE").build());
        putItem.put("itemVersion", AttributeValue.builder().n("1").build());
        putItem.put("tp_tg", AttributeValue.builder().s("foundation").build());
        putItem.put("sk", AttributeValue.builder().s("Data").build());
        putItem.put("started", AttributeValue.builder().s("2025-08-22T04:12:21.028Z").build());
        putItem.put("id",
                AttributeValue.builder().s("65156626-2a8c-4f5d-9970-8ddf6394a48b").build());
        putItem.put("pk",
                AttributeValue.builder().s("TestRun#65156626-2a8c-4f5d-9970-8ddf6394a48b").build());
        putItem.put("tgt", AttributeValue.builder().s("st12").build());
        putItem.put("expireAt", AttributeValue.builder().n("1850443956").build());
        putItem.put("status", AttributeValue.builder().s("NO_TESTS").build());
        putItem.put("master", AttributeValue.builder().n("1").build());

        Map<String, ExpectedAttributeValue> putExpected = new HashMap<>();
        putExpected.put("itemVersion", ExpectedAttributeValue.builder().exists(false).build());

        PutItemRequest putItemRequest =
                PutItemRequest.builder().tableName(tableName).item(putItem).expected(putExpected)
                        .returnValues(ReturnValue.ALL_OLD).build();

        try {
            dynamoDbClient.putItem(putItemRequest);
            Assert.fail("Should have failed with an exception");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }
        try {
            phoenixDBClientV2.putItem(putItemRequest);
            Assert.fail("Should have failed with an exception");
        } catch (ConditionalCheckFailedException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        GetItemRequest getItemRequest = GetItemRequest.builder().tableName(tableName)
                .key(updateKey)
                .build();
        GetItemResponse dynamoGetResponse = dynamoDbClient.getItem(getItemRequest);
        GetItemResponse phoenixGetResponse = phoenixDBClientV2.getItem(getItemRequest);

        Assert.assertTrue("DynamoDB should have the item", dynamoGetResponse.hasItem());
        Assert.assertTrue("Phoenix should have the item", phoenixGetResponse.hasItem());
        Assert.assertEquals("Final item content should match between DynamoDB and Phoenix",
                dynamoGetResponse.item(), phoenixGetResponse.item());

    }

    private void createTestTable(String tableName) {
        dynamoDbClient.createTable(
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S));
        phoenixDBClientV2.createTable(
                DDLTestUtils.getCreateTableRequest(tableName, "pk", ScalarAttributeType.S, "sk",
                        ScalarAttributeType.S));
    }

}
