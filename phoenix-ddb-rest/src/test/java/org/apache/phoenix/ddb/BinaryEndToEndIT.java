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
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

public class BinaryEndToEndIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinaryEndToEndIT.class);

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

    private String tableName;
    private String indexName;
    private static Random random = new Random(42);

    @Rule
    public TestName testName = new TestName();

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

    @Before
    public void createTableAndIndex() {
        tableName = "Binary.PK_Test-Table_" + testName.getMethodName();
        indexName = "Binary.PK_Test-Index_" + testName.getMethodName();
        CreateTableRequest request = CreateTableRequest.builder().tableName(tableName)
                .billingMode(BillingMode.PAY_PER_REQUEST).globalSecondaryIndexes(Arrays.asList(
                        GlobalSecondaryIndex.builder().indexName(indexName).keySchema(
                                        Arrays.asList(KeySchemaElement.builder()
                                                        .attributeName("index_hk").keyType(
                                                                KeyType.HASH)
                                                        .build(),
                                                KeySchemaElement.builder().attributeName("index_sk")
                                                        .keyType(KeyType.RANGE).build()

                                        )).projection(
                                        Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build()

                )).attributeDefinitions(Arrays.asList(
                        AttributeDefinition.builder().attributeName("hk")
                                .attributeType(ScalarAttributeType.B).build(),
                        AttributeDefinition.builder().attributeName("sk")
                                .attributeType(ScalarAttributeType.B).build(),
                        AttributeDefinition.builder().attributeName("index_hk")
                                .attributeType(ScalarAttributeType.B).build(),
                        AttributeDefinition.builder().attributeName("index_sk")
                                .attributeType(ScalarAttributeType.B).build()

                )).keySchema(Arrays.asList(
                        KeySchemaElement.builder().attributeName("hk").keyType(KeyType.HASH)
                                .build(),
                        KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE)
                                .build()

                )).build();
        dynamoDbClient.createTable(request);
        phoenixDBClientV2.createTable(request);
    }

    @After
    public void deleteTable() {
        DeleteTableRequest request = DeleteTableRequest.builder().tableName(tableName).build();
        phoenixDBClientV2.deleteTable(request);
        dynamoDbClient.deleteTable(request);
    }

    @Test
    public void putThenGetItem() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);
        Map<String, AttributeValue> key = getKey(item);
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void putThenDeleteItem() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);
        Map<String, AttributeValue> key = getKey(item);
        DeleteItemRequest del = DeleteItemRequest.builder().tableName(tableName).key(key).build();
        dynamoDbClient.deleteItem(del);
        phoenixDBClientV2.deleteItem(del);
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void getNonExistentItem() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);
        Map<String, AttributeValue> key = getKey(getItem(2));
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void putWithConditionThatSucceeds() {
        Map<String, AttributeValue> item = getItem(1);
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "hk");
        exprAttrNames.put("#2", "sk");
        PutItemRequest pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .expressionAttributeNames(exprAttrNames)
                .conditionExpression("attribute_not_exists(#1) AND attribute_not_exists(#2)")
                .build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);
        Map<String, AttributeValue> key = getKey(item);
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void putWithConditionThatFails() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        item.put("payload", AttributeValue.builder().n("2").build());
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "index_sk");
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":v1", item.get("index_sk"));
        pir = PutItemRequest.builder()
                .tableName(tableName)
                .item(item)
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrVals)
                .conditionExpression("#1 <> :v1")
                .build();
        try {
            dynamoDbClient.putItem(pir);
        } catch (ConditionalCheckFailedException expected) {}
        try {
            phoenixDBClientV2.putItem(pir);
        } catch (ConditionalCheckFailedException expected) {}

        Map<String, AttributeValue> key = getKey(item);
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void deleteWithConditionThatSucceeds() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        Map<String, AttributeValue> key = getKey(item);
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "index_hk");
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        exprAttrVals.put(":v1", item.get("index_hk"));
        DeleteItemRequest del = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrVals)
                .conditionExpression("#1 = :v1")
                .build();
        dynamoDbClient.deleteItem(del);
        phoenixDBClientV2.deleteItem(del);

        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void deleteWithConditionThatFails() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        Map<String, AttributeValue> key = getKey(item);
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "index_sk");
        Map<String, AttributeValue> exprAttrVals = new HashMap<>();
        byte[] index_sk_val = new byte[]{0, 0, 0};
        exprAttrVals.put(":v1", AttributeValue.builder().b(SdkBytes.fromByteArray(index_sk_val)).build());
        DeleteItemRequest del = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrVals)
                .conditionExpression("#1 = :v1")
                .build();
        try {
            dynamoDbClient.deleteItem(del);
        } catch (ConditionalCheckFailedException expected) {}
        try {
            phoenixDBClientV2.deleteItem(del);
        } catch (ConditionalCheckFailedException expected) {}

        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void deleteNonExistentItem() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        Map<String, AttributeValue> key = getKey(getItem(2));
        DeleteItemRequest del = DeleteItemRequest.builder().tableName(tableName).key(key).build();
        dynamoDbClient.deleteItem(del);
        phoenixDBClientV2.deleteItem(del);

        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(getKey(item)).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void putThenUpdateItemAndScanIndex() throws SQLException, InterruptedException {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        Map<String, AttributeValue> key = getKey(item);
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "index_hk");
        exprAttrNames.put("#2", "index_sk");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0, 0})).build());
        exprAttrVal.put(":v2", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 1})).build());
        UpdateItemRequest uir = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrVal)
                .updateExpression("SET #1 = :v1, #2 = :v2")
                .build();

        dynamoDbClient.updateItem(uir);
        phoenixDBClientV2.updateItem(uir);

        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(getKey(item)).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());

        ScanRequest sr = ScanRequest.builder().tableName(tableName).indexName(indexName).build();
        Thread.sleep(20000);
        Assert.assertEquals(dynamoDbClient.scan(sr).items(), phoenixDBClientV2.scan(sr).items());
        TestUtils.validateIndexUsed(sr, url, "FULL SCAN ");
    }

    @Test
    public void updateWithConditionThatSucceeds() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        Map<String, AttributeValue> key = getKey(item);
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "index_hk");
        exprAttrNames.put("#2", "index_sk");
        exprAttrNames.put("#3", "payload");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0, 0})).build());
        exprAttrVal.put(":v2", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 1})).build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("3").build());
        UpdateItemRequest uir = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrVal)
                .updateExpression("SET #1 = :v1, #2 = :v2")
                .conditionExpression("#3 < :v3")
                .build();

        dynamoDbClient.updateItem(uir);
        phoenixDBClientV2.updateItem(uir);

        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(getKey(item)).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void updateWithConditionThatFails() {
        Map<String, AttributeValue> item = getItem(1);
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(pir);
        phoenixDBClientV2.putItem(pir);

        Map<String, AttributeValue> key = getKey(item);
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "index_hk");
        exprAttrNames.put("#2", "index_sk");
        exprAttrNames.put("#3", "payload");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {0, 0})).build());
        exprAttrVal.put(":v2", AttributeValue.builder().b(SdkBytes.fromByteArray(new byte[] {1, 1})).build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("0").build());
        UpdateItemRequest uir = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .expressionAttributeNames(exprAttrNames)
                .expressionAttributeValues(exprAttrVal)
                .updateExpression("SET #1 = :v1, #2 = :v2")
                .conditionExpression("#3 < :v3")
                .build();

        try {
            dynamoDbClient.updateItem(uir);
        } catch (ConditionalCheckFailedException expected) {}
        try {
            phoenixDBClientV2.updateItem(uir);
        } catch (ConditionalCheckFailedException expected) {}

        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(getKey(item)).build();
        Assert.assertEquals(dynamoDbClient.getItem(gir).item(), phoenixDBClientV2.getItem(gir).item());
    }

    @Test
    public void batchWriteItems() throws SQLException, InterruptedException {
        Map<String, AttributeValue> item1 = getItem(1);
        PutItemRequest pir1 = PutItemRequest.builder().tableName(tableName).item(item1).build();
        dynamoDbClient.putItem(pir1);
        phoenixDBClientV2.putItem(pir1);
        Map<String, AttributeValue> item2 = getItem(2);
        PutItemRequest pir2 = PutItemRequest.builder().tableName(tableName).item(item2).build();
        dynamoDbClient.putItem(pir2);
        phoenixDBClientV2.putItem(pir2);

        List<WriteRequest> writeReqs = new ArrayList<>();
        writeReqs.add(WriteRequest.builder().putRequest(
                PutRequest.builder().item(getItem(3)).build()).build());
        writeReqs.add(WriteRequest.builder().putRequest(
                PutRequest.builder().item(getItem(4)).build()).build());
        writeReqs.add(WriteRequest.builder().deleteRequest(
                DeleteRequest.builder().key(getKey(item1)).build()).build());
        writeReqs.add(WriteRequest.builder().deleteRequest(
                DeleteRequest.builder().key(getKey(item2)).build()).build());
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName, writeReqs);
        BatchWriteItemRequest bwir = BatchWriteItemRequest.builder().requestItems(requestItems).build();
        dynamoDbClient.batchWriteItem(bwir);
        phoenixDBClientV2.batchWriteItem(bwir);

        ScanRequest sr = ScanRequest.builder().tableName(tableName).build();
        Assert.assertEquals(dynamoDbClient.scan(sr).items().size(), phoenixDBClientV2.scan(sr).items().size());

        sr = ScanRequest.builder().tableName(tableName).indexName(indexName).build();
        Thread.sleep(20000);
        Assert.assertEquals(dynamoDbClient.scan(sr).items().size(), phoenixDBClientV2.scan(sr).items().size());
        TestUtils.validateIndexUsed(sr, url, "FULL SCAN ");
    }

    @Test
    public void testComplexUpdateAndConditionExpressions() {
        Map<String, AttributeValue> item = getItem(1);

        // Add complex nested attributes with JSON-like structures
        item.put("itemType", AttributeValue.builder().s("widget-processor").build());
        item.put("accessConfig", AttributeValue.builder().s("[]").build());
        item.put("connectionList", AttributeValue.builder().s(
                "[{\"application\":\"main-connector\",\"hostname\":\"widget-processor.internal.example.com\",\"listeningPort\":8080}," +
                        "{\"application\":\"load-balancer\",\"hostname\":\"widget-processor.{region}.{instance}.example.com\",\"listeningPort\":443}]"
        ).build());
        item.put("configurationName", AttributeValue.builder().s("widget-processor-config").build());
        item.put("customSettings", AttributeValue.builder().s(
                "[{\"instanceId\":\"region1-east\",\"domain\":\"primary\",\"zone\":\"alpha\"," +
                        "\"connections\":[],\"settings\":{\"cache_timeout\":\"300\",\"retry_count\":\"3\"}," +
                        "\"groups\":[],\"accessRules\":[]}," +
                        "{\"instanceId\":\"region2-west\",\"domain\":\"primary\",\"zone\":\"beta\"," +
                        "\"connections\":[],\"settings\":{\"cache_timeout\":\"600\",\"retry_count\":\"5\"}," +
                        "\"groups\":[],\"accessRules\":[]}]"
        ).build());
        item.put("groups", AttributeValue.builder().s("[]").build());
        item.put("teamName", AttributeValue.builder().s("platform").build());
        item.put("metadata", AttributeValue.builder().s(
                "{\"sys:teamId\":\"team_abc123xyz789\",\"sys:teamName\":\"Foo Team\"," +
                        "\"config_name\":\"widget-validation-config\",\"sys:channelType\":\"STANDARD\"," +
                        "\"api_base_path\":\"api/v1/widgets\",\"admin_base_path\":\"api/v1/admin\"," +
                        "\"sys:aggregationKey\":\"widget-processor\",\"sys:notificationEnabled\":\"true\",\"sys:alerting\":\"enabled\"}"
        ).build());
        item.put("status", AttributeValue.builder().s("active").build());
        item.put("version", AttributeValue.builder().n("1").build());
        item.put("environment", AttributeValue.builder().s("development").build());

        // Insert initial item
        PutItemRequest initialPir = PutItemRequest.builder().tableName(tableName).item(item).build();
        dynamoDbClient.putItem(initialPir);
        phoenixDBClientV2.putItem(initialPir);

        // Test 1: Complex Conditional Put (should fail since item exists)
        Map<String, AttributeValue> newItem = new HashMap<>(item);
        newItem.put("itemType", AttributeValue.builder().s("updated-processor").build());

        Map<String, String> putExprAttrNames = new HashMap<>();
        putExprAttrNames.put("#it", "itemType");
        putExprAttrNames.put("#env", "environment");
        putExprAttrNames.put("#ver", "version");
        putExprAttrNames.put("#status", "status");

        Map<String, AttributeValue> putExprAttrVals = new HashMap<>();
        putExprAttrVals.put(":expectedType", AttributeValue.builder().s("non-existent-processor").build());
        putExprAttrVals.put(":expectedEnv", AttributeValue.builder().s("production").build());
        putExprAttrVals.put(":maxVersion", AttributeValue.builder().n("5").build());
        putExprAttrVals.put(":activeStatus", AttributeValue.builder().s("active").build());

        PutItemRequest conditionalPut = PutItemRequest.builder()
                .tableName(tableName)
                .item(newItem)
                .expressionAttributeNames(putExprAttrNames)
                .expressionAttributeValues(putExprAttrVals)
                .conditionExpression("(#it = :expectedType OR #env = :expectedEnv) AND #ver < :maxVersion AND #status = :activeStatus")
                .build();

        // Should fail on both DynamoDB and Phoenix
        boolean putFailedDynamo = false, putFailedPhoenix = false;
        try {
            dynamoDbClient.putItem(conditionalPut);
        } catch (ConditionalCheckFailedException expected) {
            putFailedDynamo = true;
        }
        try {
            phoenixDBClientV2.putItem(conditionalPut);
        } catch (ConditionalCheckFailedException expected) {
            putFailedPhoenix = true;
        }
        Assert.assertTrue("Conditional put should have failed on DynamoDB", putFailedDynamo);
        Assert.assertTrue("Conditional put should have failed on Phoenix", putFailedPhoenix);

        // Test 2: Complex Update with nested SET, ADD operations and complex condition
        Map<String, AttributeValue> key = getKey(item);

        Map<String, String> updateExprAttrNames = new HashMap<>();
        updateExprAttrNames.put("#it", "itemType");
        updateExprAttrNames.put("#ac", "accessConfig");
        updateExprAttrNames.put("#cl", "connectionList");
        updateExprAttrNames.put("#cn", "configurationName");
        updateExprAttrNames.put("#cs", "customSettings");
        updateExprAttrNames.put("#meta", "metadata");
        updateExprAttrNames.put("#ver", "version");
        updateExprAttrNames.put("#env", "environment");
        updateExprAttrNames.put("#status", "status");
        updateExprAttrNames.put("#lastUpdated", "lastUpdated");

        Map<String, AttributeValue> updateExprAttrVals = new HashMap<>();
        updateExprAttrVals.put(":newItemType", AttributeValue.builder().s("updated-widget-processor").build());
        updateExprAttrVals.put(":newAccessConfig", AttributeValue.builder().s("[{\"rule\":\"allow-admin\",\"scope\":\"read-write\"}]").build());
        updateExprAttrVals.put(":newConnectionList", AttributeValue.builder().s(
                "[{\"application\":\"updated-connector\",\"hostname\":\"updated-widget-processor.internal.example.com\",\"listeningPort\":9090}," +
                        "{\"application\":\"updated-lb\",\"hostname\":\"updated-widget-processor.{region}.{instance}.example.com\",\"listeningPort\":443}]"
        ).build());
        updateExprAttrVals.put(":newConfigName", AttributeValue.builder().s("updated-widget-processor-config").build());
        updateExprAttrVals.put(":newCustomSettings", AttributeValue.builder().s(
                "[{\"instanceId\":\"updated-region1-east\",\"domain\":\"updated-primary\",\"zone\":\"updated-alpha\"," +
                        "\"connections\":[{\"type\":\"internal\"}],\"settings\":{\"updated_cache_timeout\":\"450\",\"updated_retry_count\":\"4\"}," +
                        "\"groups\":[\"group1\",\"group2\"],\"accessRules\":[{\"rule\":\"updated-rule\"}]}]"
        ).build());
        updateExprAttrVals.put(":newMetadata", AttributeValue.builder().s(
                "{\"sys:teamId\":\"updated_team_xyz789abc123\",\"sys:teamName\":\"Bar Team\"," +
                        "\"config_name\":\"updated-widget-validation-config\",\"sys:channelType\":\"ENHANCED\"," +
                        "\"api_base_path\":\"api/v2/widgets\",\"admin_base_path\":\"api/v2/admin\"," +
                        "\"sys:aggregationKey\":\"updated-widget-processor\",\"sys:notificationEnabled\":\"false\",\"sys:alerting\":\"disabled\"}"
        ).build());
        updateExprAttrVals.put(":versionIncrement", AttributeValue.builder().n("1").build());
        updateExprAttrVals.put(":currentTimestamp", AttributeValue.builder().n(String.valueOf(System.currentTimeMillis())).build());
        updateExprAttrVals.put(":expectedType", AttributeValue.builder().s("widget-processor").build());
        updateExprAttrVals.put(":expectedEnv", AttributeValue.builder().s("development").build());
        updateExprAttrVals.put(":minVersion", AttributeValue.builder().n("1").build());
        updateExprAttrVals.put(":maxVersion", AttributeValue.builder().n("10").build());
        updateExprAttrVals.put(":activeStatus", AttributeValue.builder().s("active").build());

        UpdateItemRequest complexUpdate = UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .expressionAttributeNames(updateExprAttrNames)
                .expressionAttributeValues(updateExprAttrVals)
                .updateExpression("SET #it = :newItemType, #ac = :newAccessConfig, #cl = :newConnectionList, " +
                        "#cn = :newConfigName, #cs = :newCustomSettings, #meta = :newMetadata, " +
                        "#lastUpdated = :currentTimestamp " +
                        "ADD #ver :versionIncrement")
                .conditionExpression("(#it = :expectedType AND #env = :expectedEnv) AND " +
                        "#ver >= :minVersion AND #ver < :maxVersion AND #status = :activeStatus")
                .build();

        // Should succeed on both DynamoDB and Phoenix
        dynamoDbClient.updateItem(complexUpdate);
        phoenixDBClientV2.updateItem(complexUpdate);

        // Verify the update worked
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Map<String, AttributeValue> dynamoResult = dynamoDbClient.getItem(gir).item();
        Map<String, AttributeValue> phoenixResult = phoenixDBClientV2.getItem(gir).item();
        Assert.assertEquals("Results should match after complex update", dynamoResult, phoenixResult);
        Assert.assertEquals("Item type should be updated", "updated-widget-processor", dynamoResult.get("itemType").s());
        Assert.assertEquals("Version should be incremented", "2", dynamoResult.get("version").n());

        // Test 3: Complex Conditional Delete with nested conditions
        Map<String, String> deleteExprAttrNames = new HashMap<>();
        deleteExprAttrNames.put("#it", "itemType");
        deleteExprAttrNames.put("#env", "environment");
        deleteExprAttrNames.put("#ver", "version");
        deleteExprAttrNames.put("#status", "status");
        deleteExprAttrNames.put("#team", "teamName");
        deleteExprAttrNames.put("#lu", "lastUpdated");

        Map<String, AttributeValue> deleteExprAttrVals = new HashMap<>();
        deleteExprAttrVals.put(":expectedType", AttributeValue.builder().s("updated-widget-processor").build());
        deleteExprAttrVals.put(":expectedEnv", AttributeValue.builder().s("development").build());
        deleteExprAttrVals.put(":expectedVersion", AttributeValue.builder().n("2").build());
        deleteExprAttrVals.put(":activeStatus", AttributeValue.builder().s("active").build());
        deleteExprAttrVals.put(":expectedTeam", AttributeValue.builder().s("platform").build());
        deleteExprAttrVals.put(":recentTime", AttributeValue.builder().n(String.valueOf(System.currentTimeMillis() - 60000)).build()); // 1 minute ago

        DeleteItemRequest complexDelete = DeleteItemRequest.builder()
                .tableName(tableName)
                .key(key)
                .expressionAttributeNames(deleteExprAttrNames)
                .expressionAttributeValues(deleteExprAttrVals)
                .conditionExpression("(#it = :expectedType AND #env = :expectedEnv) AND " +
                        "#ver = :expectedVersion AND #status = :activeStatus AND " +
                        "#team = :expectedTeam AND #lu > :recentTime")
                .build();

        // Should succeed on both DynamoDB and Phoenix
        dynamoDbClient.deleteItem(complexDelete);
        phoenixDBClientV2.deleteItem(complexDelete);

        // Verify the item was deleted
        GetItemRequest finalGir = GetItemRequest.builder().tableName(tableName).key(key).build();
        Assert.assertEquals("Item should be deleted from both systems",
                dynamoDbClient.getItem(finalGir).item(),
                phoenixDBClientV2.getItem(finalGir).item());
        Assert.assertTrue("Item should be deleted",
                dynamoDbClient.getItem(finalGir).item() == null || dynamoDbClient.getItem(finalGir).item().isEmpty());
    }

    @Test
    public void queryTableWithProjectionAndPagination() {
        //insert items with same hk, different sk
        byte[] hk = new byte[15];
        random.nextBytes(hk);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Map<String, AttributeValue> item = getItem(i);
            item.put("hk", AttributeValue.builder().b(SdkBytes.fromByteArray(hk)).build());
            items.add(item);
            PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
            dynamoDbClient.putItem(pir);
            phoenixDBClientV2.putItem(pir);
        }

        // sort by sk
        items.sort((o1, o2) -> {
            byte[] b1 = o1.get("sk").b().asByteArray();
            byte[] b2 = o2.get("sk").b().asByteArray();
            return Bytes.compareTo(b1, b2);
        });

        // choose a middle range of sk
        byte[] startSk = items.get(6).get("sk").b().asByteArray();
        byte[] endSk = items.get(13).get("sk").b().asByteArray();

        Map<String, AttributeValue> exprVals = new HashMap<>();
        exprVals.put(":hk", AttributeValue.builder().b(SdkBytes.fromByteArray(hk)).build());
        exprVals.put(":startSk", AttributeValue.builder().b(SdkBytes.fromByteArray(startSk)).build());
        exprVals.put(":endSk", AttributeValue.builder().b(SdkBytes.fromByteArray(endSk)).build());
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName)
                .keyConditionExpression("hk = :hk AND sk BETWEEN :startSk AND :endSk")
                .projectionExpression("payload")
                .limit(2)
                .expressionAttributeValues(exprVals);
        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
        qr = QueryRequest.builder().tableName(tableName)
                .keyConditionExpression("hk = :hk AND sk BETWEEN :startSk AND :endSk")
                .projectionExpression("payload")
                .limit(2)
                .scanIndexForward(false)
                .expressionAttributeValues(exprVals);
        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
    }

    @Test
    public void queryIndexForwardWithPagination() throws InterruptedException {
        queryIndexWithPagination(true);
    }

    @Test
    public void queryIndexBackwardWithPagination() throws InterruptedException {
        queryIndexWithPagination(false);
    }

    private void queryIndexWithPagination(boolean scanIndexForward) throws InterruptedException {
        //insert items with same index_hk, different index_sk
        byte[] index_hk = new byte[15];
        random.nextBytes(index_hk);
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Map<String, AttributeValue> item = getItem(i);
            item.put("index_hk", AttributeValue.builder().b(SdkBytes.fromByteArray(index_hk)).build());
            items.add(item);
            PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
            dynamoDbClient.putItem(pir);
            phoenixDBClientV2.putItem(pir);
        }

        // sort by index_sk
        items.sort((o1, o2) -> {
            byte[] b1 = o1.get("index_sk").b().asByteArray();
            byte[] b2 = o2.get("index_sk").b().asByteArray();
            return Bytes.compareTo(b1, b2);
        });

        // choose a middle range of index_sk
        byte[] startSk = items.get(4).get("index_sk").b().asByteArray();
        byte[] endSk = items.get(16).get("index_sk").b().asByteArray();

        Map<String, AttributeValue> exprVals = new HashMap<>();
        exprVals.put(":indexHk", AttributeValue.builder().b(SdkBytes.fromByteArray(index_hk)).build());
        exprVals.put(":startIndexSk", AttributeValue.builder().b(SdkBytes.fromByteArray(startSk)).build());
        exprVals.put(":endIndexSk", AttributeValue.builder().b(SdkBytes.fromByteArray(endSk)).build());
        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName).indexName(indexName)
                .keyConditionExpression("index_hk = :indexHk AND index_sk BETWEEN :startIndexSk AND :endIndexSk")
                .projectionExpression("payload")
                .limit(3)
                .expressionAttributeValues(exprVals)
                .scanIndexForward(scanIndexForward);
        Thread.sleep(20000);
        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
    }

    @Test
    public void queryIndexWithEqualsSortKeyAndPagination() throws InterruptedException {
        // Common GSI key values (like Phoenix test: GSI_HK and GSI_SK are the same for all rows)
        byte[] gsiHk = new byte[]{0x00, 0x00, 0x00, 0x0A};  // GSI hash key
        byte[] gsiSk = "1".getBytes();                       // GSI sort key = "1"

        // Row 1: HK=[0x0B,0x01], SK="1" - ExclusiveStartKey row, should be EXCLUDED
        byte[] hk1 = new byte[]{0x0B, 0x01};
        byte[] sk1 = "1".getBytes();
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("hk", AttributeValue.builder().b(SdkBytes.fromByteArray(hk1)).build());
        item1.put("sk", AttributeValue.builder().b(SdkBytes.fromByteArray(sk1)).build());
        item1.put("index_hk", AttributeValue.builder().b(SdkBytes.fromByteArray(gsiHk)).build());
        item1.put("index_sk", AttributeValue.builder().b(SdkBytes.fromByteArray(gsiSk)).build());
        item1.put("data", AttributeValue.builder().s("row1").build());

        // Row 2: HK=[0x0B,0x01], SK="2" - should be INCLUDED (SK "2" > "1")
        byte[] hk2 = new byte[]{0x0B, 0x01};
        byte[] sk2 = "2".getBytes();
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("hk", AttributeValue.builder().b(SdkBytes.fromByteArray(hk2)).build());
        item2.put("sk", AttributeValue.builder().b(SdkBytes.fromByteArray(sk2)).build());
        item2.put("index_hk", AttributeValue.builder().b(SdkBytes.fromByteArray(gsiHk)).build());
        item2.put("index_sk", AttributeValue.builder().b(SdkBytes.fromByteArray(gsiSk)).build());
        item2.put("data", AttributeValue.builder().s("row2").build());

        // Row 3: HK=[0x0B,0x02], SK="1" - should be INCLUDED (HK [0x0B,0x02] > [0x0B,0x01])
        byte[] hk3 = new byte[]{0x0B, 0x02};
        byte[] sk3 = "1".getBytes();
        Map<String, AttributeValue> item3 = new HashMap<>();
        item3.put("hk", AttributeValue.builder().b(SdkBytes.fromByteArray(hk3)).build());
        item3.put("sk", AttributeValue.builder().b(SdkBytes.fromByteArray(sk3)).build());
        item3.put("index_hk", AttributeValue.builder().b(SdkBytes.fromByteArray(gsiHk)).build());
        item3.put("index_sk", AttributeValue.builder().b(SdkBytes.fromByteArray(gsiSk)).build());
        item3.put("data", AttributeValue.builder().s("row3").build());

        // Insert all rows
        dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item1).build());
        phoenixDBClientV2.putItem(PutItemRequest.builder().tableName(tableName).item(item1).build());
        dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item2).build());
        phoenixDBClientV2.putItem(PutItemRequest.builder().tableName(tableName).item(item2).build());
        dynamoDbClient.putItem(PutItemRequest.builder().tableName(tableName).item(item3).build());
        phoenixDBClientV2.putItem(PutItemRequest.builder().tableName(tableName).item(item3).build());

        // Query GSI with index_hk = gsiHk AND index_sk = gsiSk (EQUALS on sort key)
        Map<String, AttributeValue> exprVals = new HashMap<>();
        exprVals.put(":indexHk", AttributeValue.builder().b(SdkBytes.fromByteArray(gsiHk)).build());
        exprVals.put(":indexSk", AttributeValue.builder().b(SdkBytes.fromByteArray(gsiSk)).build());

        QueryRequest.Builder qr = QueryRequest.builder()
                .tableName(tableName)
                .indexName(indexName)
                .keyConditionExpression("index_hk = :indexHk AND index_sk = :indexSk")
                .limit(1)
                .expressionAttributeValues(exprVals);

        Thread.sleep(20000);
        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
    }

    @Test
    public void scanTableWithFilterProjectionAndPagination() {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Map<String, AttributeValue> item = getItem(i);
            items.add(item);
            PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
            dynamoDbClient.putItem(pir);
            phoenixDBClientV2.putItem(pir);
        }
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":val", items.get(45).get("index_hk"));
        ScanRequest.Builder sr = ScanRequest.builder()
                .tableName(tableName)
                .limit(7)
                .projectionExpression("index_sk")
                .filterExpression("index_hk > :val")
                .expressionAttributeValues(exprAttrValues);
        List<Map<String, AttributeValue>> phoenixResult = new ArrayList<>();
        ScanResponse phoenixResponse;
        do {
            phoenixResponse = phoenixDBClientV2.scan(sr.build());
            phoenixResult.addAll(phoenixResponse.items());
            sr.exclusiveStartKey(phoenixResponse.lastEvaluatedKey());
        } while (phoenixResponse.hasLastEvaluatedKey());
        List<Map<String, AttributeValue>> ddbResult = new ArrayList<>();
        ScanResponse ddbResponse;
        do {
            ddbResponse = dynamoDbClient.scan(sr.build());
            ddbResult.addAll(ddbResponse.items());
            sr.exclusiveStartKey(ddbResponse.lastEvaluatedKey());
        } while (ddbResponse.hasLastEvaluatedKey());
        Assert.assertEquals(ddbResult.size(), phoenixResult.size());
        for (Map<String, AttributeValue> item : ddbResult) {
            Assert.assertTrue(phoenixResult.contains(item));
        }
    }

    @Test
    public void scanIndexWithFilterProjectionAndPagination() throws InterruptedException {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            Map<String, AttributeValue> item = getItem(i);
            items.add(item);
            PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
            dynamoDbClient.putItem(pir);
            phoenixDBClientV2.putItem(pir);
        }
        Map<String, AttributeValue> exprAttrValues = new HashMap<>();
        exprAttrValues.put(":val", items.get(78).get("index_sk"));
        ScanRequest.Builder sr = ScanRequest.builder()
                .tableName(tableName)
                .indexName(indexName)
                .limit(7)
                .projectionExpression("sk")
                .filterExpression("index_sk < :val")
                .expressionAttributeValues(exprAttrValues);
        List<Map<String, AttributeValue>> phoenixResult = new ArrayList<>();
        Thread.sleep(20000);
        ScanResponse phoenixResponse;
        do {
            phoenixResponse = phoenixDBClientV2.scan(sr.build());
            phoenixResult.addAll(phoenixResponse.items());
            sr.exclusiveStartKey(phoenixResponse.lastEvaluatedKey());
        } while (phoenixResponse.hasLastEvaluatedKey());
        List<Map<String, AttributeValue>> ddbResult = new ArrayList<>();
        ScanResponse ddbResponse;
        do {
            ddbResponse = dynamoDbClient.scan(sr.build());
            ddbResult.addAll(ddbResponse.items());
            sr.exclusiveStartKey(ddbResponse.lastEvaluatedKey());
        } while (ddbResponse.hasLastEvaluatedKey());
        Assert.assertEquals(ddbResult.size(), phoenixResult.size());
        for (Map<String, AttributeValue> item : ddbResult) {
            Assert.assertTrue(phoenixResult.contains(item));
        }
    }

    @Test
    public void batchGetItems() {
        List<Map<String, AttributeValue>> keys = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            Map<String, AttributeValue> item = getItem(i);
            keys.add(getKey(item));
            PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).build();
            dynamoDbClient.putItem(pir);
            phoenixDBClientV2.putItem(pir);
        }
        KeysAndAttributes keysAndAttr = KeysAndAttributes.builder().keys(keys.subList(11, 19)).projectionExpression("payload").build();
        Map<String, KeysAndAttributes> requestItems = new HashMap<>();
        requestItems.put(tableName, keysAndAttr);
        BatchGetItemRequest bgir = BatchGetItemRequest.builder().requestItems(requestItems).build();
        List<Map<String, AttributeValue>> ddbItems = dynamoDbClient.batchGetItem(bgir).responses().get(tableName);
        List<Map<String, AttributeValue>> phoenixItems = phoenixDBClientV2.batchGetItem(bgir).responses().get(tableName);
        Assert.assertEquals(ddbItems.size(), phoenixItems.size());
        for (Map<String, AttributeValue> item : ddbItems) {
            Assert.assertTrue(phoenixItems.contains(item));
        }
    }

    public static Map<String, AttributeValue> getItem(Integer num) {
        byte[] hk = new byte[15];
        byte[] sk = new byte[23];
        byte[] index_hk = new byte[11];
        byte[] index_sk = new byte[7];
        random.nextBytes(hk);
        random.nextBytes(sk);
        random.nextBytes(index_hk);
        random.nextBytes(index_sk);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("hk", AttributeValue.builder().b(SdkBytes.fromByteArray(hk)).build());
        item.put("sk", AttributeValue.builder().b(SdkBytes.fromByteArray(sk)).build());
        item.put("index_hk", AttributeValue.builder().b(SdkBytes.fromByteArray(index_hk)).build());
        item.put("index_sk", AttributeValue.builder().b(SdkBytes.fromByteArray(index_sk)).build());
        item.put("payload", AttributeValue.builder().n(num.toString()).build());
        return item;
    }

    public static Map<String, AttributeValue> getKey(Map<String, AttributeValue> item) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("hk", item.get("hk"));
        key.put("sk", item.get("sk"));
        return key;
    }
}
