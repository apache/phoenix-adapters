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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
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
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

@RunWith(Parameterized.class)
public class BatchWriteItem2IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchWriteItem2IT.class);

    @Parameterized.Parameter(0)
    public ScalarAttributeType partitionKeyType;
    
    @Parameterized.Parameter(1)
    public ScalarAttributeType sortKeyType;

    @Parameterized.Parameters(name = "pk_{0}_sk_{1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            {ScalarAttributeType.S, ScalarAttributeType.S},
            {ScalarAttributeType.N, null},
            {ScalarAttributeType.S, ScalarAttributeType.N},
            {ScalarAttributeType.B, ScalarAttributeType.S},
            {ScalarAttributeType.N, ScalarAttributeType.B}
        });
    }

    private static DynamoDbClient dynamoDbClient;
    private static DynamoDbClient phoenixDBClientV2;
    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;
    
    private static final Random RANDOM = new Random(12345);
    private static final int ITERATIONS = 3;
    private static final int ITEMS_PER_BATCH = 20;
    private static final int INITIAL_ITEMS_PER_TABLE = 93;

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

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("Started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
        dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();
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

    @Test(timeout = 600000)
    public void testBatchWriteOperations() {
        String tableName = "batch_write_test_" + partitionKeyType + "_" + 
            (sortKeyType != null ? sortKeyType : "NONE");
        
        createTable(tableName);
        populateInitialData(tableName);
        
        for (int iteration = 1; iteration <= ITERATIONS; iteration++) {
            performBatchWriteOperations(tableName, iteration);
            validateTableConsistency(tableName);
        }
    }

    private void createTable(String tableName) {
        CreateTableRequest createRequest = DDLTestUtils.getCreateTableRequest(
            tableName, "pk", partitionKeyType, sortKeyType != null ? "sk" : null, sortKeyType);
            
        phoenixDBClientV2.createTable(createRequest);
        dynamoDbClient.createTable(createRequest);
    }

    private void populateInitialData(String tableName) {
        List<WriteRequest> writeRequests = new ArrayList<>();
        
        for (int i = 0; i < INITIAL_ITEMS_PER_TABLE; i++) {
            Map<String, AttributeValue> item = createItem("init", i);
            writeRequests.add(WriteRequest.builder()
                .putRequest(PutRequest.builder().item(item).build())
                .build());
                
            if (writeRequests.size() == 25) {
                executeBatchWrite(tableName, writeRequests);
                writeRequests.clear();
            }
        }
        
        if (!writeRequests.isEmpty()) {
            executeBatchWrite(tableName, writeRequests);
        }
    }

    private void performBatchWriteOperations(String tableName, int iteration) {
        List<WriteRequest> writeRequests = new ArrayList<>();
        
        for (int i = 0; i < ITEMS_PER_BATCH / 2; i++) {
            Map<String, AttributeValue> item = createItem("iter" + iteration, i);
            writeRequests.add(WriteRequest.builder()
                .putRequest(PutRequest.builder().item(item).build())
                .build());
        }
        
        int deleteStartIndex = (iteration - 1) * (ITEMS_PER_BATCH / 2);
        for (int i = 0; i < ITEMS_PER_BATCH / 2; i++) {
            Map<String, AttributeValue> key = createKey("init", deleteStartIndex + i);
            writeRequests.add(WriteRequest.builder()
                .deleteRequest(DeleteRequest.builder().key(key).build())
                .build());
        }
        
        executeBatchWrite(tableName, writeRequests);
    }

    private void executeBatchWrite(String tableName, List<WriteRequest> writeRequests) {
        Map<String, List<WriteRequest>> requestMap = new HashMap<>();
        requestMap.put(tableName, writeRequests);
        
        BatchWriteItemRequest batchRequest = BatchWriteItemRequest.builder()
            .requestItems(requestMap)
            .build();
            
        phoenixDBClientV2.batchWriteItem(batchRequest);
        dynamoDbClient.batchWriteItem(batchRequest);
    }

    private void validateTableConsistency(String tableName) {
        ScanRequest.Builder scanRequest = ScanRequest.builder().tableName(tableName);
        TestUtils.compareScanOutputs(scanRequest, phoenixDBClientV2, dynamoDbClient,
            "pk", sortKeyType != null ? "sk" : null, partitionKeyType, sortKeyType);
    }

    private Map<String, AttributeValue> createItem(String prefix, int index) {
        Map<String, AttributeValue> item = new HashMap<>();
        
        item.put("pk", createAttributeValue(partitionKeyType, prefix + "_pk_" + index, index));
        
        if (sortKeyType != null) {
            item.put("sk", createAttributeValue(sortKeyType, prefix + "_sk_" + index, index));
        }
        
        item.put("data", AttributeValue.builder()
            .s("data_" + prefix + "_" + index + "_" + System.currentTimeMillis()).build());
        item.put("counter", AttributeValue.builder()
            .n(String.valueOf(RANDOM.nextInt(1000))).build());
        item.put("flag", AttributeValue.builder().bool(RANDOM.nextBoolean()).build());
        
        return item;
    }

    private Map<String, AttributeValue> createKey(String prefix, int index) {
        Map<String, AttributeValue> key = new HashMap<>();
        
        key.put("pk", createAttributeValue(partitionKeyType, prefix + "_pk_" + index, index));
        
        if (sortKeyType != null) {
            key.put("sk", createAttributeValue(sortKeyType, prefix + "_sk_" + index, index));
        }
        
        return key;
    }

    private AttributeValue createAttributeValue(ScalarAttributeType type, String stringValue, int intValue) {
        switch (type) {
            case S:
                return AttributeValue.builder().s(stringValue).build();
            case N:
                return AttributeValue.builder().n(String.valueOf((stringValue.hashCode() % 1000) + intValue)).build();
            case B:
                byte[] bytes = (stringValue + "_" + intValue).getBytes();
                return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }
}
