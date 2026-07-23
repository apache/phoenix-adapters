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

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class JMXMetricsIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(JMXMetricsIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

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
    }

    @AfterClass
    public static void stop() throws Exception {
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

    @Ignore("This test flaps.")
    @Test
    public void testJMXMetrics() throws Exception {
        final String tableName = "TestApiMetrics";
        
        // Perform multiple operations that should generate metrics
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        
        // Add some items using PutItem
        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("PK1", AttributeValue.builder().s("key1").build());
        item1.put("data", AttributeValue.builder().s("value1").build());
        phoenixDBClientV2.putItem(builder -> builder.tableName(tableName).item(item1));
        
        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("PK1", AttributeValue.builder().s("key2").build());
        item2.put("data", AttributeValue.builder().s("value2").build());
        phoenixDBClientV2.putItem(builder -> builder.tableName(tableName).item(item2));
        
        // Get an item using GetItem
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK1", AttributeValue.builder().s("key1").build());
        phoenixDBClientV2.getItem(builder -> builder.tableName(tableName).key(key));
        
        // Scan the table
        phoenixDBClientV2.scan(builder -> builder.tableName(tableName).limit(10));
        
        // Query the table
        Map<String, AttributeValue> expressionValues = new HashMap<>();
        expressionValues.put(":pk", AttributeValue.builder().s("key1").build());
        phoenixDBClientV2.query(builder -> builder
                .tableName(tableName)
                .keyConditionExpression("PK1 = :pk")
                .expressionAttributeValues(expressionValues));
        
        // List tables
        phoenixDBClientV2.listTables();
        
        // Describe the table
        phoenixDBClientV2.describeTable(builder -> builder.tableName(tableName));
        
        // Retry mechanism for getting JMX metrics in case of timing issues
        Map<String, Object> phoenixRestBean = null;
        int maxRetries = 10;
        int retryCount = 0;
        
        while (phoenixRestBean == null && retryCount < maxRetries) {
            try {
                phoenixRestBean = getPhoenixRestJMXBean();
                if (phoenixRestBean != null) {
                    // Check if we have the expected number of requests
                    Object requests = phoenixRestBean.get("requests");
                    if (requests != null && (Integer)requests >= 8) {
                        break; // We have the expected metrics
                    }
                }
            } catch (Exception e) {
                LOGGER.warn("Attempt {} to get JMX metrics failed: {}", retryCount + 1, e.getMessage());
            }
            retryCount++;
            if (retryCount < maxRetries) {
                Thread.sleep(500);
            }
        }
        
        Assert.assertNotNull("PHOENIX-REST JMX bean should be found", phoenixRestBean);
        
        // Verify request counter is incremented
        Object requests = phoenixRestBean.get("requests");
        Assert.assertNotNull("requests metric should be present", requests);
        Assert.assertTrue("requests metric should be at least 8, but was " + requests, (Integer)requests >= 8);
        
        // Verify various API success metrics exist
        String[] expectedMetrics = {
            "CreateTableSuccessTime", "PutItemSuccessTime", "GetItemSuccessTime",
            "ScanSuccessTime", "QuerySuccessTime", "ListTablesSuccessTime", "DescribeTableSuccessTime"
        };
        
        for (String metricName : expectedMetrics) {
            if (!phoenixRestBean.containsKey(metricName + "_num_ops")) {
                Assert.fail(metricName + " metric should be present");
            }
            Integer metric = (Integer) phoenixRestBean.get(metricName + "_num_ops");
            Assert.assertTrue(metricName + " count should be greater than 0",
                    metric > 0);
        }
    }
    
    private Map<String, Object> getPhoenixRestJMXBean() throws Exception {
        URL url = new URL("http://" + restServer.getServerAddress() + "/jmx");
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        
        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonMap =
                objectMapper.readValue(in, new TypeReference<Map<String, Object>>() {
                });
        
        List<?> beans = (List<?>) jsonMap.get("beans");
        Map<String, Object> phoenixRestBean = null;
        
        for (Map<String, Object> beanObj : (List<Map<String, Object>>) beans) {
            String beanName = (String) beanObj.get("name");
            if (beanName.contains("name=PHOENIX-REST")) {
                phoenixRestBean = beanObj;
                break;
            }
        }
        in.close();
        connection.disconnect();
        return phoenixRestBean;
    }
}
