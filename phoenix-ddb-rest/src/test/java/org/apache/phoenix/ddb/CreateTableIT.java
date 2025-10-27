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
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ResourceInUseException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.exception.PhoenixIOException;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests for CreateTable API. Brings up local DynamoDB server and HBase miniCluster, and tests
 * CreateTable API with same request against both DDB and HBase/Phoenix servers and
 * compares the response.
 */
public class CreateTableIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableIT.class);

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
        validateRestServerInitFailure();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
    }

    private static void validateRestServerInitFailure() throws Exception {
        try {
            restServer = new RESTServer(new Configuration());
            restServer.run();
            throw new RuntimeException("validation should fail");
        } catch (PhoenixIOException e) {
            // expected
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
    public void createTableTest1() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.B, "PK2",
                        ScalarAttributeType.S);

        // add global index
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, "IDX1_" + tableName,
                        "COL1", ScalarAttributeType.N, "COL2", ScalarAttributeType.B);

        // add local index
        createTableRequest =
                DDLTestUtils.addIndexToRequest(false, createTableRequest, "IDX2_" + tableName,
                        "PK1", ScalarAttributeType.B, "LCOL2", ScalarAttributeType.S);

        CreateTableResponse CreateTableResponse1 = dynamoDbClient.createTable(createTableRequest);

        CreateTableResponse CreateTableResponse2 =
                phoenixDBClientV2.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}", CreateTableResponse1.toString());
        LOGGER.info("Create Table response from Phoenix: {}", CreateTableResponse2.toString());

        TableDescription tableDescription1 = CreateTableResponse1.tableDescription();
        TableDescription tableDescription2 = CreateTableResponse2.tableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);
        TestUtils.validateTableProps(url, tableName, true);
        TestUtils.validateTableProps(url, tableName + "_IDX1_" + tableName, true);
        TestUtils.validateTableProps(url, tableName + "_IDX2_" + tableName, true);
    }

    @Test(timeout = 120000)
    public void createTableTest2() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();

        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "HASH_KEY", ScalarAttributeType.S,
                        null, null);

        // add global index
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName,
                        "idx_key1", ScalarAttributeType.B, null, null);

        CreateTableResponse CreateTableResponse1 = dynamoDbClient.createTable(createTableRequest);
        CreateTableResponse CreateTableResponse2 =
                phoenixDBClientV2.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}", CreateTableResponse1.toString());
        LOGGER.info("Create Table response from Phoenix: {}", CreateTableResponse2.toString());

        TableDescription tableDescription1 = CreateTableResponse1.tableDescription();
        TableDescription tableDescription2 = CreateTableResponse2.tableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);
    }

    @Test(timeout = 120000)
    public void createTableTest3() throws Exception {
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(testName.getMethodName().toUpperCase(), "PK1",
                        ScalarAttributeType.B, "SORT_KEY", ScalarAttributeType.N);

        // add local index
        createTableRequest =
                DDLTestUtils.addIndexToRequest(false, createTableRequest, "L_IDX", "PK1",
                        ScalarAttributeType.B, "LCOL2", ScalarAttributeType.B);

        CreateTableResponse CreateTableResponse1 = dynamoDbClient.createTable(createTableRequest);

        CreateTableResponse CreateTableResponse2 =
                phoenixDBClientV2.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}", CreateTableResponse1.toString());
        LOGGER.info("Create Table response from Phoenix: {}", CreateTableResponse2.toString());

        TableDescription tableDescription1 = CreateTableResponse1.tableDescription();
        TableDescription tableDescription2 = CreateTableResponse2.tableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);
    }

    @Test(timeout = 120000)
    public void createTableTest4() throws Exception {
        String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "aBc_DeF", ScalarAttributeType.B,
                        "xYzwQt", ScalarAttributeType.N);

        phoenixDBClientV2.createTable(createTableRequest);
        try (Connection connection = DriverManager.getConnection(url)) {
            List<PColumn> pkCols = PhoenixUtils.getPKColumns(connection, tableName);
            Assert.assertEquals(2, pkCols.size());
            Assert.assertEquals("aBc_DeF", pkCols.get(0).getName().getString());
            Assert.assertEquals("xYzwQt", pkCols.get(1).getName().getString());
        }
    }

    @Test(timeout = 120000)
    public void createTableTest5() throws Exception {
        String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "lowercase", ScalarAttributeType.B,
                        "UPPERCASE", ScalarAttributeType.N);

        phoenixDBClientV2.createTable(createTableRequest);
        try (Connection connection = DriverManager.getConnection(url)) {
            List<PColumn> pkCols = PhoenixUtils.getPKColumns(connection, tableName);
            Assert.assertEquals(2, pkCols.size());
            Assert.assertEquals("lowercase", pkCols.get(0).getName().getString());
            Assert.assertEquals("UPPERCASE", pkCols.get(1).getName().getString());
        }
    }

    @Test(timeout = 120000)
    public void createTableTestCaseSensitiveNames() throws Exception {
        final String tableName = testName.getMethodName();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        // add global index
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, "idx1_" + tableName, "COL1",
                ScalarAttributeType.N, "COL2", ScalarAttributeType.B);

        // add local index
        createTableRequest = DDLTestUtils.addIndexToRequest(false, createTableRequest, "idx2_" + tableName, "PK1",
                ScalarAttributeType.B, "LCOL2", ScalarAttributeType.S);

        CreateTableResponse CreateTableResponse1 = dynamoDbClient.createTable(createTableRequest);
        CreateTableResponse CreateTableResponse2 = phoenixDBClientV2.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}", CreateTableResponse1.toString());
        LOGGER.info("Create Table response from Phoenix: {}", CreateTableResponse2.toString());

        TableDescription tableDescription1 = CreateTableResponse1.tableDescription();
        TableDescription tableDescription2 = CreateTableResponse2.tableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);
    }

    @Test(timeout = 120000)
    public void createTableWithStreamTest() throws Exception {
        String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey", ScalarAttributeType.B,
                        "sortKey", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_IMAGE");
        createTableRequest =
                DDLTestUtils.addIndexToRequest(true, createTableRequest, "IDX1_" + tableName,
                        "COL1", ScalarAttributeType.N, "COL2", ScalarAttributeType.B);

        CreateTableResponse CreateTableResponse1 = dynamoDbClient.createTable(createTableRequest);
        CreateTableResponse CreateTableResponse2 =
                phoenixDBClientV2.createTable(createTableRequest);
        TableDescription tableDescription1 = CreateTableResponse1.tableDescription();
        TableDescription tableDescription2 = CreateTableResponse2.tableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);

        try (Connection connection = DriverManager.getConnection(url)) {
            DDLTestUtils.assertCDCMetadata(connection.unwrap(PhoenixConnection.class),
                    tableDescription2, "NEW_IMAGE");
        }
        TestUtils.validateTableProps(url, tableName, false);
        TestUtils.validateTableProps(url, tableName + "_IDX1_" + tableName, true);
    }

    @Test(timeout = 120000)
    public void createTableTest() throws Exception {
        createTable(dynamoDbClient);
        createTable(phoenixDBClientV2);
        Thread.sleep(1000);
        testJmxMetrics();
    }

    @Test(timeout = 120000)
    public void createTableDuplicateFails() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();

        CreateTableRequest request =
                DDLTestUtils.getCreateTableRequest(tableName, "HK", ScalarAttributeType.B, null,
                        null);

        dynamoDbClient.createTable(request);
        phoenixDBClientV2.createTable(request);

        try {
            phoenixDBClientV2.createTable(request);
            // Should succeed within 5 seconds
        } catch (ResourceInUseException e) {
            Assert.fail("Should not throw ResourceInUseException within 5 seconds"
                    + " of table creation");
        }

        Thread.sleep(6000);

        try {
            dynamoDbClient.createTable(request);
            Assert.fail("Expected ResourceInUseException from DynamoDB after 5 seconds");
        } catch (ResourceInUseException e) {
            Assert.assertEquals(400, e.statusCode());
        }

        try {
            phoenixDBClientV2.createTable(request);
            Assert.fail("Expected ResourceInUseException from Phoenix REST after 5 seconds");
        } catch (ResourceInUseException e) {
            Assert.assertEquals(400, e.statusCode());
        }
    }

    private static void testJmxMetrics() throws Exception {
        URL url = new URL("http://" + restServer.getServerAddress()
                + "/jmx?get=Hadoop:service=*,name=PHOENIX-REST::CreateTableSuccessTime_num_ops");

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        int responseCode = connection.getResponseCode();
        Assert.assertEquals(HttpURLConnection.HTTP_OK, responseCode);

        BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, Object> jsonMap =
                objectMapper.readValue(in, new TypeReference<Map<String, Object>>() {
                });
        Assert.assertTrue(
                (Integer) ((Map<String, Object>) ((List) jsonMap.get("beans")).get(0)).get(
                        "CreateTableSuccessTime_num_ops") > 0);
        in.close();
        connection.disconnect();
    }

    private static void createTable(DynamoDbClient db) {
        DescribeTableRequest request =
                DescribeTableRequest.builder().tableName("Weo495bfl-TaBLe_9_9_9_._.-_.-").build();
        try {
            db.describeTable(request);
        } catch (ResourceNotFoundException e) {
            // Ignore ResourceNotFoundException
        }

        CreateTableRequest createTableRequest =
                CreateTableRequest.builder().tableName("Weo495bfl-TaBLe_9_9_9_._.-_.-")
                        .billingMode(BillingMode.PAY_PER_REQUEST).globalSecondaryIndexes(
                                Arrays.asList(GlobalSecondaryIndex.builder().indexName("outstanding_tasks")
                                        .keySchema(Arrays.asList(KeySchemaElement.builder()
                                                        .attributeName("outstanding_tasks_hk").keyType(KeyType.HASH)
                                                        .build(),
                                                KeySchemaElement.builder().attributeName("execute_after")
                                                        .keyType(KeyType.RANGE).build()

                                        )).projection(
                                                Projection.builder().projectionType(ProjectionType.ALL)
                                                        .build()).build()

                                )).attributeDefinitions(Arrays.asList(
                                AttributeDefinition.builder().attributeName("hk")
                                        .attributeType(ScalarAttributeType.B).build(),
                                AttributeDefinition.builder().attributeName("sk")
                                        .attributeType(ScalarAttributeType.B).build(),
                                AttributeDefinition.builder().attributeName("outstanding_tasks_hk")
                                        .attributeType(ScalarAttributeType.B).build(),
                                AttributeDefinition.builder().attributeName("execute_after")
                                        .attributeType(ScalarAttributeType.B).build()

                        )).keySchema(Arrays.asList(
                                KeySchemaElement.builder().attributeName("hk").keyType(KeyType.HASH)
                                        .build(),
                                KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE)
                                        .build()

                        )).build();
        db.createTable(createTableRequest);
    }
}
