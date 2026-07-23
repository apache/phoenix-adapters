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
import java.util.Map;

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
import software.amazon.awssdk.services.dynamodb.model.PointInTimeRecoverySpecification;
import software.amazon.awssdk.services.dynamodb.model.ResourceNotFoundException;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateContinuousBackupsResponse;

import org.apache.hadoop.conf.Configuration;
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

/**
 * Integration tests for UpdateContinuousBackups API.
 */
public class UpdateContinuousBackupsIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateContinuousBackupsIT.class);

    private static final String NON_EXISTENT_TABLE_ERROR =
            "Cannot do operations on a non-existent table (Service: DynamoDb, Status Code: 400,";

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
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        Map<String, String> props = Maps.newHashMapWithExpectedSize(1);
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
    public void testUpdateContinuousBackupsReturnsDisabled() throws Exception {
        final String tableName = testName.getMethodName();

        // Create table first
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey", ScalarAttributeType.S,
                        null, null);
        phoenixDBClientV2.createTable(createTableRequest);

        // Test UpdateContinuousBackups on Phoenix
        UpdateContinuousBackupsRequest updateContinuousBackupsRequest =
                UpdateContinuousBackupsRequest.builder()
                        .tableName(tableName)
                        .pointInTimeRecoverySpecification(
                                PointInTimeRecoverySpecification.builder()
                                        .pointInTimeRecoveryEnabled(true)
                                        .build())
                        .build();

        UpdateContinuousBackupsResponse phoenixResponse =
                phoenixDBClientV2.updateContinuousBackups(updateContinuousBackupsRequest);

        // Verify response structure and values
        Assert.assertNotNull("Response should not be null", phoenixResponse);
        Assert.assertNotNull("ContinuousBackupsDescription should not be null",
                phoenixResponse.continuousBackupsDescription());

        // Verify continuous backups status is DISABLED (no-op stub)
        Assert.assertEquals("ContinuousBackupsStatus should be DISABLED", "DISABLED",
                phoenixResponse.continuousBackupsDescription().continuousBackupsStatusAsString());

        // Verify point in time recovery is also disabled (no-op stub)
        Assert.assertNotNull("PointInTimeRecoveryDescription should not be null",
                phoenixResponse.continuousBackupsDescription().pointInTimeRecoveryDescription());
        Assert.assertEquals("PointInTimeRecoveryStatus should be DISABLED", "DISABLED",
                phoenixResponse.continuousBackupsDescription().pointInTimeRecoveryDescription()
                        .pointInTimeRecoveryStatusAsString());
    }

    @Test(timeout = 120000)
    public void testUpdateContinuousBackupsWithNonExistentTable() throws Exception {
        final String nonExistentTableName = "non_existent_table_" + System.currentTimeMillis();

        // Test UpdateContinuousBackups on non-existent table
        UpdateContinuousBackupsRequest updateContinuousBackupsRequest =
                UpdateContinuousBackupsRequest.builder()
                        .tableName(nonExistentTableName)
                        .pointInTimeRecoverySpecification(
                                PointInTimeRecoverySpecification.builder()
                                        .pointInTimeRecoveryEnabled(true)
                                        .build())
                        .build();

        try {
            UpdateContinuousBackupsResponse phoenixResponse =
                    phoenixDBClientV2.updateContinuousBackups(updateContinuousBackupsRequest);

            // Should not reach here - expecting an exception for non-existent table
            Assert.fail("Expected exception for non-existent table, but got response: "
                    + phoenixResponse);

        } catch (ResourceNotFoundException e) {
            Assert.assertTrue(e.getMessage().startsWith(NON_EXISTENT_TABLE_ERROR));
        }
    }
}
