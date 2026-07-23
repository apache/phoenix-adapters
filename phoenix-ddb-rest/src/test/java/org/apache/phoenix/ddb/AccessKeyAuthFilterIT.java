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

import java.net.URI;
import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.ddb.rest.auth.CredentialStore;
import org.apache.phoenix.ddb.rest.auth.UserCredentials;
import org.apache.phoenix.ddb.rest.util.Constants;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Integration test for AccessKeyAuthFilter functionality.
 * Tests createTable operations with valid and invalid access keys.
 */
public class AccessKeyAuthFilterIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(AccessKeyAuthFilterIT.class);

    private static RESTServer restServer;
    private static String serverAddress;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    public static class TestCredentialStore implements CredentialStore {
        private final Map<String, UserCredentials> credentials = new HashMap<>();

        public TestCredentialStore() {
            credentials.put("dummykey", new UserCredentials("testuser", "dummykey", "secret123"));
            credentials.put("AKIAIOSFODNN7EXAMPLE",
                    new UserCredentials("john.doe", "AKIAIOSFODNN7EXAMPLE", "secretkey"));
        }

        @Override
        public UserCredentials getCredentials(String accessKeyId) {
            return credentials.get(accessKeyId);
        }
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);
        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        String url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        conf.set(Constants.AUTH_CREDENTIAL_STORE_CLASS, TestCredentialStore.class.getName());

        TestUtils.awaitPhoenixReady(url);

        restServer = new RESTServer(conf);
        restServer.run();
        serverAddress = restServer.getServerAddress();

        LOGGER.info("REST Server started with authentication at: {}", serverAddress);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
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
    public void testCreateTableWithValidAccessKey() throws Exception {
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://" + serverAddress))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("dummykey", "secret123")))
                .build();

        CreateTableRequest createRequest = CreateTableRequest.builder()
                .tableName("TestTable1")
                .keySchema(KeySchemaElement.builder()
                        .attributeName("id")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
                .build();

        client.createTable(createRequest);
        LOGGER.info("CreateTable succeeded with valid access key");
    }

    @Test
    public void testCreateTableWithInvalidAccessKey() throws Exception {
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://" + serverAddress))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("invalidkey", "secret123")))
                .build();

        CreateTableRequest createRequest = CreateTableRequest.builder()
                .tableName("TestTable2")
                .keySchema(KeySchemaElement.builder()
                        .attributeName("id")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
                .build();

        try {
            client.createTable(createRequest);
            Assert.fail("CreateTable should fail with invalid access key");
        } catch (DynamoDbException e) {
            LOGGER.info("CreateTable correctly failed with invalid access key: {}", e.getMessage());
            Assert.assertEquals(403, e.statusCode());
        }
    }

    @Test
    public void testCreateTableWithAnotherValidAccessKey() throws Exception {
        DynamoDbClient client = DynamoDbClient.builder()
                .endpointOverride(URI.create("http://" + serverAddress))
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("AKIAIOSFODNN7EXAMPLE", "secretkey")))
                .build();

        CreateTableRequest createRequest = CreateTableRequest.builder()
                .tableName("TestTable3")
                .keySchema(KeySchemaElement.builder()
                        .attributeName("id")
                        .keyType(KeyType.HASH)
                        .build())
                .attributeDefinitions(AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build())
                .provisionedThroughput(ProvisionedThroughput.builder()
                        .readCapacityUnits(5L)
                        .writeCapacityUnits(5L)
                        .build())
                .build();

        try {
            client.createTable(createRequest);
            LOGGER.info("CreateTable succeeded with another valid access key");
        } catch (Exception e) {
            LOGGER.error("CreateTable failed with valid access key", e);
            Assert.fail("CreateTable should succeed with valid access key: " + e.getMessage());
        } finally {
            client.close();
        }
    }
} 