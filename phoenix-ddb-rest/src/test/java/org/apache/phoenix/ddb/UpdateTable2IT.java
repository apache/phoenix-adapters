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
import org.apache.phoenix.ddb.utils.IndexBuildingActivator;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.mapreduce.index.IndexTool;
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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class UpdateTable2IT {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateTable2IT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        DriverManager.registerDriver(new PhoenixTestDriver());
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

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
    public void testAsyncIndexCreationAndQuery() throws Exception {
        final String tableName = "testTable-123.xyz";
        final String indexName = "gIdx" + tableName;

        CreateTableRequest baseTableRequest = 
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "Subject", ScalarAttributeType.S);
        
        CreateTableRequest ddbTableRequest = 
                DDLTestUtils.addIndexToRequest(true, baseTableRequest, indexName, "Title",
                        ScalarAttributeType.S, null, null);
        
        dynamoDbClient.createTable(ddbTableRequest);
        TestUtils.waitForIndexState(dynamoDbClient, tableName, indexName, "ACTIVE");
        phoenixDBClientV2.createTable(baseTableRequest);

        Map<String, AttributeValue> item1 = getTestItem("Forum1", "Subject1", "Title1");
        Map<String, AttributeValue> item2 = getTestItem("Forum2", "Subject2", "Title1");
        Map<String, AttributeValue> item3 = getTestItem("Forum3", "Subject3", "Title2");

        PutItemRequest putRequest1 = PutItemRequest.builder().tableName(tableName).item(item1).build();
        PutItemRequest putRequest2 = PutItemRequest.builder().tableName(tableName).item(item2).build();
        PutItemRequest putRequest3 = PutItemRequest.builder().tableName(tableName).item(item3).build();

        phoenixDBClientV2.putItem(putRequest1);
        phoenixDBClientV2.putItem(putRequest2);
        phoenixDBClientV2.putItem(putRequest3);
        dynamoDbClient.putItem(putRequest1);
        dynamoDbClient.putItem(putRequest2);
        dynamoDbClient.putItem(putRequest3);

        UpdateTableRequest.Builder utr = UpdateTableRequest.builder().tableName(tableName);
        List<GlobalSecondaryIndexUpdate> indexUpdates = new ArrayList<>();
        CreateGlobalSecondaryIndexAction.Builder createIndexAction =
                CreateGlobalSecondaryIndexAction.builder().indexName(indexName);
        List<KeySchemaElement> idxKeySchemaElements = new ArrayList<>();
        idxKeySchemaElements.add(
                KeySchemaElement.builder().attributeName("Title").keyType(KeyType.HASH).build());
        createIndexAction.keySchema(idxKeySchemaElements);
        createIndexAction.projection(
                Projection.builder().projectionType(ProjectionType.ALL).build());
        createIndexAction.provisionedThroughput(
                ProvisionedThroughput.builder().readCapacityUnits(50L).writeCapacityUnits(50L).build());

        GlobalSecondaryIndexUpdate indexUpdate = GlobalSecondaryIndexUpdate.builder()
                .create(createIndexAction.build()).build();
        indexUpdates.add(indexUpdate);
        utr.globalSecondaryIndexUpdates(indexUpdates);

        List<AttributeDefinition> attrDefs =
                new ArrayList<>(baseTableRequest.attributeDefinitions());
        attrDefs.add(AttributeDefinition.builder().attributeName("Title")
                .attributeType(ScalarAttributeType.S).build());
        utr.attributeDefinitions(attrDefs);

        phoenixDBClientV2.updateTable(utr.build());

        try (Connection connection = DriverManager.getConnection(url)) {
            // background thread activates index to BUILDING state
            IndexBuildingActivator.activateIndexesForBuilding(connection, 0);
            // run MR tool to build index and set state to ACTIVE
            Configuration conf = new Configuration(utility.getConfiguration());
            TestUtils.runIndexTool(conf, false, "DDB",
                    PhoenixUtils.getEscapedArgument(tableName),
                    PhoenixUtils.getEscapedArgument(tableName + "_" + indexName),
                    null, 0, IndexTool.IndexVerifyType.NONE, IndexTool.IndexDisableLoggingType.NONE);
        }

        // make sure Index is active
        TestUtils.waitForIndexState(phoenixDBClientV2, tableName, indexName, "ACTIVE");

        Map<String, AttributeValue> item4 = getTestItem("Forum4", "Subject4", "Title2");
        Map<String, AttributeValue> item5 = getTestItem("Forum5", "Subject5", "Title2");
        Map<String, AttributeValue> item6 = getTestItem("Forum6", "Subject6", "Title1");

        PutItemRequest putRequest4 = PutItemRequest.builder().tableName(tableName).item(item4).build();
        PutItemRequest putRequest5 = PutItemRequest.builder().tableName(tableName).item(item5).build();
        PutItemRequest putRequest6 = PutItemRequest.builder().tableName(tableName).item(item6).build();

        phoenixDBClientV2.putItem(putRequest4);
        phoenixDBClientV2.putItem(putRequest5);
        phoenixDBClientV2.putItem(putRequest6);
        dynamoDbClient.putItem(putRequest4);
        dynamoDbClient.putItem(putRequest5);
        dynamoDbClient.putItem(putRequest6);

        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.indexName(indexName);
        qr.keyConditionExpression("#0 = :v0");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "Title");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("Title1").build());
        qr.expressionAttributeValues(exprAttrVal);

        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());

        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        ItemComparator.areItemsEqual(dynamoResult.items(), phoenixResult.items());

        TestUtils.validateIndexUsed(qr.build(), url);
    }

    private Map<String, AttributeValue> getTestItem(String forumName, String subject, String title) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("ForumName", AttributeValue.builder().s(forumName).build());
        item.put("Subject", AttributeValue.builder().s(subject).build());
        item.put("Title", AttributeValue.builder().s(title).build());
        return item;
    }
}
