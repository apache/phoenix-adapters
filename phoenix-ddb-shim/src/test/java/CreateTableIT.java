/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.PhoenixRuntime;

import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests for CreateTable API. Brings up local DynamoDB server and HBase miniCluster, and tests
 * CreateTable API with same request against both DDB and HBase/Phoenix servers and
 * compares the response.
 */
public class CreateTableIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableIT.class);

    private final AmazonDynamoDB amazonDynamoDB =
        LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void initialize() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        HBaseTestingUtility utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
    }

    @AfterClass
    public static void stopLocalDynamoDb() {
        LocalDynamoDbTestBase.localDynamoDb().stop();
    }

    @Test
    public void createTableTest1() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
            DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        // add global index
        DDLTestUtils.addIndexToRequest(true, createTableRequest, "IDX1_" + tableName, "COL1",
            ScalarAttributeType.N, "COL2", ScalarAttributeType.B);

        // add local index
        DDLTestUtils.addIndexToRequest(false, createTableRequest, "IDX2_" + tableName, "PK1",
            ScalarAttributeType.B, "LCOL2", ScalarAttributeType.S);

        CreateTableResult createTableResult1 = amazonDynamoDB.createTable(createTableRequest);

        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        CreateTableResult createTableResult2 = phoenixDBClient.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}",
            JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult1));
        LOGGER.info("Create Table response from Phoenix: {}",
            JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult2));

        TableDescription tableDescription1 = createTableResult1.getTableDescription();
        TableDescription tableDescription2 = createTableResult2.getTableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);
    }

    @Test
    public void createTableTest2() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();

        // create table request
        CreateTableRequest createTableRequest =
            DDLTestUtils.getCreateTableRequest(tableName, "HASH_KEY", ScalarAttributeType.S,
                null, null);

        // add global index
        DDLTestUtils.addIndexToRequest(true, createTableRequest, "G_IDX_" + tableName, "idx_key1",
            ScalarAttributeType.B, null, null);

        CreateTableResult createTableResult1 = amazonDynamoDB.createTable(createTableRequest);

        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        CreateTableResult createTableResult2 = phoenixDBClient.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}",
            JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult1));
        LOGGER.info("Create Table response from Phoenix: {}",
            JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult2));

        TableDescription tableDescription1 = createTableResult1.getTableDescription();
        TableDescription tableDescription2 = createTableResult2.getTableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);
    }

    @Test
    public void createTableTest3() throws Exception {
        // create table request
        CreateTableRequest createTableRequest =
            DDLTestUtils.getCreateTableRequest(testName.getMethodName().toUpperCase(), "PK1",
                ScalarAttributeType.B, "SORT_KEY", ScalarAttributeType.N);

        // add local index
        DDLTestUtils.addIndexToRequest(false, createTableRequest, "L_IDX", "PK1",
                ScalarAttributeType.B, "LCOL2", ScalarAttributeType.B);

        CreateTableResult createTableResult1 = amazonDynamoDB.createTable(createTableRequest);

        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        CreateTableResult createTableResult2 = phoenixDBClient.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}",
            JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult1));
        LOGGER.info("Create Table response from Phoenix: {}",
            JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult2));

        TableDescription tableDescription1 = createTableResult1.getTableDescription();
        TableDescription tableDescription2 = createTableResult2.getTableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);
    }


    @Test
    public void createTableTest4() throws Exception {
        String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "aBc_DeF",
                        ScalarAttributeType.B, "xYzwQt", ScalarAttributeType.N);


        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        try (Connection connection = DriverManager.getConnection(url)) {
            List<PColumn> pkCols = PhoenixUtils.getPKColumns(connection, tableName);
            Assert.assertEquals(2, pkCols.size());
            Assert.assertEquals("aBc_DeF", pkCols.get(0).getName().getString());
            Assert.assertEquals("xYzwQt", pkCols.get(1).getName().getString());
        }
    }

    @Test
    public void createTableTest5() throws Exception {
        String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "lowercase",
                        ScalarAttributeType.B, "UPPERCASE", ScalarAttributeType.N);


        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);
        try (Connection connection = DriverManager.getConnection(url)) {
            List<PColumn> pkCols = PhoenixUtils.getPKColumns(connection, tableName);
            Assert.assertEquals(2, pkCols.size());
            Assert.assertEquals("lowercase", pkCols.get(0).getName().getString());
            Assert.assertEquals("UPPERCASE", pkCols.get(1).getName().getString());
        }
    }
}
