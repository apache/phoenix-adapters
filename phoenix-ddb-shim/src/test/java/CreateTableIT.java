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
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.PhoenixRuntime;

import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class CreateTableIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableIT.class);

    private AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static HBaseTestingUtility utility;
    private static String url;

    @BeforeClass
    public static void initialize() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
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
    public void createTableTest() throws Exception {
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableName("TABLE1");
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement("PK1", KeyType.HASH));
        keySchemaElements.add(new KeySchemaElement("PK2", KeyType.RANGE));
        createTableRequest.setKeySchema(keySchemaElements);
        createTableRequest.withAttributeDefinitions(
                new AttributeDefinition("PK1", ScalarAttributeType.B));
        createTableRequest.withAttributeDefinitions(
                new AttributeDefinition("PK2", ScalarAttributeType.S));
        createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(50L, 50L));

        List<KeySchemaElement> idx1KeySchemaElements = new ArrayList<>();
        idx1KeySchemaElements.add(new KeySchemaElement("COL1", KeyType.HASH));
        idx1KeySchemaElements.add(new KeySchemaElement("COL2", KeyType.RANGE));
        createTableRequest.withAttributeDefinitions(
                new AttributeDefinition("COL1", ScalarAttributeType.N));
        createTableRequest.withAttributeDefinitions(
            new AttributeDefinition("COL2", ScalarAttributeType.B));

        List<KeySchemaElement> idx2KeySchemaElements = new ArrayList<>();
        idx2KeySchemaElements.add(new KeySchemaElement("PK1", KeyType.HASH));
        idx2KeySchemaElements.add(new KeySchemaElement("LCOL2", KeyType.RANGE));
        createTableRequest.withAttributeDefinitions(
            new AttributeDefinition("LCOL2", ScalarAttributeType.S));

        List<GlobalSecondaryIndex> globalSecondaryIndexes = new ArrayList<>();
        globalSecondaryIndexes.add(
                new GlobalSecondaryIndex().withIndexName("IDX1").withKeySchema()
                        .withKeySchema(idx1KeySchemaElements)
                        .withProvisionedThroughput(new ProvisionedThroughput(50L, 50L))
                        .withProjection(new Projection().withProjectionType(
                                ProjectionType.ALL)));
        createTableRequest.setGlobalSecondaryIndexes(globalSecondaryIndexes);

        List<LocalSecondaryIndex> localSecondaryIndexes = new ArrayList<>();
        localSecondaryIndexes.add(
            new LocalSecondaryIndex().withIndexName("IDX2").withKeySchema()
                .withKeySchema(idx2KeySchemaElements)
                .withProjection(new Projection().withProjectionType(
                    ProjectionType.ALL)));
        createTableRequest.setLocalSecondaryIndexes(localSecondaryIndexes);

        CreateTableResult createTableResult1 = amazonDynamoDB.createTable(createTableRequest);

        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        CreateTableResult createTableResult2 = phoenixDBClient.createTable(createTableRequest);

        LOGGER.info("Create Table response from DynamoDB: {}",
            JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult1));
        LOGGER.info("Create Table response from Phoenix: {}",
            JacksonUtil.getObjectWriterPretty().writeValueAsString(createTableResult2));

        TableDescription tableDescription1 = createTableResult1.getTableDescription();
        TableDescription tableDescription2 = createTableResult2.getTableDescription();

        Assert.assertEquals(tableDescription1.getTableName(), tableDescription2.getTableName());
        Assert.assertEquals(tableDescription1.getTableStatus(), tableDescription2.getTableStatus());

        List<KeySchemaElement> keySchemaElements1 = tableDescription1.getKeySchema();
        List<KeySchemaElement> keySchemaElements2 = tableDescription2.getKeySchema();

        Assert.assertEquals(keySchemaElements1.size(), keySchemaElements2.size());
        for (int i = 0; i < keySchemaElements1.size(); i++) {
            Assert.assertEquals(keySchemaElements1.get(i).getAttributeName(),
                keySchemaElements2.get(i).getAttributeName());
            Assert.assertEquals(keySchemaElements1.get(i).getKeyType(),
                keySchemaElements2.get(i).getKeyType());
        }

        List<AttributeDefinition> attributeDefinitions1 =
            tableDescription1.getAttributeDefinitions();
        List<AttributeDefinition> attributeDefinitions2 =
            tableDescription2.getAttributeDefinitions();

        Assert.assertEquals(attributeDefinitions1.size(), attributeDefinitions2.size());
        for (int i = 0; i < attributeDefinitions1.size(); i++) {
            Assert.assertEquals(attributeDefinitions1.get(i).getAttributeName(),
                attributeDefinitions2.get(i).getAttributeName());
            Assert.assertEquals(attributeDefinitions1.get(i).getAttributeType(),
                attributeDefinitions2.get(i).getAttributeType());
        }

        Assert.assertEquals(tableDescription1.getGlobalSecondaryIndexes().size(),
            tableDescription2.getGlobalSecondaryIndexes().size());
        Assert.assertEquals(tableDescription1.getLocalSecondaryIndexes().size(),
            tableDescription2.getLocalSecondaryIndexes().size());

        List<GlobalSecondaryIndexDescription> indexDescriptions1 =
            tableDescription1.getGlobalSecondaryIndexes();
        List<GlobalSecondaryIndexDescription> indexDescriptions2 =
            tableDescription2.getGlobalSecondaryIndexes();

        for (int i = 0; i < indexDescriptions1.size(); i++) {
            Assert.assertEquals(indexDescriptions1.get(i).getIndexName(),
                indexDescriptions2.get(i).getIndexName());
            Assert.assertEquals(indexDescriptions1.get(i).getIndexStatus(),
                indexDescriptions2.get(i).getIndexStatus());
            Assert.assertEquals(indexDescriptions1.get(i).getKeySchema().size(),
                indexDescriptions2.get(i).getKeySchema().size());
            for (int j = 0; j < indexDescriptions1.get(i).getKeySchema().size(); j++) {
                Assert.assertEquals(
                    indexDescriptions1.get(i).getKeySchema().get(j).getAttributeName(),
                    indexDescriptions2.get(i).getKeySchema().get(j).getAttributeName());
                Assert.assertEquals(
                    indexDescriptions1.get(i).getKeySchema().get(j).getKeyType(),
                    indexDescriptions2.get(i).getKeySchema().get(j).getKeyType());
            }
        }

        List<LocalSecondaryIndexDescription> localIndexDescriptions1 =
            tableDescription1.getLocalSecondaryIndexes();
        List<LocalSecondaryIndexDescription> localIndexDescriptions2 =
            tableDescription2.getLocalSecondaryIndexes();

        for (int i = 0; i < localIndexDescriptions1.size(); i++) {
            Assert.assertEquals(localIndexDescriptions1.get(i).getIndexName(),
                localIndexDescriptions2.get(i).getIndexName());
            Assert.assertEquals(localIndexDescriptions1.get(i).getKeySchema().size(),
                localIndexDescriptions2.get(i).getKeySchema().size());
            for (int j = 0; j < localIndexDescriptions1.get(i).getKeySchema().size(); j++) {
                Assert.assertEquals(
                    localIndexDescriptions1.get(i).getKeySchema().get(j).getAttributeName(),
                    localIndexDescriptions2.get(i).getKeySchema().get(j).getAttributeName());
                Assert.assertEquals(
                    localIndexDescriptions1.get(i).getKeySchema().get(j).getKeyType(),
                    localIndexDescriptions2.get(i).getKeySchema().get(j).getKeyType());
            }
        }

        Thread.sleep(1000);

        Assert.assertTrue("DDB table should have been created before Phoenix table",
            tableDescription1.getCreationDateTime()
                .before(tableDescription2.getCreationDateTime()));
        Assert.assertTrue(
            tableDescription2.getCreationDateTime()
                .before(new Date(System.currentTimeMillis())));

        Assert.assertNull(tableDescription2.getTableArn());
    }

}
