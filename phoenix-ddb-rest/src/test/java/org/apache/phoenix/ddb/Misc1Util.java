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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TimeToLiveSpecification;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

public class Misc1Util {

    private static final Logger LOGGER = LoggerFactory.getLogger(Misc1Util.class);

    public static void test1(DynamoDbClient dynamoDbClient, DynamoDbClient phoenixDBClientV2,
            DynamoDbStreamsClient dynamoDbStreamsClient,
            DynamoDbStreamsClient phoenixDBStreamsClientV2) throws InterruptedException {
        String leaseTableName = "test.dataLease--12_99";
        String objectsTableName = "test.cloudX9_";

        CreateTableRequest leaseTableRequest =
                CreateTableRequest.builder().tableName(leaseTableName).attributeDefinitions(
                        AttributeDefinition.builder().attributeName("shardKey")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("owner")
                                .attributeType(ScalarAttributeType.S).build()).keySchema(
                        KeySchemaElement.builder().attributeName("shardKey").keyType(KeyType.HASH)
                                .build()).globalSecondaryIndexes(
                        GlobalSecondaryIndex.builder().indexName("ownerIndex").keySchema(
                                        KeySchemaElement.builder().attributeName("owner")
                                                .keyType(KeyType.HASH).build()).projection(
                                        Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build()).billingMode(BillingMode.PAY_PER_REQUEST)
                        .streamSpecification(StreamSpecification.builder().streamEnabled(true)
                .streamViewType("NEW_AND_OLD_IMAGES").build()).build();

        dynamoDbClient.createTable(leaseTableRequest);
        phoenixDBClientV2.createTable(leaseTableRequest);

        CreateTableRequest objectsTableRequest =
                CreateTableRequest.builder().tableName(objectsTableName).attributeDefinitions(
                                AttributeDefinition.builder().attributeName("0")
                                        .attributeType(ScalarAttributeType.B).build(),
                                AttributeDefinition.builder().attributeName("1")
                                        .attributeType(ScalarAttributeType.B).build()).keySchema(
                                KeySchemaElement.builder().attributeName("0").keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("1").keyType(KeyType.RANGE)
                                        .build()).billingMode(BillingMode.PAY_PER_REQUEST)
                        .streamSpecification(StreamSpecification.builder().streamEnabled(true)
                                .streamViewType("NEW_AND_OLD_IMAGES").build()).build();

        dynamoDbClient.createTable(objectsTableRequest);
        phoenixDBClientV2.createTable(objectsTableRequest);

        UpdateTimeToLiveRequest enableTtlRequest =
                UpdateTimeToLiveRequest.builder().tableName(objectsTableName)
                        .timeToLiveSpecification(
                                TimeToLiveSpecification.builder().attributeName("#").enabled(true)
                                        .build()).build();

        UpdateTimeToLiveResponse dynamoUpdateTtlResponse =
                dynamoDbClient.updateTimeToLive(enableTtlRequest);
        UpdateTimeToLiveResponse phoenixUpdateTtlResponse =
                phoenixDBClientV2.updateTimeToLive(enableTtlRequest);

        Assert.assertEquals("UpdateTimeToLive responses should match",
                dynamoUpdateTtlResponse.timeToLiveSpecification(),
                phoenixUpdateTtlResponse.timeToLiveSpecification());

        String migrationKey = "Migration_test.cloudX9_/2025-06-19T20:19:58.767";
        GetItemRequest migrationGetRequest = GetItemRequest.builder().tableName(leaseTableName)
                .key(Collections.singletonMap("shardKey",
                        AttributeValue.builder().s(migrationKey).build())).consistentRead(true)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        GetItemResponse dynamoMigrationGet = dynamoDbClient.getItem(migrationGetRequest);
        GetItemResponse phoenixMigrationGet = phoenixDBClientV2.getItem(migrationGetRequest);

        Assert.assertEquals("Migration GetItem responses should match", dynamoMigrationGet.item(),
                phoenixMigrationGet.item());

        PutItemRequest migrationPutRequest = PutItemRequest.builder().tableName(leaseTableName)
                .item(Collections.singletonMap("shardKey",
                        AttributeValue.builder().s(migrationKey).build()))
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        dynamoDbClient.putItem(migrationPutRequest);
        phoenixDBClientV2.putItem(migrationPutRequest);

        String leaseKey =
                "test.cloudX9_/2025-06-19T20:19:58.767/d0dba5c1e3e646d04bff003bbdefde0e";
        Map<String, AttributeValue> leaseItem = new HashMap<>();
        leaseItem.put("shardKey", AttributeValue.builder().s(leaseKey).build());
        leaseItem.put("owner", AttributeValue.builder().s("localhost:4999").build());
        leaseItem.put("createdTime", AttributeValue.builder().n("1750364402944").build());

        PutItemRequest leasePutRequest =
                PutItemRequest.builder().tableName(leaseTableName).item(leaseItem)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .conditionExpression("attribute_not_exists(#0)")
                        .expressionAttributeNames(Collections.singletonMap("#0", "shardKey"))
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        dynamoDbClient.putItem(leasePutRequest);
        phoenixDBClientV2.putItem(leasePutRequest);

        Map<String, AttributeValue> objectItem = new HashMap<>();
        objectItem.put("0", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(Base64.getDecoder().decode("AAEAIA==")))
                .build());
        objectItem.put("1", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(Base64.getDecoder().decode("Zm9vAAE=")))
                .build());
        objectItem.put("B", AttributeValue.builder().n("42").build());

        Map<String, AttributeValue> nestedC = new HashMap<>();
        nestedC.put("C", AttributeValue.builder().n("42").build());
        Map<String, AttributeValue> nestedE = new HashMap<>();
        nestedE.put("D", AttributeValue.builder().n("42").build());
        nestedC.put("E", AttributeValue.builder()
                .m(Collections.singletonMap("YmF6AAE", AttributeValue.builder().m(nestedE).build()))
                .build());

        objectItem.put("C", AttributeValue.builder()
                .m(Collections.singletonMap("YmFyAAE", AttributeValue.builder().m(nestedC).build()))
                .build());
        objectItem.put("-", AttributeValue.builder().n("1").build());

        Map<String, String> objectAttrNames = new HashMap<>();
        objectAttrNames.put("#0", "0");
        objectAttrNames.put("#1", "1");
        objectAttrNames.put("#2", "%");
        objectAttrNames.put("#3", "-");

        PutItemRequest objectPutRequest =
                PutItemRequest.builder().tableName(objectsTableName).item(objectItem)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).conditionExpression(
                                "((attribute_not_exists(#0) AND attribute_not_exists(#1)) OR "
                                        + "attribute_exists(#2)) AND (((attribute_not_exists(#0)"
                                        + " AND attribute_not_exists(#1)) OR attribute_exists(#2))"
                                        + " OR (#3 = :0))")
                        .expressionAttributeNames(objectAttrNames).expressionAttributeValues(
                                Collections.singletonMap(":0", AttributeValue.builder().n("1").build()))
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        dynamoDbClient.putItem(objectPutRequest);
        phoenixDBClientV2.putItem(objectPutRequest);

        Map<String, AttributeValue> objectKey = new HashMap<>();
        objectKey.put("0", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(Base64.getDecoder().decode("AAEAIA==")))
                .build());
        objectKey.put("1", AttributeValue.builder()
                .b(SdkBytes.fromByteArray(Base64.getDecoder().decode("Zm9vAAE=")))
                .build());

        GetItemRequest objectGetRequest =
                GetItemRequest.builder().tableName(objectsTableName).key(objectKey)
                        .consistentRead(true).returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .build();

        GetItemResponse dynamoObjectGet = dynamoDbClient.getItem(objectGetRequest);
        GetItemResponse phoenixObjectGet = phoenixDBClientV2.getItem(objectGetRequest);

        Assert.assertTrue("Object GetItem responses should be equal",
                ItemComparator.areItemsEqual(dynamoObjectGet.item(), phoenixObjectGet.item()));

        Map<String, String> updateAttrNames = new HashMap<>();
        updateAttrNames.put("#0", "0");
        updateAttrNames.put("#1", "1");
        updateAttrNames.put("#2", "%");
        updateAttrNames.put("#3", "-");
        updateAttrNames.put("#4", "^");
        updateAttrNames.put("#5", ">");
        updateAttrNames.put("#6", "B");
        updateAttrNames.put("#7", "C");

        Map<String, AttributeValue> updateAttrValues = new HashMap<>();
        updateAttrValues.put(":0", AttributeValue.builder().n("1").build());
        updateAttrValues.put(":1", AttributeValue.builder().n("42").build());
        Map<String, AttributeValue> newC = new HashMap<>();
        newC.put("C", AttributeValue.builder().n("42").build());
        newC.put("E", AttributeValue.builder().m(Collections.emptyMap()).build());
        updateAttrValues.put(":2", AttributeValue.builder()
                .m(Collections.singletonMap("YmFyAAE", AttributeValue.builder().m(newC).build()))
                .build());

        UpdateItemRequest objectUpdateRequest =
                UpdateItemRequest.builder().tableName(objectsTableName).key(objectKey)
                        .returnValues("ALL_NEW")
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("SET #6 = :1, #7 = :2 REMOVE #4, #5").conditionExpression(
                                "((attribute_exists(#0) AND attribute_exists(#1)) AND "
                                        + "attribute_not_exists(#2)) AND (((attribute_exists(#0) "
                                        + "AND attribute_exists(#1)) AND attribute_not_exists(#2))"
                                        + " AND (((attribute_not_exists(#0) AND "
                                        + "attribute_not_exists(#1)) OR attribute_exists(#2)) "
                                        + "OR (#3 = :0)))")
                        .expressionAttributeNames(updateAttrNames)
                        .expressionAttributeValues(updateAttrValues)
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        UpdateItemResponse dynamoObjectUpdate = dynamoDbClient.updateItem(objectUpdateRequest);
        UpdateItemResponse phoenixObjectUpdate = phoenixDBClientV2.updateItem(objectUpdateRequest);

        Assert.assertTrue("Object UpdateItem responses should be equal",
                ItemComparator.areItemsEqual(dynamoObjectUpdate.attributes(),
                        phoenixObjectUpdate.attributes()));

        Map<String, String> leaseAttrNames = new HashMap<>();
        leaseAttrNames.put("#0", "shardKey");
        leaseAttrNames.put("#1", "owner");
        leaseAttrNames.put("#2", "checkpoint");
        leaseAttrNames.put("#3", "lastCheckpointTime");

        Map<String, AttributeValue> leaseAttrValues = new HashMap<>();
        leaseAttrValues.put(":0", AttributeValue.builder().s("localhost:4999").build());
        leaseAttrValues.put(":1", AttributeValue.builder().s("000175036441063000000").build());
        leaseAttrValues.put(":2", AttributeValue.builder().n("1750364411612").build());

        UpdateItemRequest leaseUpdateRequest = UpdateItemRequest.builder().tableName(leaseTableName)
                .key(Collections.singletonMap("shardKey",
                        AttributeValue.builder().s(leaseKey).build()))
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                .updateExpression("SET #2 = :1, #3 = :2").conditionExpression(
                        "attribute_exists(#0) AND #1 = :0 AND attribute_not_exists(#2)")
                .expressionAttributeNames(leaseAttrNames).expressionAttributeValues(leaseAttrValues)
                .returnValuesOnConditionCheckFailure(ReturnValuesOnConditionCheckFailure.ALL_OLD)
                .build();

        dynamoDbClient.updateItem(leaseUpdateRequest);
        phoenixDBClientV2.updateItem(leaseUpdateRequest);

        GetItemResponse dynamoMigrationGet2 = dynamoDbClient.getItem(migrationGetRequest);
        GetItemResponse phoenixMigrationGet2 = phoenixDBClientV2.getItem(migrationGetRequest);

        Assert.assertEquals("Second migration GetItem responses should match",
                dynamoMigrationGet2.item(), phoenixMigrationGet2.item());

        Map<String, String> releaseAttrNames = new HashMap<>();
        releaseAttrNames.put("#0", "shardKey");
        releaseAttrNames.put("#1", "owner");
        releaseAttrNames.put("#2", "checkpoint");
        releaseAttrNames.put("#3", "lastOwnerChangeTime");

        Map<String, AttributeValue> releaseAttrValues = new HashMap<>();
        releaseAttrValues.put(":0", AttributeValue.builder().s("localhost:4999").build());
        releaseAttrValues.put(":1", AttributeValue.builder().s("000175036441063000000").build());
        releaseAttrValues.put(":2", AttributeValue.builder().n("42").build());

        UpdateItemRequest leaseReleaseRequest =
                UpdateItemRequest.builder().tableName(leaseTableName)
                        .key(Collections.singletonMap("shardKey",
                                AttributeValue.builder().s(leaseKey).build()))
                        .returnValues("ALL_NEW")
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("SET #3 = :2 REMOVE #1")
                        .conditionExpression("attribute_exists(#0) AND #1 = :0 AND #2 = :1")
                        .expressionAttributeNames(releaseAttrNames)
                        .expressionAttributeValues(releaseAttrValues)
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        UpdateItemResponse dynamoReleaseUpdate = dynamoDbClient.updateItem(leaseReleaseRequest);
        UpdateItemResponse phoenixReleaseUpdate = phoenixDBClientV2.updateItem(leaseReleaseRequest);

        Assert.assertTrue("Lease release responses should be equal",
                ItemComparator.areItemsEqual(dynamoReleaseUpdate.attributes(),
                        phoenixReleaseUpdate.attributes()));

        GetItemResponse dynamoMigrationGet3 = dynamoDbClient.getItem(migrationGetRequest);
        GetItemResponse phoenixMigrationGet3 = phoenixDBClientV2.getItem(migrationGetRequest);

        Assert.assertEquals("Third migration GetItem responses should match",
                dynamoMigrationGet3.item(), phoenixMigrationGet3.item());

        Map<String, String> reacquireAttrNames = new HashMap<>();
        reacquireAttrNames.put("#0", "shardKey");
        reacquireAttrNames.put("#1", "owner");
        reacquireAttrNames.put("#2", "checkpoint");
        reacquireAttrNames.put("#3", "lastOwnerChangeTime");

        Map<String, AttributeValue> reacquireAttrValues = new HashMap<>();
        reacquireAttrValues.put(":0", AttributeValue.builder().s("000175036441063000000").build());
        reacquireAttrValues.put(":1", AttributeValue.builder().s("localhost:4999").build());
        reacquireAttrValues.put(":2", AttributeValue.builder().n("1750364462264").build());

        UpdateItemRequest leaseReacquireRequest =
                UpdateItemRequest.builder().tableName(leaseTableName)
                        .key(Collections.singletonMap("shardKey",
                                AttributeValue.builder().s(leaseKey).build()))
                        .returnValues("ALL_NEW")
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("SET #1 = :1, #3 = :2").conditionExpression(
                                "attribute_exists(#0) AND attribute_not_exists(#1) AND #2 = :0")
                        .expressionAttributeNames(reacquireAttrNames)
                        .expressionAttributeValues(reacquireAttrValues)
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        UpdateItemResponse dynamoReacquireUpdate = dynamoDbClient.updateItem(leaseReacquireRequest);
        UpdateItemResponse phoenixReacquireUpdate =
                phoenixDBClientV2.updateItem(leaseReacquireRequest);

        Assert.assertTrue("Lease reacquire responses should be equal",
                ItemComparator.areItemsEqual(dynamoReacquireUpdate.attributes(),
                        phoenixReacquireUpdate.attributes()));

        Map<String, String> finalReleaseAttrNames = new HashMap<>();
        finalReleaseAttrNames.put("#0", "shardKey");
        finalReleaseAttrNames.put("#1", "owner");
        finalReleaseAttrNames.put("#2", "checkpoint");
        finalReleaseAttrNames.put("#3", "lastOwnerChangeTime");

        Map<String, AttributeValue> finalReleaseAttrValues = new HashMap<>();
        finalReleaseAttrValues.put(":0", AttributeValue.builder().s("localhost:4999").build());
        finalReleaseAttrValues.put(":1",
                AttributeValue.builder().s("000175036441063000000").build());
        finalReleaseAttrValues.put(":2", AttributeValue.builder().n("1750364465201").build());

        UpdateItemRequest finalReleaseRequest =
                UpdateItemRequest.builder().tableName(leaseTableName)
                        .key(Collections.singletonMap("shardKey",
                                AttributeValue.builder().s(leaseKey).build()))
                        .returnValues("ALL_NEW")
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("SET #3 = :2 REMOVE #1")
                        .conditionExpression("attribute_exists(#0) AND #1 = :0 AND #2 = :1")
                        .expressionAttributeNames(finalReleaseAttrNames)
                        .expressionAttributeValues(finalReleaseAttrValues)
                        .returnValuesOnConditionCheckFailure(
                                ReturnValuesOnConditionCheckFailure.ALL_OLD).build();

        UpdateItemResponse dynamoFinalUpdate = dynamoDbClient.updateItem(finalReleaseRequest);
        UpdateItemResponse phoenixFinalUpdate = phoenixDBClientV2.updateItem(finalReleaseRequest);

        Assert.assertTrue("Final lease release responses should be equal",
                ItemComparator.areItemsEqual(dynamoFinalUpdate.attributes(),
                        phoenixFinalUpdate.attributes()));

        TestUtils.compareAllStreamRecords(leaseTableName, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);
        TestUtils.compareAllStreamRecords(objectsTableName, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);

        DeleteTableRequest deleteLeaseTable =
                DeleteTableRequest.builder().tableName(leaseTableName).build();
        DeleteTableRequest deleteObjectsTable =
                DeleteTableRequest.builder().tableName(objectsTableName).build();

        dynamoDbClient.deleteTable(deleteLeaseTable);
        phoenixDBClientV2.deleteTable(deleteLeaseTable);
        dynamoDbClient.deleteTable(deleteObjectsTable);
        phoenixDBClientV2.deleteTable(deleteObjectsTable);
    }

    public static void test2(DynamoDbClient dynamoDbClient, DynamoDbClient phoenixDBClientV2,
            DynamoDbStreamsClient dynamoDbStreamsClient,
            DynamoDbStreamsClient phoenixDBStreamsClientV2) throws InterruptedException {
        String tableName = "test..__--1234TesT4321";

        CreateTableRequest createRequest = CreateTableRequest.builder().tableName(tableName)
                .attributeDefinitions(AttributeDefinition.builder().attributeName("workerId")
                        .attributeType(ScalarAttributeType.S).build()).keySchema(
                        KeySchemaElement.builder().attributeName("workerId").keyType(KeyType.HASH)
                                .build()).billingMode(BillingMode.PAY_PER_REQUEST)
                .streamSpecification(StreamSpecification.builder().streamEnabled(true)
                        .streamViewType("NEW_AND_OLD_IMAGES").build()).build();

        dynamoDbClient.createTable(createRequest);
        phoenixDBClientV2.createTable(createRequest);

        ListStreamsRequest listStreamsRequest =
                ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams =
                phoenixDBStreamsClientV2.listStreams(listStreamsRequest);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn =
                dynamoDbStreamsClient.listStreams(listStreamsRequest).streams().get(0).streamArn();

        TestUtils.waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        TestUtils.waitForStream(dynamoDbStreamsClient, dynamoStreamArn);

        ScanRequest scanRequest = ScanRequest.builder().tableName(tableName)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        ScanResponse localScanResponse = dynamoDbClient.scan(scanRequest);
        ScanResponse phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        Assert.assertEquals("Initial scan count should match", localScanResponse.count(),
                phoenixScanResponse.count());
        Assert.assertEquals("Initial scan count 0", new Integer(0), localScanResponse.count());

        Map<String, AttributeValue> item1 = new HashMap<>();
        item1.put("workerId", AttributeValue.builder().s("localhost:4999").build());
        item1.put("lastAliveTime", AttributeValue.builder().n("1750209020569").build());
        item1.put("appStartupTime", AttributeValue.builder().n("1750209013852").build());
        item1.put("expirationTime", AttributeValue.builder().n("1750209140").build());

        PutItemRequest putRequest1 = PutItemRequest.builder().tableName(tableName).item(item1)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        dynamoDbClient.putItem(putRequest1);
        phoenixDBClientV2.putItem(putRequest1);

        localScanResponse = dynamoDbClient.scan(scanRequest);
        phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        compareScanResponsesSortedByWorkerId(localScanResponse, phoenixScanResponse,
                "Scan after first put");

        List<Long> timestamps =
                Arrays.asList(1750209030572L, 1750209040572L, 1750209050574L, 1750209060571L,
                        1750209070574L, 1750209080572L, 1750209090574L, 1750209100574L);

        for (int i = 0; i < timestamps.size(); i++) {
            Long timestamp = timestamps.get(i);

            Map<String, AttributeValue> updatedItem = new HashMap<>();
            updatedItem.put("workerId", AttributeValue.builder().s("localhost:4999").build());
            updatedItem.put("lastAliveTime",
                    AttributeValue.builder().n(timestamp.toString()).build());
            updatedItem.put("appStartupTime", AttributeValue.builder().n("1750209013852").build());
            updatedItem.put("expirationTime",
                    AttributeValue.builder().n(String.valueOf(timestamp + 120000)).build());

            PutItemRequest putRequest =
                    PutItemRequest.builder().tableName(tableName).item(updatedItem)
                            .returnConsumedCapacity(
                                    ReturnConsumedCapacity.TOTAL)
                            .build();

            dynamoDbClient.putItem(putRequest);
            phoenixDBClientV2.putItem(putRequest);

            if (i % 2 == 0) {
                localScanResponse = dynamoDbClient.scan(scanRequest);
                phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

                compareScanResponsesSortedByWorkerId(localScanResponse, phoenixScanResponse,
                        "Scan at iteration " + i);
            }

            if (i % 3 == 0) {
                ScanRequest paginatedScanRequest = ScanRequest.builder().tableName(tableName)
                        .exclusiveStartKey(Collections.singletonMap("workerId",
                                AttributeValue.builder().s("localhost:4999").build()))
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

                ScanResponse ddbScanResponse = dynamoDbClient.scan(paginatedScanRequest);
                ScanResponse phoenixDdbScanResponse = phoenixDBClientV2.scan(paginatedScanRequest);

                compareScanResponsesSortedByWorkerId(ddbScanResponse, phoenixDdbScanResponse,
                        "Paginated scan at iteration " + i);
            }
        }

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 60; i++) {
                Map<String, AttributeValue> initialItem = new HashMap<>();
                initialItem.put("workerId",
                        AttributeValue.builder().s(j == 0 ? "batch:delete:" : "batch:worker:" + i)
                                .build());
                initialItem.put("lastAliveTime",
                        AttributeValue.builder().n(String.valueOf(System.currentTimeMillis()))
                                .build());
                initialItem.put("appStartupTime",
                        AttributeValue.builder().n(String.valueOf(System.currentTimeMillis()))
                                .build());
                initialItem.put("expirationTime", AttributeValue.builder()
                        .n(String.valueOf(System.currentTimeMillis() + 120000)).build());

                PutItemRequest putRequest =
                        PutItemRequest.builder().tableName(tableName).item(initialItem).build();
                dynamoDbClient.putItem(putRequest);
                phoenixDBClientV2.putItem(putRequest);
            }
        }

        Map<String, AttributeValue> item2 = new HashMap<>();
        item2.put("workerId", AttributeValue.builder().s("localhost:33279").build());
        item2.put("lastAliveTime", AttributeValue.builder().n("42").build());
        item2.put("appStartupTime", AttributeValue.builder().n("42").build());
        item2.put("expirationTime", AttributeValue.builder().n("120").build());

        PutItemRequest putRequest2 = PutItemRequest.builder().tableName(tableName).item(item2)
                .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        dynamoDbClient.putItem(putRequest2);
        phoenixDBClientV2.putItem(putRequest2);

        localScanResponse = dynamoDbClient.scan(scanRequest);
        phoenixScanResponse = phoenixDBClientV2.scan(scanRequest);

        compareScanResponsesSortedByWorkerId(localScanResponse, phoenixScanResponse, "Final scan");

        Map<String, AttributeValue> updateItem = new HashMap<>();
        updateItem.put("workerId", AttributeValue.builder().s("localhost:4999").build());
        updateItem.put("lastAliveTime", AttributeValue.builder().n("1636629071000").build());
        updateItem.put("appStartupTime", AttributeValue.builder().n("1636629071000").build());
        updateItem.put("expirationTime", AttributeValue.builder().n("1636629191").build());

        PutItemRequest updatePutRequest =
                PutItemRequest.builder().tableName(tableName).item(updateItem)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL).build();

        dynamoDbClient.putItem(updatePutRequest);
        phoenixDBClientV2.putItem(updatePutRequest);

        Map<String, AttributeValue> updateKey = Collections.singletonMap("workerId",
                AttributeValue.builder().s("localhost:4999").build());

        UpdateItemRequest updateRequest =
                UpdateItemRequest.builder().tableName(tableName).key(updateKey)
                        .returnConsumedCapacity(ReturnConsumedCapacity.TOTAL)
                        .updateExpression("REMOVE #0")
                        .expressionAttributeNames(Collections.singletonMap("#0", "appStartupTime"))
                        .build();

        dynamoDbClient.updateItem(updateRequest);
        phoenixDBClientV2.updateItem(updateRequest);

        GetItemRequest updateGetRequest =
                GetItemRequest.builder().tableName(tableName).key(updateKey).build();

        Map<String, AttributeValue> localUpdatedItem =
                dynamoDbClient.getItem(updateGetRequest).item();
        Map<String, AttributeValue> phoenixUpdatedItem =
                phoenixDBClientV2.getItem(updateGetRequest).item();

        Assert.assertTrue("Items should be equal after REMOVE update",
                ItemComparator.areItemsEqual(localUpdatedItem, phoenixUpdatedItem));
        Assert.assertFalse("appStartupTime should be removed from local item",
                localUpdatedItem.containsKey("appStartupTime"));
        Assert.assertFalse("appStartupTime should be removed from phoenix item",
                phoenixUpdatedItem.containsKey("appStartupTime"));

        DescribeTimeToLiveRequest ttlRequest =
                DescribeTimeToLiveRequest.builder().tableName(tableName).build();

        DescribeTimeToLiveResponse localTtlResponse = dynamoDbClient.describeTimeToLive(ttlRequest);
        DescribeTimeToLiveResponse phoenixTtlResponse =
                phoenixDBClientV2.describeTimeToLive(ttlRequest);

        Assert.assertEquals("TTL status should match",
                localTtlResponse.timeToLiveDescription().timeToLiveStatus(),
                phoenixTtlResponse.timeToLiveDescription().timeToLiveStatus());

        ListTablesRequest listRequest = ListTablesRequest.builder().build();

        ListTablesResponse localListResponse = dynamoDbClient.listTables(listRequest);
        ListTablesResponse phoenixListResponse = phoenixDBClientV2.listTables(listRequest);

        List<String> localTestTables =
                localListResponse.tableNames().stream().filter(name -> name.startsWith("test."))
                        .sorted().collect(Collectors.toList());
        List<String> phoenixTestTables =
                phoenixListResponse.tableNames().stream().filter(name -> name.startsWith("test."))
                        .sorted().collect(Collectors.toList());

        Assert.assertEquals("Test table count should match", localTestTables.size(),
                phoenixTestTables.size());
        Assert.assertEquals("Test table names should match", localTestTables, phoenixTestTables);

        ListTablesRequest paginatedListRequest =
                ListTablesRequest.builder().exclusiveStartTableName(tableName).limit(100).build();

        ListTablesResponse localPaginatedResponse = dynamoDbClient.listTables(paginatedListRequest);
        ListTablesResponse phoenixPaginatedResponse =
                phoenixDBClientV2.listTables(paginatedListRequest);

        List<String> localPaginatedTestTables = localPaginatedResponse.tableNames().stream()
                .filter(name -> name.startsWith("test.")).sorted().collect(Collectors.toList());
        List<String> phoenixPaginatedTestTables = phoenixPaginatedResponse.tableNames().stream()
                .filter(name -> name.startsWith("test.")).sorted().collect(Collectors.toList());

        Assert.assertEquals("Paginated test table count should match",
                localPaginatedTestTables.size(), phoenixPaginatedTestTables.size());
        Assert.assertEquals("Paginated test table names should match", localPaginatedTestTables,
                phoenixPaginatedTestTables);

        DescribeStreamRequest describeStreamRequest =
                DescribeStreamRequest.builder().streamArn(phoenixStreamArn).build();
        StreamDescription phoenixStreamDesc =
                phoenixDBStreamsClientV2.describeStream(describeStreamRequest).streamDescription();
        String phoenixShardId = phoenixStreamDesc.shards().get(0).shardId();

        describeStreamRequest = DescribeStreamRequest.builder().streamArn(dynamoStreamArn).build();
        StreamDescription dynamoStreamDesc =
                dynamoDbStreamsClient.describeStream(describeStreamRequest).streamDescription();
        String dynamoShardId = dynamoStreamDesc.shards().get(0).shardId();

        GetShardIteratorRequest phoenixShardIteratorRequest =
                GetShardIteratorRequest.builder().streamArn(phoenixStreamArn)
                        .shardId(phoenixShardId).shardIteratorType(TRIM_HORIZON).build();
        String phoenixShardIterator =
                phoenixDBStreamsClientV2.getShardIterator(phoenixShardIteratorRequest)
                        .shardIterator();

        GetShardIteratorRequest dynamoShardIteratorRequest =
                GetShardIteratorRequest.builder().streamArn(dynamoStreamArn).shardId(dynamoShardId)
                        .shardIteratorType(TRIM_HORIZON).build();
        String dynamoShardIterator =
                dynamoDbStreamsClient.getShardIterator(dynamoShardIteratorRequest).shardIterator();

        List<Record> allPhoenixRecords = new ArrayList<>();
        List<Record> allDynamoRecords = new ArrayList<>();

        GetRecordsResponse phoenixRecordsResponse;
        GetRecordsResponse dynamoRecordsResponse;

        do {
            GetRecordsRequest phoenixRecordsRequest =
                    GetRecordsRequest.builder().shardIterator(phoenixShardIterator).limit(5)
                            .build();
            phoenixRecordsResponse = phoenixDBStreamsClientV2.getRecords(phoenixRecordsRequest);
            allPhoenixRecords.addAll(phoenixRecordsResponse.records());
            phoenixShardIterator = phoenixRecordsResponse.nextShardIterator();
            LOGGER.info("Phoenix shard iterator: {}", phoenixShardIterator);
        } while (phoenixShardIterator != null && !phoenixRecordsResponse.records().isEmpty());

        do {
            GetRecordsRequest dynamoRecordsRequest =
                    GetRecordsRequest.builder().shardIterator(dynamoShardIterator).limit(6).build();
            dynamoRecordsResponse = dynamoDbStreamsClient.getRecords(dynamoRecordsRequest);
            allDynamoRecords.addAll(dynamoRecordsResponse.records());
            dynamoShardIterator = dynamoRecordsResponse.nextShardIterator();
            LOGGER.info("Dynamo shard iterator: {}", dynamoShardIterator);
        } while (dynamoShardIterator != null && !dynamoRecordsResponse.records().isEmpty());

        LOGGER.info("Phoenix stream records count: {}", allPhoenixRecords.size());
        LOGGER.info("DynamoDB stream records count: {}", allDynamoRecords.size());

        TestUtils.validateRecords(allPhoenixRecords, allDynamoRecords);

        int totalItems = 120;
        int batchSize = 25;
        int numBatches = (int) Math.ceil((double) totalItems / batchSize);

        for (int batchNum = 0; batchNum < numBatches; batchNum++) {
            List<WriteRequest> writeRequests = new ArrayList<>();

            int startIndex = batchNum * batchSize;
            int endIndex = Math.min(startIndex + batchSize, totalItems);
            int itemsInThisBatch = endIndex - startIndex;

            for (int i = 0; i < itemsInThisBatch; i++) {
                int itemIndex = startIndex + i;

                if (itemIndex % 2 == 0) {
                    Map<String, AttributeValue> batchItem = new HashMap<>();
                    batchItem.put("workerId", AttributeValue.builder()
                            .s("batch:delete:" + (itemIndex + itemsInThisBatch)).build());
                    batchItem.put("lastAliveTime", AttributeValue.builder()
                            .n(String.valueOf(System.currentTimeMillis() + itemIndex)).build());
                    batchItem.put("appStartupTime",
                            AttributeValue.builder().n(String.valueOf(System.currentTimeMillis()))
                                    .build());
                    batchItem.put("expirationTime", AttributeValue.builder()
                            .n(String.valueOf(System.currentTimeMillis() + 120000 + itemIndex))
                            .build());

                    writeRequests.add(WriteRequest.builder()
                            .putRequest(PutRequest.builder().item(batchItem).build()).build());
                } else {
                    int deleteIndex = itemIndex / 2;
                    Map<String, AttributeValue> deleteKey = Collections.singletonMap("workerId",
                            AttributeValue.builder().s("batch:delete:" + deleteIndex).build());

                    writeRequests.add(WriteRequest.builder()
                            .deleteRequest(DeleteRequest.builder().key(deleteKey).build()).build());
                }
            }

            Map<String, List<WriteRequest>> requestItems = new HashMap<>();
            requestItems.put(tableName, writeRequests);

            BatchWriteItemRequest batchWriteRequest =
                    BatchWriteItemRequest.builder().requestItems(requestItems).build();

            BatchWriteItemResponse dynamoBatchResponse =
                    dynamoDbClient.batchWriteItem(batchWriteRequest);
            BatchWriteItemResponse phoenixBatchResponse =
                    phoenixDBClientV2.batchWriteItem(batchWriteRequest);

            Assert.assertEquals("Batch " + batchNum + " unprocessed items should match",
                    dynamoBatchResponse.unprocessedItems().size(),
                    phoenixBatchResponse.unprocessedItems().size());

            if (!dynamoBatchResponse.unprocessedItems().isEmpty()) {
                LOGGER.warn("Batch {} had {} unprocessed items", batchNum,
                        dynamoBatchResponse.unprocessedItems().size());
            }

            LOGGER.info("Completed batch {} with {} write requests (items {}-{})",
                    batchNum, writeRequests.size(), startIndex, endIndex - 1);
        }

        phoenixShardIterator = phoenixDBStreamsClientV2
                .getShardIterator(phoenixShardIteratorRequest).shardIterator();

        dynamoShardIterator = dynamoDbStreamsClient
                .getShardIterator(dynamoShardIteratorRequest).shardIterator();

        allPhoenixRecords = new ArrayList<>();
        allDynamoRecords = new ArrayList<>();

        do {
            GetRecordsRequest phoenixRecordsRequest =
                    GetRecordsRequest.builder().shardIterator(phoenixShardIterator).limit(5)
                            .build();
            phoenixRecordsResponse = phoenixDBStreamsClientV2.getRecords(phoenixRecordsRequest);
            allPhoenixRecords.addAll(phoenixRecordsResponse.records());
            phoenixShardIterator = phoenixRecordsResponse.nextShardIterator();
            LOGGER.info("Phoenix shard iterator: {}", phoenixShardIterator);
        } while (phoenixShardIterator != null && !phoenixRecordsResponse.records().isEmpty());

        do {
            GetRecordsRequest dynamoRecordsRequest =
                    GetRecordsRequest.builder().shardIterator(dynamoShardIterator).limit(6).build();
            dynamoRecordsResponse = dynamoDbStreamsClient.getRecords(dynamoRecordsRequest);
            allDynamoRecords.addAll(dynamoRecordsResponse.records());
            dynamoShardIterator = dynamoRecordsResponse.nextShardIterator();
            LOGGER.info("Dynamo shard iterator: {}", dynamoShardIterator);
        } while (dynamoShardIterator != null && !dynamoRecordsResponse.records().isEmpty());

        LOGGER.info("Phoenix stream records count: {}", allPhoenixRecords.size());
        LOGGER.info("DynamoDB stream records count: {}", allDynamoRecords.size());

        allDynamoRecords.sort(
                Comparator.comparing(record -> record.dynamodb().keys().get("workerId").s()));
        allPhoenixRecords.sort(
                Comparator.comparing(record -> record.dynamodb().keys().get("workerId").s()));

        TestUtils.validateRecords(allPhoenixRecords, allDynamoRecords);

        DeleteTableRequest deleteRequest =
                DeleteTableRequest.builder().tableName(tableName).build();
        dynamoDbClient.deleteTable(deleteRequest);
        phoenixDBClientV2.deleteTable(deleteRequest);
    }

    private static void compareScanResponsesSortedByWorkerId(ScanResponse localScanResponse,
            ScanResponse phoenixScanResponse, String contextMessage) {
        Assert.assertEquals(contextMessage + " - scan count should match",
                localScanResponse.count(), phoenixScanResponse.count());

        List<Map<String, AttributeValue>> localSortedItems = localScanResponse.items().stream()
                .sorted(Comparator.comparing(a -> a.get("workerId").s()))
                .collect(Collectors.toList());
        List<Map<String, AttributeValue>> phoenixSortedItems = phoenixScanResponse.items().stream()
                .sorted(Comparator.comparing(a -> a.get("workerId").s()))
                .collect(Collectors.toList());

        for (int i = 0; i < localSortedItems.size(); i++) {
            Assert.assertTrue(contextMessage + " - items should be equal at index " + i + ": "
                            + localSortedItems.get(i) + " vs " + phoenixSortedItems.get(i),
                    ItemComparator.areItemsEqual(localSortedItems.get(i),
                            phoenixSortedItems.get(i)));
        }
    }

    public static void test3(DynamoDbClient dynamoDbClient, DynamoDbClient phoenixDBClientV2,
            DynamoDbStreamsClient dynamoDbStreamsClient,
            DynamoDbStreamsClient phoenixDBStreamsClientV2) throws InterruptedException {

        String table1Name = "test.highVolume_table1";
        String table2Name = "test.highVolume_table2";

        CreateTableRequest table1Request = createHighVolumeTableRequest(table1Name);
        CreateTableRequest table2Request = createHighVolumeTableRequest(table2Name);

        dynamoDbClient.createTable(table1Request);
        phoenixDBClientV2.createTable(table1Request);
        dynamoDbClient.createTable(table2Request);
        phoenixDBClientV2.createTable(table2Request);

        int totalItems = 10000;
        int numPartitions = 13;

        List<Map<String, AttributeValue>> table1Items =
                generateItems(totalItems, numPartitions, "table1");
        List<Map<String, AttributeValue>> table2Items =
                generateItems(totalItems, numPartitions, "table2");

        batchWriteItems(dynamoDbClient, phoenixDBClientV2, table1Name, table1Items);
        batchWriteItems(dynamoDbClient, phoenixDBClientV2, table2Name, table2Items);

        List<Map<String, AttributeValue>> table1ItemsToDelete =
                selectItemsToDelete(table1Items, numPartitions, 0.22);
        List<Map<String, AttributeValue>> table2ItemsToDelete =
                selectItemsToDelete(table2Items, numPartitions, 0.27);

        batchDeleteItems(dynamoDbClient, phoenixDBClientV2, table1Name, table1ItemsToDelete);
        batchDeleteItems(dynamoDbClient, phoenixDBClientV2, table2Name, table2ItemsToDelete);

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            String pkValueTable1 = "pk_" + partitionId + "_t1";
            String pkValueTable2 = "pk_" + partitionId + "_t2";

            LOGGER.info("Querying and comparing partition {} for table1", pkValueTable1);
            QueryRequest.Builder queryBuilder1 = QueryRequest.builder().tableName(table1Name)
                    .keyConditionExpression("pk = :pkval").expressionAttributeValues(
                            Collections.singletonMap(":pkval",
                                    AttributeValue.builder().s(pkValueTable1).build()));
            TestUtils.compareQueryOutputs(queryBuilder1, phoenixDBClientV2, dynamoDbClient);

            LOGGER.info("Querying and comparing partition {} for table2", pkValueTable2);
            QueryRequest.Builder queryBuilder2 = QueryRequest.builder().tableName(table2Name)
                    .keyConditionExpression("pk = :pkval").expressionAttributeValues(
                            Collections.singletonMap(":pkval",
                                    AttributeValue.builder().s(pkValueTable2).build()));
            TestUtils.compareQueryOutputs(queryBuilder2, phoenixDBClientV2, dynamoDbClient);
        }

        queryGSIsWithoutConditions(dynamoDbClient, phoenixDBClientV2, table1Name, "_t1");
        queryGSIsWithSortKeyConditions(dynamoDbClient, phoenixDBClientV2, table2Name, "_t2");

        queryLSIsWithSortKeyConditions(dynamoDbClient, phoenixDBClientV2, table1Name, "_t1");
        queryLSIsWithSortKeyConditions(dynamoDbClient, phoenixDBClientV2, table2Name, "_t2");

        TestUtils.compareAllStreamRecords(table1Name, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);
        TestUtils.compareAllStreamRecords(table2Name, dynamoDbStreamsClient,
                phoenixDBStreamsClientV2);

        dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(table1Name).build());
        phoenixDBClientV2.deleteTable(DeleteTableRequest.builder().tableName(table1Name).build());
        dynamoDbClient.deleteTable(DeleteTableRequest.builder().tableName(table2Name).build());
        phoenixDBClientV2.deleteTable(DeleteTableRequest.builder().tableName(table2Name).build());
    }

    private static CreateTableRequest createHighVolumeTableRequest(String tableName) {
        return CreateTableRequest.builder().tableName(tableName).attributeDefinitions(
                        AttributeDefinition.builder().attributeName("pk")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("sk")
                                .attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("gsi1_pk")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("gsi2_pk")
                                .attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("gsi3_pk")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("gsi1_sk")
                                .attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("gsi2_sk")
                                .attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("gsi3_sk")
                                .attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("lsi1_sk")
                                .attributeType(ScalarAttributeType.N).build(),
                        AttributeDefinition.builder().attributeName("lsi2_sk")
                                .attributeType(ScalarAttributeType.S).build(),
                        AttributeDefinition.builder().attributeName("lsi3_sk")
                                .attributeType(ScalarAttributeType.N).build()).keySchema(
                        KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH).build(),
                        KeySchemaElement.builder().attributeName("sk").keyType(KeyType.RANGE).build())
                .globalSecondaryIndexes(GlobalSecondaryIndex.builder().indexName("gsi_index_1")
                        .keySchema(KeySchemaElement.builder().attributeName("gsi1_pk")
                                        .keyType(KeyType.HASH).build(),
                                KeySchemaElement.builder().attributeName("gsi1_sk")
                                        .keyType(KeyType.RANGE).build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build(), GlobalSecondaryIndex.builder().indexName("gsi_index_2").keySchema(
                                KeySchemaElement.builder().attributeName("gsi2_pk").keyType(KeyType.HASH)
                                        .build(),
                                KeySchemaElement.builder().attributeName("gsi2_sk").keyType(KeyType.RANGE)
                                        .build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build(), GlobalSecondaryIndex.builder().indexName("gsi_index_3").keySchema(
                                KeySchemaElement.builder().attributeName("gsi3_pk").keyType(KeyType.HASH)
                                        .build(),
                                KeySchemaElement.builder().attributeName("gsi3_sk").keyType(KeyType.RANGE)
                                        .build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build())
                        .build()).localSecondaryIndexes(
                        LocalSecondaryIndex.builder().indexName("lsi_index_1").keySchema(
                                        KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH)
                                                .build(),
                                        KeySchemaElement.builder().attributeName("lsi1_sk")
                                                .keyType(KeyType.RANGE).build()).projection(
                                        Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build(), LocalSecondaryIndex.builder().indexName("lsi_index_2")
                                .keySchema(KeySchemaElement.builder().attributeName("pk")
                                                .keyType(KeyType.HASH).build(),
                                        KeySchemaElement.builder().attributeName("lsi2_sk")
                                                .keyType(KeyType.RANGE).build()).projection(
                                        Projection.builder().projectionType(ProjectionType.ALL)
                                                .build()).build(),
                        LocalSecondaryIndex.builder().indexName("lsi_index_3").keySchema(
                                        KeySchemaElement.builder().attributeName("pk").keyType(KeyType.HASH)
                                                .build(),
                                        KeySchemaElement.builder().attributeName("lsi3_sk")
                                                .keyType(KeyType.RANGE).build()).projection(
                                        Projection.builder().projectionType(ProjectionType.ALL).build())
                                .build()).billingMode(BillingMode.PAY_PER_REQUEST)
                .streamSpecification(StreamSpecification.builder().streamEnabled(true)
                        .streamViewType("NEW_AND_OLD_IMAGES").build()).build();
    }

    private static List<Map<String, AttributeValue>> generateItems(int totalItems,
            int numPartitions, String tableId) {
        List<Map<String, AttributeValue>> items = new ArrayList<>();
        int itemsPerPartition = (int) Math.ceil((double) totalItems / numPartitions);

        String tableSuffix = tableId.equals("table1") ? "_t1" : "_t2";
        long timestampOffset = tableId.equals("table1") ? 0 : 1000000;
        int sortKeyOffset = tableId.equals("table1") ? 0 : 1000000;

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            String pkValue = "pk_" + partitionId + tableSuffix;
            int itemsForThisPartition = (partitionId == numPartitions - 1) ?
                    (totalItems - items.size()) :
                    itemsPerPartition;

            for (int sortKeyId = 0; sortKeyId < itemsForThisPartition; sortKeyId++) {
                Map<String, AttributeValue> item = new HashMap<>();
                item.put("pk", AttributeValue.builder().s(pkValue).build());
                item.put("sk", AttributeValue.builder().n(String.valueOf(sortKeyId + sortKeyOffset))
                        .build());

                int gsi1PartitionMod = partitionId % 10;
                int gsi2PartitionMod = partitionId % 20;
                int gsi3PartitionMod = partitionId % 5;

                item.put("gsi1_pk",
                        AttributeValue.builder().s("gsi1_" + gsi1PartitionMod + tableSuffix)
                                .build());
                item.put("gsi2_pk", AttributeValue.builder()
                        .n(String.valueOf(gsi2PartitionMod + (tableId.equals("table1") ? 0 : 100)))
                        .build());
                item.put("gsi3_pk",
                        AttributeValue.builder().s("gsi3_cat_" + gsi3PartitionMod + tableSuffix)
                                .build());

                int gsi1SortKey = sortKeyId + (partitionId * 10000) + sortKeyOffset;
                int gsi2SortKey = sortKeyId + (partitionId * 5000) + sortKeyOffset;
                int gsi3SortKey = sortKeyId + (partitionId * 20000) + sortKeyOffset;

                item.put("gsi1_sk",
                        AttributeValue.builder().n(String.valueOf(gsi1SortKey)).build());
                item.put("gsi2_sk",
                        AttributeValue.builder().n(String.valueOf(gsi2SortKey)).build());
                item.put("gsi3_sk",
                        AttributeValue.builder().n(String.valueOf(gsi3SortKey)).build());

                int lsi1SortKey = 1000000 - sortKeyId + sortKeyOffset;
                item.put("lsi1_sk",
                        AttributeValue.builder().n(String.valueOf(lsi1SortKey)).build());

                String lsi2SortKey = String.format("item_%05d%s", sortKeyId, tableSuffix);
                item.put("lsi2_sk", AttributeValue.builder().s(lsi2SortKey).build());

                long lsi3SortKey =
                        System.currentTimeMillis() + (sortKeyId * 1000L) + timestampOffset;
                item.put("lsi3_sk",
                        AttributeValue.builder().n(String.valueOf(lsi3SortKey)).build());

                item.put("data", AttributeValue.builder()
                        .s("item_data_" + partitionId + "_" + sortKeyId + tableSuffix).build());
                item.put("timestamp", AttributeValue.builder()
                        .n(String.valueOf(System.currentTimeMillis() + sortKeyId + timestampOffset))
                        .build());
                item.put("category",
                        AttributeValue.builder().s("category_" + (sortKeyId % 10) + tableSuffix)
                                .build());

                item.put("isActive", AttributeValue.builder()
                        .bool(tableId.equals("table1") ? sortKeyId % 2 == 0 : sortKeyId % 3 == 0)
                        .build());

                String binaryData = "binary_" + partitionId + "_" + sortKeyId + tableSuffix;
                item.put("binaryData",
                        AttributeValue.builder().b(SdkBytes.fromUtf8String(binaryData)).build());

                item.put("tags", AttributeValue.builder()
                        .l(AttributeValue.builder().s("tag_" + sortKeyId + tableSuffix).build(),
                                AttributeValue.builder().n(String.valueOf(sortKeyId)).build(),
                                AttributeValue.builder().bool(tableId.equals("table1")).build())
                        .build());

                Map<String, AttributeValue> map = new HashMap<>();
                map.put("region", AttributeValue.builder()
                        .s(tableId.equals("table1") ? "us-west-2" : "us-east-1").build());
                map.put("count",
                        AttributeValue.builder().n(String.valueOf(sortKeyId % 100)).build());
                map.put("enabled", AttributeValue.builder().bool(tableId.equals("table1")).build());
                item.put("metadata", AttributeValue.builder().m(map).build());

                item.put("stringSet", AttributeValue.builder()
                        .ss("value1_" + sortKeyId + tableSuffix,
                                "value2_" + partitionId + tableSuffix, "value3" + tableSuffix)
                        .build());

                item.put("numberSet", AttributeValue.builder()
                        .ns(String.valueOf(sortKeyId), String.valueOf(10000 + partitionId),
                                String.valueOf(20000 + (sortKeyId % 100))).build());

                item.put("binarySet", AttributeValue.builder()
                        .bs(SdkBytes.fromUtf8String("bin1_" + sortKeyId + tableSuffix),
                                SdkBytes.fromUtf8String("bin2_" + partitionId + tableSuffix),
                                SdkBytes.fromUtf8String("bin3" + tableSuffix)).build());

                if (sortKeyId % 50 == 0) {
                    item.put("optionalField", AttributeValue.builder().nul(true).build());
                }
                items.add(item);
            }
        }
        return items;
    }

    private static void batchWriteItems(DynamoDbClient dynamoDbClient,
            DynamoDbClient phoenixDBClientV2, String tableName,
            List<Map<String, AttributeValue>> items) {
        int batchSize = 25;
        int totalBatches = (int) Math.ceil((double) items.size() / batchSize);

        for (int batchNum = 0; batchNum < totalBatches; batchNum++) {
            int startIdx = batchNum * batchSize;
            int endIdx = Math.min(startIdx + batchSize, items.size());
            List<Map<String, AttributeValue>> batchItems = items.subList(startIdx, endIdx);

            List<WriteRequest> writeRequests = batchItems.stream()
                    .map(item -> WriteRequest.builder()
                            .putRequest(PutRequest.builder().item(item).build()).build())
                    .collect(Collectors.toList());

            Map<String, List<WriteRequest>> requestItems =
                    Collections.singletonMap(tableName, writeRequests);

            BatchWriteItemRequest batchWriteRequest =
                    BatchWriteItemRequest.builder().requestItems(requestItems).build();

            dynamoDbClient.batchWriteItem(batchWriteRequest);
            phoenixDBClientV2.batchWriteItem(batchWriteRequest);
        }
    }

    private static void batchDeleteItems(DynamoDbClient dynamoDbClient,
            DynamoDbClient phoenixDBClientV2, String tableName,
            List<Map<String, AttributeValue>> itemKeys) {
        int batchSize = 25;
        int totalBatches = (int) Math.ceil((double) itemKeys.size() / batchSize);

        for (int batchNum = 0; batchNum < totalBatches; batchNum++) {
            int startIdx = batchNum * batchSize;
            int endIdx = Math.min(startIdx + batchSize, itemKeys.size());
            List<Map<String, AttributeValue>> batchKeys = itemKeys.subList(startIdx, endIdx);

            List<WriteRequest> deleteRequests = batchKeys.stream().map(key -> WriteRequest.builder()
                    .deleteRequest(DeleteRequest.builder()
                            .key(key).build()).build()).collect(Collectors.toList());

            Map<String, List<WriteRequest>> requestItems =
                    Collections.singletonMap(tableName, deleteRequests);

            BatchWriteItemRequest batchDeleteRequest =
                    BatchWriteItemRequest.builder().requestItems(requestItems).build();

            dynamoDbClient.batchWriteItem(batchDeleteRequest);
            phoenixDBClientV2.batchWriteItem(batchDeleteRequest);
        }
    }

    private static List<Map<String, AttributeValue>> selectItemsToDelete(
            List<Map<String, AttributeValue>> items, int numPartitions, double deletePercentage) {
        List<Map<String, AttributeValue>> itemsToDelete = new ArrayList<>();
        int itemsPerPartition = (int) Math.ceil((double) items.size() / numPartitions);

        for (int partitionId = 0; partitionId < numPartitions; partitionId++) {
            int startIdx = partitionId * itemsPerPartition;
            int endIdx = Math.min(startIdx + itemsPerPartition, items.size());
            int itemsInThisPartition = endIdx - startIdx;
            int itemsToDeleteFromPartition =
                    (int) Math.ceil(itemsInThisPartition * deletePercentage);

            for (int i = 0; i < itemsToDeleteFromPartition && (startIdx + i) < endIdx; i++) {
                int itemIdx = startIdx + (i * itemsInThisPartition / itemsToDeleteFromPartition);
                if (itemIdx < endIdx) {
                    Map<String, AttributeValue> item = items.get(itemIdx);
                    Map<String, AttributeValue> key = new HashMap<>();
                    key.put("pk", item.get("pk"));
                    key.put("sk", item.get("sk"));
                    itemsToDelete.add(key);
                }
            }
        }

        itemsToDelete.sort(
                Comparator.comparing((Map<String, AttributeValue> item) -> item.get("pk").s())
                        .thenComparing(item -> Long.parseLong(item.get("sk").n())));
        return itemsToDelete;
    }

    private static void queryGSIsWithoutConditions(DynamoDbClient dynamoDbClient,
            DynamoDbClient phoenixDBClientV2, String tableName, String tableSuffix) {

        // Query GSI 1 (gsi1_pk values: gsi1_0 to gsi1_9)
        for (int i = 0; i < 10; i++) {
            String gsi1Value = "gsi1_" + i + tableSuffix;
            QueryRequest.Builder queryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("gsi_index_1")
                            .keyConditionExpression("gsi1_pk = :gsi1val").expressionAttributeValues(
                                    Collections.singletonMap(":gsi1val",
                                            AttributeValue.builder().s(gsi1Value).build()));
            TestUtils.compareQueryOutputs(queryBuilder, phoenixDBClientV2, dynamoDbClient);
        }

        // Query GSI 2 (gsi2_pk values: 0 to 19 for table1, 100 to 119 for table2)
        int gsi2Offset = tableSuffix.equals("_t1") ? 0 : 100;
        for (int i = 0; i < 20; i++) {
            QueryRequest.Builder queryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("gsi_index_2")
                            .keyConditionExpression("gsi2_pk = :gsi2val").expressionAttributeValues(
                                    Collections.singletonMap(":gsi2val",
                                            AttributeValue.builder().n(String.valueOf(i + gsi2Offset))
                                                    .build()));
            TestUtils.compareQueryOutputs(queryBuilder, phoenixDBClientV2, dynamoDbClient);
        }

        // Query GSI 3 (gsi3_pk values: gsi3_cat_0 to gsi3_cat_4)
        for (int i = 0; i < 5; i++) {
            String gsi3Value = "gsi3_cat_" + i + tableSuffix;
            QueryRequest.Builder queryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("gsi_index_3")
                            .keyConditionExpression("gsi3_pk = :gsi3val").expressionAttributeValues(
                                    Collections.singletonMap(":gsi3val",
                                            AttributeValue.builder().s(gsi3Value).build()));
            TestUtils.compareQueryOutputs(queryBuilder, phoenixDBClientV2, dynamoDbClient);
        }
    }

    private static void queryGSIsWithSortKeyConditions(DynamoDbClient dynamoDbClient,
            DynamoDbClient phoenixDBClientV2, String tableName, String tableSuffix) {

        // Sort key offset: 0 for table1, 1000000 for table2
        int sortKeyOffset = tableSuffix.equals("_t1") ? 0 : 1000000;

        for (int i = 0; i < 10; i++) {
            String gsi1Value = "gsi1_" + i + tableSuffix;
            // Query items with gsi1_sk > threshold (adjusted for table offset)
            int skThreshold = i * 10000 + 400 + sortKeyOffset;

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":gsi1val", AttributeValue.builder().s(gsi1Value).build());
            expressionValues.put(":skval",
                    AttributeValue.builder().n(String.valueOf(skThreshold)).build());

            QueryRequest.Builder queryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("gsi_index_1")
                            .keyConditionExpression("gsi1_pk = :gsi1val AND gsi1_sk > :skval")
                            .expressionAttributeValues(expressionValues);
            TestUtils.compareQueryOutputs(queryBuilder, phoenixDBClientV2, dynamoDbClient);
        }

        // GSI 2 offset: 0 for table1, 100 for table2
        int gsi2Offset = tableSuffix.equals("_t1") ? 0 : 100;
        for (int i = 0; i < 20; i++) {
            int baseOffset = i * 5000;
            int lowerBound = baseOffset + 100 + sortKeyOffset;
            int upperBound = baseOffset + 400 + sortKeyOffset;

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":gsi2val",
                    AttributeValue.builder().n(String.valueOf(i + gsi2Offset)).build());
            expressionValues.put(":lower",
                    AttributeValue.builder().n(String.valueOf(lowerBound)).build());
            expressionValues.put(":upper",
                    AttributeValue.builder().n(String.valueOf(upperBound)).build());

            QueryRequest.Builder queryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("gsi_index_2")
                            .keyConditionExpression(
                                    "gsi2_pk = :gsi2val AND gsi2_sk BETWEEN :lower AND :upper")
                            .expressionAttributeValues(expressionValues);
            TestUtils.compareQueryOutputs(queryBuilder, phoenixDBClientV2, dynamoDbClient);
        }

        for (int i = 0; i < 5; i++) {
            String gsi3Value = "gsi3_cat_" + i + tableSuffix;
            // Query items with gsi3_sk < threshold (adjusted for table offset)
            int skThreshold = i * 20000 + 500 + sortKeyOffset;

            Map<String, AttributeValue> expressionValues = new HashMap<>();
            expressionValues.put(":gsi3val", AttributeValue.builder().s(gsi3Value).build());
            expressionValues.put(":skval",
                    AttributeValue.builder().n(String.valueOf(skThreshold)).build());

            QueryRequest.Builder queryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("gsi_index_3")
                            .keyConditionExpression("gsi3_pk = :gsi3val AND gsi3_sk < :skval")
                            .expressionAttributeValues(expressionValues);
            TestUtils.compareQueryOutputs(queryBuilder, phoenixDBClientV2, dynamoDbClient);
        }
    }

    private static void queryLSIsWithSortKeyConditions(DynamoDbClient dynamoDbClient,
            DynamoDbClient phoenixDBClientV2, String tableName, String tableSuffix) {

        int sortKeyOffset = tableSuffix.equals("_t1") ? 0 : 1000000;

        for (int partitionId = 0; partitionId < 5; partitionId++) {
            String pkValue = "pk_" + partitionId + tableSuffix;

            // LSI 1: Query with numeric sort key > condition (reverse order from base sk)
            // lsi1_sk = 1000000 - sortKeyId + sortKeyOffset
            int lsi1Threshold = 1000000 - 200 + sortKeyOffset; // Get items where original sk < 200
            Map<String, AttributeValue> lsi1Expr = new HashMap<>();
            lsi1Expr.put(":pkval", AttributeValue.builder().s(pkValue).build());
            lsi1Expr.put(":skval",
                    AttributeValue.builder().n(String.valueOf(lsi1Threshold)).build());

            QueryRequest.Builder lsi1QueryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("lsi_index_1")
                            .keyConditionExpression("pk = :pkval AND lsi1_sk > :skval")
                            .expressionAttributeValues(lsi1Expr);
            TestUtils.compareQueryOutputs(lsi1QueryBuilder, phoenixDBClientV2, dynamoDbClient);

            // LSI 2: Query with string sort key BETWEEN condition
            // lsi2_sk format: "item_XXXXX_t1" or "item_XXXXX_t2"
            String lsi2Lower = String.format("item_%05d%s", 100, tableSuffix);
            String lsi2Upper = String.format("item_%05d%s", 300, tableSuffix);
            Map<String, AttributeValue> lsi2Expr = new HashMap<>();
            lsi2Expr.put(":pkval", AttributeValue.builder().s(pkValue).build());
            lsi2Expr.put(":lower", AttributeValue.builder().s(lsi2Lower).build());
            lsi2Expr.put(":upper", AttributeValue.builder().s(lsi2Upper).build());

            QueryRequest.Builder lsi2QueryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("lsi_index_2")
                            .keyConditionExpression(
                                    "pk = :pkval AND lsi2_sk BETWEEN :lower AND :upper")
                            .expressionAttributeValues(lsi2Expr);
            TestUtils.compareQueryOutputs(lsi2QueryBuilder, phoenixDBClientV2, dynamoDbClient);

            // LSI 3: Query with timestamp-based sort key < condition
            // lsi3_sk = currentTime + (sortKeyId * 1000) + timestampOffset
            // Query for items with lsi3_sk less than a certain threshold
            long lsi3Threshold =
                    System.currentTimeMillis() + (150 * 1000) + (tableSuffix.equals("_t1") ?
                            0 :
                            1000000);
            Map<String, AttributeValue> lsi3Expr = new HashMap<>();
            lsi3Expr.put(":pkval", AttributeValue.builder().s(pkValue).build());
            lsi3Expr.put(":skval",
                    AttributeValue.builder().n(String.valueOf(lsi3Threshold)).build());

            QueryRequest.Builder lsi3QueryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("lsi_index_3")
                            .keyConditionExpression("pk = :pkval AND lsi3_sk <= :skval")
                            .expressionAttributeValues(lsi3Expr);
            TestUtils.compareQueryOutputs(lsi3QueryBuilder, phoenixDBClientV2, dynamoDbClient);

            QueryRequest.Builder lsi4QueryBuilder =
                    QueryRequest.builder().tableName(tableName).indexName("lsi_index_3")
                            .scanIndexForward(false)
                            .keyConditionExpression("pk = :pkval AND lsi3_sk < :skval")
                            .expressionAttributeValues(lsi3Expr);
            TestUtils.compareQueryOutputs(lsi4QueryBuilder, phoenixDBClientV2, dynamoDbClient);
        }
    }

}
