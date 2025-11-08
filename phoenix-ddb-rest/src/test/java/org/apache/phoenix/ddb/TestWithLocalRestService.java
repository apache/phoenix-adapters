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

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

public class TestWithLocalRestService {

    private final String phoenixRestEndpoint;
    private final DynamoDbClient dynamoDbClient;
    private final DynamoDbClient phoenixDBClientV2;

    public TestWithLocalRestService(String phoenixRestEndpoint) {
        this.phoenixRestEndpoint = phoenixRestEndpoint;
        this.phoenixDBClientV2 = LocalDynamoDB.createV2Client(phoenixRestEndpoint);
        LocalDynamoDbTestBase.localDynamoDb().start();
        this.dynamoDbClient = LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    }

    public void runTests() throws Exception {
        queryLimitAndFilterTest();
    }

    private void queryLimitAndFilterTest() throws Exception {
        final String tableName = "TEST_TABLE1";
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "attr_0",
                        ScalarAttributeType.S, "attr_1", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        PutItemRequest putItemRequest1 =
                PutItemRequest.builder().tableName(tableName).item(QueryIT.getItem1()).build();
        PutItemRequest putItemRequest2 =
                PutItemRequest.builder().tableName(tableName).item(QueryIT.getItem2()).build();
        PutItemRequest putItemRequest3 =
                PutItemRequest.builder().tableName(tableName).item(QueryIT.getItem3()).build();
        PutItemRequest putItemRequest4 =
                PutItemRequest.builder().tableName(tableName).item(QueryIT.getItem4()).build();
        phoenixDBClientV2.putItem(putItemRequest1);
        phoenixDBClientV2.putItem(putItemRequest2);
        phoenixDBClientV2.putItem(putItemRequest3);
        phoenixDBClientV2.putItem(putItemRequest4);
        dynamoDbClient.putItem(putItemRequest1);
        dynamoDbClient.putItem(putItemRequest2);
        dynamoDbClient.putItem(putItemRequest3);
        dynamoDbClient.putItem(putItemRequest4);

        QueryRequest.Builder qr = QueryRequest.builder().tableName(tableName);
        qr.keyConditionExpression("#0 = :v0 AND #1 < :v1");
        qr.filterExpression("#2 <= :v2");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#0", "attr_0");
        exprAttrNames.put("#1", "attr_1");
        exprAttrNames.put("#2", "Id2");
        qr.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v0", AttributeValue.builder().s("B").build());
        exprAttrVal.put(":v1", AttributeValue.builder().n("4").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("1000.10").build());
        qr.expressionAttributeValues(exprAttrVal);

        QueryResponse phoenixResult = phoenixDBClientV2.query(qr.build());
        QueryResponse dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 2);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());

        qr.limit(1);
        phoenixResult = phoenixDBClientV2.query(qr.build());
        dynamoResult = dynamoDbClient.query(qr.build());
        Assert.assertTrue(dynamoResult.count() == 1);
        Assert.assertEquals(dynamoResult.count(), phoenixResult.count());
        Assert.assertEquals(dynamoResult.items().get(0), phoenixResult.items().get(0));
    }

    public static void main(String[] args) throws Exception {
        // Before starting this test, run command:
        // "bin/phoenix-adapters rest start -p 8842 -z localhost:2181" to start rest service
        TestWithLocalRestService testWithLocalRestService =
                new TestWithLocalRestService("http://localhost:8842");
        try {
            testWithLocalRestService.runTests();
        } finally {
            LocalDynamoDbTestBase.localDynamoDb().stop();
        }
        System.exit(0);
    }
}
