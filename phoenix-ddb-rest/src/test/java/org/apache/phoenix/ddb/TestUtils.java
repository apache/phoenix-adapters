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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.bson.BsonDocument;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.ddb.service.QueryService;
import org.apache.phoenix.ddb.service.ScanService;
import org.apache.phoenix.ddb.service.utils.ScanConfig;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;

import static org.apache.phoenix.end2end.IndexToolIT.getArgValues;
import static org.junit.Assert.assertEquals;
import static software.amazon.awssdk.services.dynamodb.model.ShardIteratorType.TRIM_HORIZON;

public class TestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

    /**
     * Verify index is used for a SQL query formed using a QueryRequest.
     */
    public static void validateIndexUsed(QueryRequest qr, String url) throws SQLException {
        String tableName = qr.tableName();
        String indexName = qr.indexName();
        List<PColumn> tablePKCols, indexPKCols;
        try (Connection connection = DriverManager.getConnection(url)) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            PreparedStatement ps =
                    QueryService.getPreparedStatement(connection, getQueryRequest(qr), true,
                            tablePKCols, indexPKCols).getFirst();
            ExplainPlan plan =
                    ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
            Assert.assertEquals("RANGE SCAN ", explainPlanAttributes.getExplainScanType());
            Assert.assertEquals(PhoenixUtils.getFullTableName(tableName + "_" + indexName, false),
                    explainPlanAttributes.getTableName());
        }
    }

    private static Map<String, Object> getQueryRequest(QueryRequest qr) {
        Map<String, Object> queryRequest = new HashMap<>();
        queryRequest.put("TableName", qr.tableName());
        queryRequest.put("ScanIndexForward", qr.scanIndexForward());
        queryRequest.put("ExpressionAttributeNames", qr.expressionAttributeNames());
        queryRequest.put("ExpressionAttributeValues",
                getMapFromAttributeValueMap(qr.expressionAttributeValues()));
        queryRequest.put("Limit", qr.limit());
        queryRequest.put("ProjectionExpression", qr.projectionExpression());
        queryRequest.put("AttributesToGet", qr.attributesToGet());
        queryRequest.put("ExclusiveStartKey", getMapFromAttributeValueMap(qr.exclusiveStartKey()));
        queryRequest.put("IndexName", qr.indexName());
        queryRequest.put("KeyConditionExpression", qr.keyConditionExpression());
        return queryRequest;
    }

    private static Map<String, Object> getScanRequest(ScanRequest sr) {
        Map<String, Object> queryRequest = new HashMap<>();
        queryRequest.put("TableName", sr.tableName());
        queryRequest.put("ExpressionAttributeNames", sr.expressionAttributeNames());
        queryRequest.put("ExpressionAttributeValues",
                getMapFromAttributeValueMap(sr.expressionAttributeValues()));
        queryRequest.put("Limit", sr.limit());
        queryRequest.put("ProjectionExpression", sr.projectionExpression());
        queryRequest.put("AttributesToGet", sr.attributesToGet());
        queryRequest.put("ExclusiveStartKey", getMapFromAttributeValueMap(sr.exclusiveStartKey()));
        queryRequest.put("IndexName", sr.indexName());
        return queryRequest;
    }

    private static Map<String, Object> getMapFromAttributeValueMap(
            Map<String, AttributeValue> item) {
        BsonDocument document = DdbAttributesToBsonDocument.getBsonDocument(item);
        return BsonDocumentToMap.getFullItem(document);
    }

    /**
     * Verify index is used for a SQL query formed using a ScanRequest.
     */
    public static void validateIndexUsed(ScanRequest sr, String url, String scanType)
            throws SQLException {
        String indexName = sr.indexName();
        String tableName = sr.tableName();
        try (Connection connection = DriverManager.getConnection(url)) {
            PreparedStatement ps = getPreparedStatementForScan(connection, getScanRequest(sr));
            ExplainPlan plan =
                    ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
            Assert.assertEquals(scanType, explainPlanAttributes.getExplainScanType());
            Assert.assertEquals(PhoenixUtils.getFullTableName(tableName + "_" + indexName, false),
                    explainPlanAttributes.getTableName());
        }
    }

    /**
     * Returns the primary PreparedStatement that would be used for the scan request.
     * For two-query scenarios, returns the first query's PreparedStatement.
     */
    private static PreparedStatement getPreparedStatementForScan(Connection connection,
            Map<String, Object> request)
            throws SQLException {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String indexName = (String) request.get(ApiMetadata.INDEX_NAME);
        boolean useIndex = !StringUtils.isEmpty(indexName);

        List<PColumn> tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
        List<PColumn> indexPKCols = useIndex ? PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName) : null;

        Map<String, Object> exclusiveStartKey = (Map<String, Object>) request.get(ApiMetadata.EXCLUSIVE_START_KEY);
        int effectiveLimit = 100;
        boolean countOnly = ApiMetadata.SELECT_COUNT.equals(request.get(ApiMetadata.SELECT));

        ScanConfig config = new ScanConfig(
                ScanService.determineScanType(exclusiveStartKey, useIndex, tablePKCols, indexPKCols),
                useIndex, tablePKCols, indexPKCols, effectiveLimit, tableName, indexName, countOnly
        );

        // For two-query scenarios, return the first query's PreparedStatement
        if (config.getType() == ScanConfig.ScanType.TWO_KEY_FIRST_QUERY) {
            return ScanService.buildQuery(connection, request, config);
        } else {
            return ScanService.buildQuery(connection, request, config);
        }
    }

    /**
     * Wait for stream to get ENABLED.
     */
    public static void waitForStream(DynamoDbStreamsClient client, String streamArn)
            throws InterruptedException {
        DescribeStreamRequest dsr = DescribeStreamRequest.builder().streamArn(streamArn).build();
        StreamDescription streamDesc = client.describeStream(dsr).streamDescription();
        int i = 0;
        while (i < 200 && StreamStatus.ENABLING == streamDesc.streamStatus()) {
            streamDesc = client.describeStream(dsr).streamDescription();
            if (StreamStatus.ENABLED == streamDesc.streamStatus()) {
                break;
            }
            i++;
            Thread.sleep(1000);
        }
        Assert.assertEquals(StreamStatus.ENABLED, streamDesc.streamStatus());
    }

    /**
     * Validate change records.
     */
    public static void validateRecords(List<Record> phoenixRecords, List<Record> dynamoRecords) {
        Assert.assertEquals("Stream record counts should match between Phoenix and DynamoDB",
                dynamoRecords.size(), phoenixRecords.size());
        for (int i = 0; i < dynamoRecords.size(); i++) {
            Record dr = dynamoRecords.get(i);
            Record pr = phoenixRecords.get(i);
            Assert.assertEquals("Event names should match for record " + i, dr.eventName(),
                    pr.eventName());
            Assert.assertNotNull("DynamoDB record should have dynamodb field for record " + i,
                    dr.dynamodb());
            Assert.assertNotNull("Phoenix record should have dynamodb field for record " + i,
                    pr.dynamodb());
            Assert.assertEquals("Stream view types should match for record " + i,
                    dr.dynamodb().streamViewType(), pr.dynamodb().streamViewType());
            Assert.assertEquals("Record keys should match for record " + i, dr.dynamodb().keys(),
                    pr.dynamodb().keys());
            Assert.assertEquals("Old images should match for record " + i, dr.dynamodb().oldImage(),
                    pr.dynamodb().oldImage());
            Assert.assertEquals("New images should match for record " + i, dr.dynamodb().newImage(),
                    pr.dynamodb().newImage());
            Assert.assertTrue("DynamoDB record size should be greater than 0 for record " + i,
                    dr.dynamodb().sizeBytes() > 0);
            Assert.assertTrue("Phoenix record size should be greater than 0 for record " + i,
                    pr.dynamodb().sizeBytes() > 0);
        }
    }

    /**
     * Split a table at the given split point.
     */
    public static void splitTable(Connection conn, String tableName, byte[] splitPoint)
            throws Exception {
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        Configuration configuration = services.getConfiguration();
        org.apache.hadoop.hbase.client.Connection hbaseConn =
                ConnectionFactory.createConnection(configuration);
        Admin admin = services.getAdmin();
        RegionLocator regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName));
        int nRegions = regionLocator.getAllRegionLocations().size();
        admin.split(TableName.valueOf(tableName), splitPoint);
        int retryCount = 0;
        while (retryCount < 20 && regionLocator.getAllRegionLocations().size() == nRegions) {
            Thread.sleep(5000);
            retryCount++;
        }
        Assert.assertNotEquals(regionLocator.getAllRegionLocations().size(), nRegions);
    }

    /**
     * Return all records from a shard of a stream using TRIM_HORIZON.
     */
    public static List<Record> getRecordsFromShardWithLimit(DynamoDbStreamsClient client,
            String streamArn, String shardId, ShardIteratorType iterType, String seqNum,
            Integer limit) {
        GetShardIteratorRequest gsir =
                GetShardIteratorRequest.builder().streamArn(streamArn).shardId(shardId)
                        .shardIteratorType(iterType).sequenceNumber(seqNum).build();
        String shardIter = client.getShardIterator(gsir).shardIterator();

        // get records
        GetRecordsRequest grr =
                GetRecordsRequest.builder().shardIterator(shardIter).limit(limit).build();
        List<Record> records = new ArrayList<>();
        GetRecordsResponse result;
        do {
            result = client.getRecords(grr);
            records.addAll(result.records());
            grr = grr.toBuilder().shardIterator(result.nextShardIterator()).build();
        } while (result.nextShardIterator() != null && !result.records().isEmpty());
        return records;
    }

    public static void compareAllStreamRecords(final String tableName,
            final DynamoDbStreamsClient dynamoDbStreamsClient,
            final DynamoDbStreamsClient phoenixDBStreamsClientV2) throws InterruptedException {

        ListStreamsRequest listStreamsRequest =
                ListStreamsRequest.builder().tableName(tableName).build();
        ListStreamsResponse phoenixStreams =
                phoenixDBStreamsClientV2.listStreams(listStreamsRequest);
        String phoenixStreamArn = phoenixStreams.streams().get(0).streamArn();
        String dynamoStreamArn =
                dynamoDbStreamsClient.listStreams(listStreamsRequest).streams().get(0).streamArn();

        waitForStream(phoenixDBStreamsClientV2, phoenixStreamArn);
        waitForStream(dynamoDbStreamsClient, dynamoStreamArn);

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
        } while (phoenixShardIterator != null && !phoenixRecordsResponse.records().isEmpty());

        do {
            GetRecordsRequest dynamoRecordsRequest =
                    GetRecordsRequest.builder().shardIterator(dynamoShardIterator).limit(6).build();
            dynamoRecordsResponse = dynamoDbStreamsClient.getRecords(dynamoRecordsRequest);
            allDynamoRecords.addAll(dynamoRecordsResponse.records());
            dynamoShardIterator = dynamoRecordsResponse.nextShardIterator();
        } while (dynamoShardIterator != null && !dynamoRecordsResponse.records().isEmpty());

        LOGGER.info("Phoenix stream records count: {}", allPhoenixRecords.size());
        LOGGER.info("DynamoDB stream records count: {}", allDynamoRecords.size());

        validateRecords(allPhoenixRecords, allDynamoRecords);
    }

    public static int getNumberOfTableRegions(Connection conn, String tableName)
            throws SQLException {
        String fullTableName = PhoenixUtils.getFullTableName(tableName, false);
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        List<HRegionLocation> regs
                = pconn.getQueryServices().getAllTableRegions(fullTableName.getBytes(), 30000);
        return regs.size();
    }

    public static void compareQueryOutputs(QueryRequest.Builder qr,
            DynamoDbClient phoenixDBClientV2, DynamoDbClient dynamoDbClient) {
        List<Map<String, AttributeValue>> phoenixResult = new ArrayList<>();
        QueryResponse phoenixResponse;
        do {
            phoenixResponse = phoenixDBClientV2.query(qr.build());
            phoenixResult.addAll(phoenixResponse.items());
            qr.exclusiveStartKey(phoenixResponse.lastEvaluatedKey());
        } while (phoenixResponse.hasLastEvaluatedKey());

        qr.exclusiveStartKey(null);
        List<Map<String, AttributeValue>> ddbResult = new ArrayList<>();
        QueryResponse ddbResponse;
        do {
            ddbResponse = dynamoDbClient.query(qr.build());
            ddbResult.addAll(ddbResponse.items());
            qr.exclusiveStartKey(ddbResponse.lastEvaluatedKey());
        } while (ddbResponse.hasLastEvaluatedKey());
        Assert.assertEquals(ddbResult.size(), phoenixResult.size());
        Assert.assertTrue(ItemComparator.areItemsEqual(ddbResult, phoenixResult));
    }

    public static void compareScanOutputs(ScanRequest.Builder sr,
            DynamoDbClient phoenixDBClientV2, DynamoDbClient dynamoDbClient,
            String partitionKeyName, String sortKeyName, ScalarAttributeType partitionKeyType,
            ScalarAttributeType sortKeyType) {
        List<Map<String, AttributeValue>> phoenixResult = new ArrayList<>();
        ScanResponse phoenixResponse;
        do {
            phoenixResponse = phoenixDBClientV2.scan(sr.build());
            phoenixResult.addAll(phoenixResponse.items());
            sr.exclusiveStartKey(phoenixResponse.lastEvaluatedKey());
        } while (phoenixResponse.hasLastEvaluatedKey());

        List<Map<String, AttributeValue>> ddbResult = new ArrayList<>();
        ScanResponse ddbResponse;
        do {
            ddbResponse = dynamoDbClient.scan(sr.build());
            ddbResult.addAll(ddbResponse.items());
            sr.exclusiveStartKey(ddbResponse.lastEvaluatedKey());
        } while (ddbResponse.hasLastEvaluatedKey());

        verifyItemsEqual(ddbResult, phoenixResult, partitionKeyName, sortKeyName);
    }

    /**
     * Verify that two lists of items contain the same items.
     */
    public static void verifyItemsEqual(List<Map<String, AttributeValue>> expectedItems,
            List<Map<String, AttributeValue>> actualItems, String hashKeyName, String sortKeyName) {

        Assert.assertEquals("Item count mismatch.", expectedItems.size(), actualItems.size());

        // Sort both lists by primary key for consistent comparison
        List<Map<String, AttributeValue>> sortedExpected = sortItemsByPartitionAndSortKey(expectedItems, hashKeyName, sortKeyName);
        List<Map<String, AttributeValue>> sortedActual = sortItemsByPartitionAndSortKey(actualItems, hashKeyName, sortKeyName);

        // Use org.apache.phoenix.ddb.ItemComparator for proper item comparison
        Assert.assertTrue("Items don't match. ",  ItemComparator.areItemsEqual(sortedExpected, sortedActual));
    }

    /**
     * Sort scan result items by partition key and sort key with automatic type detection.
     * This is useful when the attribute types are not known at compile time.
     */
    public static List<Map<String, AttributeValue>> sortItemsByPartitionAndSortKey(
            List<Map<String, AttributeValue>> items, String partitionKeyName, String sortKeyName) {
        return items.stream().sorted((item1, item2) -> {
            // Compare partition keys first
            AttributeValue partitionValue1 = item1.get(partitionKeyName);
            AttributeValue partitionValue2 = item2.get(partitionKeyName);
            int partitionComparison = compareAttributeValues(partitionValue1, partitionValue2);
            
            if (partitionComparison != 0) {
                return partitionComparison;
            }
            
            // If partition keys are equal and sort key exists, compare sort keys
            if (sortKeyName != null) {
                AttributeValue sortValue1 = item1.get(sortKeyName);
                AttributeValue sortValue2 = item2.get(sortKeyName);
                return compareAttributeValues(sortValue1, sortValue2);
            }
            
            return 0;
        }).collect(Collectors.toList());
    }

    /**
     * Compare two AttributeValues with automatic type detection.
     * This is useful when the attribute types are not known at compile time.
     */
    public static int compareAttributeValues(AttributeValue value1, AttributeValue value2) {
        if (value1 == null && value2 == null) return 0;
        if (value1 == null) return -1;
        if (value2 == null) return 1;
        
        // String comparison
        if (value1.s() != null && value2.s() != null) {
            return value1.s().compareTo(value2.s());
        }
        
        // Number comparison
        if (value1.n() != null && value2.n() != null) {
            try {
                Double num1 = Double.parseDouble(value1.n());
                Double num2 = Double.parseDouble(value2.n());
                return num1.compareTo(num2);
            } catch (NumberFormatException e) {
                // Fallback to string comparison if parsing fails
                return value1.n().compareTo(value2.n());
            }
        }
        
        // Binary comparison - use proper byte array comparison
        if (value1.b() != null && value2.b() != null) {
            byte[] bytes1 = value1.b().asByteArray();
            byte[] bytes2 = value2.b().asByteArray();
            return Bytes.compareTo(bytes1, bytes2);
        }
        
        // Fallback: convert to string for comparison
        return String.valueOf(value1).compareTo(String.valueOf(value2));
    }

    public static void validateTableProps(String url, String tableName, boolean mergeEnabled) throws SQLException {
        String fullTableName = PhoenixUtils.getFullTableName(tableName, false);
        try (Connection conn = DriverManager.getConnection(url)) {
            PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTable(
                    new PTableKey(phoenixConnection.getTenantId(), fullTableName));
            Assert.assertFalse(table.isStrictTTL());
            Assert.assertEquals(1800000, table.getUpdateCacheFrequency());
            TableDescriptor td = phoenixConnection.getQueryServices()
                    .getTableDescriptor(fullTableName.getBytes());
            Assert.assertEquals(mergeEnabled, td.isMergeEnabled());
            Assert.assertEquals(97200,
                    Integer.parseInt(td.getValue("phoenix.max.lookback.age.seconds")));
            Assert.assertEquals(172800000,
                    Integer.parseInt(td.getValue("hbase.hregion.majorcompaction")));
        }
    }

    public static void waitForIndexState(DynamoDbClient client, String tableName, String indexName,
            String expectedState) {
        DescribeTableRequest dtr = DescribeTableRequest.builder().tableName(tableName).build();
        int maxTries = 20, nTries = 0;
        do {
            DescribeTableResponse dtrResponse = client.describeTable(dtr);
            List<GlobalSecondaryIndexDescription> gsiList = dtrResponse.table().globalSecondaryIndexes();
            Assert.assertFalse(gsiList.isEmpty());
            String indexStatus = "";
            for (GlobalSecondaryIndexDescription gsi : gsiList) {
                if (indexName.equals(gsi.indexName())) {
                    indexStatus = gsi.indexStatus().toString();
                    break;
                }
            }
            if (expectedState.equals(indexStatus)) {
                return;
            }
        }
        while (++nTries < maxTries);
        Assert.fail("Timed out waiting for index state to become: " + expectedState);
    }

    public static IndexTool runIndexTool(Configuration conf, boolean useSnapshot, String schemaName,
            String dataTableName, String indexTableName, String tenantId, int expectedStatus,
            IndexTool.IndexVerifyType verifyType, IndexTool.IndexDisableLoggingType disableLoggingType,
            String... additionalArgs) throws Exception {
        IndexTool indexingTool = new IndexTool();
        indexingTool.setConf(conf);
        final String[] cmdArgs = getArgValues(useSnapshot, schemaName, dataTableName, indexTableName,
                tenantId, verifyType, disableLoggingType);
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        cmdArgList.addAll(Arrays.asList(additionalArgs));
        LOGGER.info("Running IndexTool with {}", Arrays.toString(cmdArgList.toArray()),
                new Exception("Stack Trace"));
        int status = indexingTool.run(cmdArgList.toArray(new String[cmdArgList.size()]));
        assertEquals(expectedStatus, status);
        return indexingTool;
    }
}
