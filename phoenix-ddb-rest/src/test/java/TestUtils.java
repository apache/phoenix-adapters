import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.apache.phoenix.ddb.service.QueryService;
import org.apache.phoenix.ddb.service.ScanService;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.util.SchemaUtil;

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
            Assert.assertEquals("DDB." + indexName, explainPlanAttributes.getTableName());
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
        try (Connection connection = DriverManager.getConnection(url)) {
            PreparedStatement ps = ScanService.getPreparedStatement(connection, getScanRequest(sr));
            ExplainPlan plan =
                    ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
            Assert.assertEquals(scanType, explainPlanAttributes.getExplainScanType());
            Assert.assertEquals("DDB." + indexName, explainPlanAttributes.getTableName());
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

        validateRecords(allPhoenixRecords, allDynamoRecords);
    }

    public static int getNumberOfTableRegions(Connection conn, String tableName)
            throws SQLException {
        String fullTableName = SchemaUtil.getTableName("DDB", tableName);
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

        List<Map<String, AttributeValue>> ddbResult = new ArrayList<>();
        QueryResponse ddbResponse;
        do {
            ddbResponse = dynamoDbClient.query(qr.build());
            ddbResult.addAll(ddbResponse.items());
            qr.exclusiveStartKey(ddbResponse.lastEvaluatedKey());
        } while (ddbResponse.hasLastEvaluatedKey());
        Assert.assertEquals(ddbResult, phoenixResult);
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
        List<Map<String, AttributeValue>> sortedExpected = TestUtils.sortItemsByPartitionAndSortKey(expectedItems, hashKeyName, sortKeyName);
        List<Map<String, AttributeValue>> sortedActual = TestUtils.sortItemsByPartitionAndSortKey(actualItems, hashKeyName, sortKeyName);

        // Use ItemComparator for proper item comparison
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
}
