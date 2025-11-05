import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.BsonDocument;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ShardIteratorType;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamStatus;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
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
        String tableName = sr.tableName();
        String indexName = sr.indexName();
        List<PColumn> tablePKCols, indexPKCols;
        try (Connection connection = DriverManager.getConnection(url)) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            PreparedStatement ps =
                    ScanService.getPreparedStatement(connection, getScanRequest(sr), true,
                            tablePKCols, indexPKCols);
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
}
