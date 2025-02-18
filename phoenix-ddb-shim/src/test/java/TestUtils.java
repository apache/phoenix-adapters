import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.Record;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ShardIteratorType;
import com.amazonaws.services.dynamodbv2.model.StreamDescription;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.ddb.PhoenixDBStreamsClient;
import org.apache.phoenix.ddb.service.QueryService;
import org.apache.phoenix.ddb.service.ScanService;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.query.ConnectionQueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.util.CDCUtil;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class TestUtils {

    /**
     * Verify index is used for a SQL query formed using a QueryRequest.
     */
    public static void validateIndexUsed(QueryRequest qr, String url) throws SQLException {
        String tableName = qr.getTableName();
        String indexName = qr.getIndexName();
        List<PColumn> tablePKCols, indexPKCols;
        try (Connection connection = DriverManager.getConnection(url)) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            PreparedStatement ps = QueryService.getPreparedStatement(connection, qr, true, tablePKCols, indexPKCols);
            ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
            Assert.assertEquals(indexName, explainPlanAttributes.getTableName());
        }
    }

    /**
     * Verify index is used for a SQL query formed using a ScanRequest.
     */
    public static void validateIndexUsed(ScanRequest sr, String url) throws SQLException {
        String tableName = sr.getTableName();
        String indexName = sr.getIndexName();
        List<PColumn> tablePKCols, indexPKCols;
        try (Connection connection = DriverManager.getConnection(url)) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            PreparedStatement ps = ScanService.getPreparedStatement(connection, sr, true, tablePKCols, indexPKCols);
            ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
            Assert.assertEquals(indexName, explainPlanAttributes.getTableName());
        }
    }

    /**
     * Wait for stream to get ENABLED.
     */
    public static void waitForStream(PhoenixDBStreamsClient client, String streamArn) throws InterruptedException {
        DescribeStreamRequest dsr = new DescribeStreamRequest().withStreamArn(streamArn);
        StreamDescription phoenixStreamDesc = client.describeStream(dsr).getStreamDescription();
        int i=0;
        while (i < 20 && CDCUtil.CdcStreamStatus.ENABLING.getSerializedValue().equals(phoenixStreamDesc.getStreamStatus())) {
            phoenixStreamDesc = client.describeStream(dsr).getStreamDescription();
            if (CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue().equals(phoenixStreamDesc.getStreamStatus())) {
                break;
            }
            i++;
            Thread.sleep(1000);
        }
        Assert.assertEquals(CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue(), phoenixStreamDesc.getStreamStatus());
    }

    /**
     * Validate change records.
     */
    public static void validateRecords(List<Record> phoenixRecords, List<Record> dynamoRecords) {
        Assert.assertEquals(dynamoRecords.size(), phoenixRecords.size());
        for (int i = 0; i<dynamoRecords.size(); i++) {
            Record dr = dynamoRecords.get(i);
            Record pr = phoenixRecords.get(i);
            Assert.assertEquals(dr.getEventName(), pr.getEventName());
            Assert.assertNotNull(dr.getDynamodb());
            Assert.assertNotNull(pr.getDynamodb());
            Assert.assertEquals(dr.getDynamodb().getStreamViewType(), pr.getDynamodb().getStreamViewType());
            Assert.assertEquals(dr.getDynamodb().getKeys(), pr.getDynamodb().getKeys());
            Assert.assertEquals(dr.getDynamodb().getOldImage(), pr.getDynamodb().getOldImage());
            Assert.assertEquals(dr.getDynamodb().getNewImage(), pr.getDynamodb().getNewImage());
        }
    }

    /**
     * Split a table at the given split point.
     */
    public static void splitTable(Connection conn, String tableName, byte[] splitPoint) throws Exception {
        ConnectionQueryServices services = conn.unwrap(PhoenixConnection.class).getQueryServices();
        Configuration configuration = services.getConfiguration();
        org.apache.hadoop.hbase.client.Connection hbaseConn
                = ConnectionFactory.createConnection(configuration);
        Admin admin = services.getAdmin();
        RegionLocator regionLocator = hbaseConn.getRegionLocator(TableName.valueOf(tableName));
        int nRegions = regionLocator.getAllRegionLocations().size();
        admin.split(TableName.valueOf(tableName), splitPoint);
        int retryCount = 0;
        while (retryCount < 20
                && regionLocator.getAllRegionLocations().size() == nRegions) {
            Thread.sleep(5000);
            retryCount++;
        }
        Assert.assertNotEquals(regionLocator.getAllRegionLocations().size(), nRegions);
    }

    /**
     * Return all records from a shard of a stream using TRIM_HORIZON.
     */
    public static List<Record> getRecordsFromShardWithLimit(AmazonDynamoDBStreams client,
                                                            String streamArn, String shardId,
                                                            ShardIteratorType iterType, String seqNum,
                                                            Integer limit) {
        GetShardIteratorRequest gsir = new GetShardIteratorRequest();
        gsir.setStreamArn(streamArn);
        gsir.setShardId(shardId);
        gsir.setShardIteratorType(iterType);
        gsir.setSequenceNumber(seqNum);
        String shardIter = client.getShardIterator(gsir).getShardIterator();

        // get records
        GetRecordsRequest grr = new GetRecordsRequest().withShardIterator(shardIter).withLimit(limit);
        List<Record> records = new ArrayList<>();
        GetRecordsResult result;
        do {
            result = client.getRecords(grr);
            records.addAll(result.getRecords());
            grr.setShardIterator(result.getNextShardIterator());
        } while (result.getNextShardIterator() != null && !result.getRecords().isEmpty());
        return records;
    }
}
