package org.apache.phoenix.ddb.service;

import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.SequenceNumberRange;
import software.amazon.awssdk.services.dynamodb.model.Shard;
import software.amazon.awssdk.services.dynamodb.model.StreamDescription;
import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.phoenix.ddb.utils.DDBShimCDCUtils.MAX_NUM_CHANGES_AT_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;

public class DescribeStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeStreamService.class);

    private static String DESCRIBE_STREAM_QUERY
            = "SELECT PARTITION_ID, PARENT_PARTITION_ID, PARTITION_START_TIME, PARTITION_END_TIME FROM "
            + SYSTEM_CDC_STREAM_NAME + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s' ";

    public static DescribeStreamResponse describeStream(DescribeStreamRequest request, String connectionUrl) {
        String streamName = request.streamArn();
        String exclusiveStartShardId = request.exclusiveStartShardId();
        Integer limit = request.limit();
        String tableName = DDBShimCDCUtils.getTableNameFromStreamName(streamName);
        StreamDescription.Builder streamDesc;
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            streamDesc = getStreamDescriptionObject(conn, tableName, streamName);
            String streamStatus = DDBShimCDCUtils.getStreamStatus(conn, tableName, streamName);
            streamDesc.streamStatus(streamStatus);
            // query partitions only if stream is ENABLED
            if (CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue().equals(streamStatus)) {
                StringBuilder sb = new StringBuilder(String.format(DESCRIBE_STREAM_QUERY, tableName, streamName));
                if (!StringUtils.isEmpty(exclusiveStartShardId)) {
                    sb.append(" AND PARTITION_ID > ' ");
                    sb.append(exclusiveStartShardId);
                    sb.append("'");
                }
                if (limit != null && limit > 0) {
                    sb.append(" LIMIT ");
                    sb.append(limit);
                }
                LOGGER.info("Describe Stream Query: " + sb);
                List<Shard> shards = new ArrayList<>();
                String lastEvaluatedShardId = null;
                ResultSet rs = conn.createStatement().executeQuery(sb.toString());
                while (rs.next()) {
                    Shard shard = getShardMetadata(rs);
                    shards.add(shard);
                    lastEvaluatedShardId = shard.shardId();
                }
                streamDesc.shards(shards);
                streamDesc.lastEvaluatedShardId(lastEvaluatedShardId);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return DescribeStreamResponse.builder().streamDescription(streamDesc.build()).build();
    }

    /**
     * Return a StreamDescription object for the given tableName and streamName.
     * Populate all attributes except the list of the shards.
     */
    private static StreamDescription.Builder getStreamDescriptionObject(Connection conn,
                                                                String tableName,
                                                                String streamName)
            throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getTable(tableName);
        StreamDescription.Builder streamDesc = StreamDescription.builder();
        streamDesc.streamArn(streamName);
        streamDesc.tableName(tableName);
        long creationTS = DDBShimCDCUtils.getCDCIndexTimestampFromStreamName(streamName);
        streamDesc.streamLabel(DDBShimCDCUtils.getStreamLabel(creationTS));
        streamDesc.streamViewType(table.getSchemaVersion());
        streamDesc.creationRequestDateTime(new Date(creationTS).toInstant());
        streamDesc.keySchema(DDBShimCDCUtils.getKeySchema(table));
        return streamDesc;
    }

    /**
     * Build a Shard object using a ResultSet cursor from a query on SYSTEM.CDC_STREAM.
     */
    private static Shard getShardMetadata(ResultSet rs) throws SQLException {
        // rs --> id, parentId, startTime, endTime
        Shard.Builder shard = Shard.builder();
        // shard id
        shard.shardId(rs.getString(1));
        // parent shard id
        if (rs.getString(2) != null) {
            shard.parentShardId(rs.getString(2));
        }
        // start sequence number
        SequenceNumberRange.Builder seqNumRange = SequenceNumberRange.builder();
        seqNumRange.startingSequenceNumber(String.valueOf(rs.getLong(3) * MAX_NUM_CHANGES_AT_TIMESTAMP));
        // end sequence number
        if (rs.getLong(4) > 0) {
            seqNumRange.endingSequenceNumber(String.valueOf(((rs.getLong(4)+1) * MAX_NUM_CHANGES_AT_TIMESTAMP) - 1));
        }
        shard.sequenceNumberRange(seqNumRange.build());
        return shard.build();
    }
}
