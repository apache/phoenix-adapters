package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.service.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.ddb.utils.DDBShimCDCUtils.MAX_NUM_CHANGES_AT_TIMESTAMP;
import static org.apache.phoenix.ddb.utils.DDBShimCDCUtils.SHARD_ITERATOR_FORMAT;

public class GetShardIteratorService {

    public static Map<String, Object> getShardIterator(Map<String, Object> request,
                                                       String connectionUrl) {
        Map<String, Object> result = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            String streamArn = (String) request.get(ApiMetadata.STREAM_ARN);
            String shardId = (String) request.get(ApiMetadata.SHARD_ID);
            String seqNum = (String) request.get(ApiMetadata.SEQUENCE_NUMBER);
            String shardIterType = (String) request.get(ApiMetadata.SHARD_ITERATOR_TYPE);
            String tableName = DDBShimCDCUtils.getTableNameFromStreamName(streamArn);
            String cdcObj = DDBShimCDCUtils.getCDCObjectNameFromStreamName(streamArn);
            String startSeqNum = getStartingSequenceNumber(conn, tableName, streamArn, shardId,
                    seqNum, shardIterType);
            String streamType = DDBShimCDCUtils.getStreamType(conn, tableName);
            result.put(ApiMetadata.SHARD_ITERATOR, String.format(SHARD_ITERATOR_FORMAT, tableName,
                    cdcObj, streamType, shardId, startSeqNum));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private static String getStartingSequenceNumber(Connection conn, String tableName,
                                                    String streamName, String shardId,
                                                    String seqNum, String type)
            throws SQLException {
        String startSeqNum = null;
        switch (type) {
            case "AT_SEQUENCE_NUMBER" :
                startSeqNum = seqNum;
                break;
            case "AFTER_SEQUENCE_NUMBER":
                startSeqNum = String.valueOf(Long.parseLong(seqNum) + 1);
                break;
            case "LATEST":
                // new records only i.e. use current time.
                startSeqNum = String.valueOf(EnvironmentEdgeManager.currentTimeMillis()
                        * MAX_NUM_CHANGES_AT_TIMESTAMP);
                break;
            case "TRIM_HORIZON":
                // Oldest available sequence number in the shard, we will use shard's start sequence number
                long partitionStartTime = DDBShimCDCUtils.getPartitionStartTime(
                        conn, tableName, streamName, shardId);
                startSeqNum = String.valueOf(partitionStartTime * MAX_NUM_CHANGES_AT_TIMESTAMP);
                break;
        }
        return startSeqNum;
    }
}
