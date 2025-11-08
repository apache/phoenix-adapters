package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.MAX_NUM_CHANGES_AT_TIMESTAMP;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SHARD_ITERATOR_FORMAT;

public class GetShardIteratorService {

    public static Map<String, Object> getShardIterator(Map<String, Object> request,
                                                       String connectionUrl) {
        Map<String, Object> result = new HashMap<>();
        try (Connection conn = ConnectionUtil.getConnection(connectionUrl)) {
            String streamArn = (String) request.get(ApiMetadata.STREAM_ARN);
            String shardId = (String) request.get(ApiMetadata.SHARD_ID);
            String seqNum = (String) request.get(ApiMetadata.SEQUENCE_NUMBER);
            String shardIterType = (String) request.get(ApiMetadata.SHARD_ITERATOR_TYPE);
            String tableName = DdbAdapterCdcUtils.getTableNameFromStreamName(streamArn);
            String cdcObj = DdbAdapterCdcUtils.getCDCObjectNameFromStreamName(streamArn);
            String startSeqNum = getStartingSequenceNumber(conn, tableName, streamArn, shardId,
                    seqNum, shardIterType);
            String streamType = DdbAdapterCdcUtils.getStreamType(conn, tableName);
            result.put(ApiMetadata.SHARD_ITERATOR, String.format(SHARD_ITERATOR_FORMAT, tableName,
                    cdcObj, streamType, shardId, startSeqNum));
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
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
                long partitionStartTime = DdbAdapterCdcUtils.getPartitionStartTime(
                        conn, tableName, streamName, shardId);
                startSeqNum = String.valueOf(partitionStartTime * MAX_NUM_CHANGES_AT_TIMESTAMP);
                break;
        default:
                throw new ValidationException("Invalid shard iterator type: " + type);
        }
        return startSeqNum;
    }
}
