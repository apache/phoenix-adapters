package org.apache.phoenix.ddb.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.ddb.utils.DDBShimCDCUtils.MAX_NUM_CHANGES_AT_TIMESTAMP;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;

public class DescribeStreamService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeStreamService.class);
    private static final int MAX_LIMIT = 100;

    private static final String DESCRIBE_STREAM_QUERY
            = "SELECT PARTITION_ID, PARENT_PARTITION_ID, PARTITION_START_TIME, PARTITION_END_TIME FROM "
            + SYSTEM_CDC_STREAM_NAME + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s' ";

    public static Map<String, Object> describeStream(Map<String, Object> request, String connectionUrl) {
        String streamName = (String) request.get(ApiMetadata.STREAM_ARN);
        String exclusiveStartShardId = (String) request.get(ApiMetadata.EXCLUSIVE_START_SHARD_ID);
        Integer limit = (Integer) request.getOrDefault(ApiMetadata.LIMIT, MAX_LIMIT);
        String tableName = DDBShimCDCUtils.getTableNameFromStreamName(streamName);
        Map<String, Object> streamDesc;
        try (Connection conn = ConnectionUtil.getConnection(connectionUrl)) {
            streamDesc = getStreamDescriptionObject(conn, tableName, streamName);
            String streamStatus = DDBShimCDCUtils.getStreamStatus(conn, tableName, streamName);
            streamDesc.put(ApiMetadata.STREAM_STATUS, streamStatus);
            // query partitions only if stream is ENABLED
            if (CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue().equals(streamStatus)) {
                StringBuilder sb = new StringBuilder(String.format(DESCRIBE_STREAM_QUERY, tableName, streamName));
                if (!StringUtils.isEmpty(exclusiveStartShardId)) {
                    sb.append(" AND PARTITION_ID > '");
                    sb.append(exclusiveStartShardId);
                    sb.append("'");
                }
                sb.append(" LIMIT ");
                sb.append(limit);
                LOGGER.debug("Describe Stream Query: {}", sb);
                List<Map<String, Object>> shards = new ArrayList<>();
                String lastEvaluatedShardId = null;
                ResultSet rs = conn.createStatement().executeQuery(sb.toString());
                while (rs.next()) {
                    Map<String, Object> shard = getShardMetadata(rs);
                    shards.add(shard);
                    lastEvaluatedShardId = (String) shard.get(ApiMetadata.SHARD_ID);
                }
                streamDesc.put(ApiMetadata.SHARDS, shards);
                streamDesc.put(ApiMetadata.LAST_EVALUATED_SHARD_ID, lastEvaluatedShardId);
            }
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
        Map<String, Object> result = new HashMap<>();
        result.put(ApiMetadata.STREAM_DESCRIPTION, streamDesc);
        return result;
    }

    /**
     * Return a StreamDescription object for the given tableName and streamName.
     * Populate all attributes except the list of the shards.
     */
    private static Map<String, Object> getStreamDescriptionObject(Connection conn,
                                                                        String tableName,
                                                                        String streamName)
            throws SQLException {
        PhoenixConnection pconn = conn.unwrap(PhoenixConnection.class);
        PTable table = pconn.getTable(tableName);
        Map<String, Object> streamDesc = new HashMap<>();
        streamDesc.put(ApiMetadata.STREAM_ARN, streamName);
        streamDesc.put(ApiMetadata.TABLE_NAME, PhoenixUtils.getTableNameFromFullName(tableName, false));
        long creationTS = DDBShimCDCUtils.getCDCIndexTimestampFromStreamName(streamName);
        streamDesc.put(ApiMetadata.STREAM_LABEL, DDBShimCDCUtils.getStreamLabel(streamName));
        streamDesc.put(ApiMetadata.STREAM_VIEW_TYPE, table.getSchemaVersion());
        streamDesc.put(ApiMetadata.CREATION_REQUEST_DATE_TIME,
                BigDecimal.valueOf(creationTS).movePointLeft(3));
        streamDesc.put(ApiMetadata.KEY_SCHEMA, DDBShimCDCUtils.getKeySchemaForRest(table));
        return streamDesc;
    }

    /**
     * Build a Shard object using a ResultSet cursor from a query on SYSTEM.CDC_STREAM.
     */
    private static Map<String, Object> getShardMetadata(ResultSet rs) throws SQLException {
        // rs --> id, parentId, startTime, endTime
        Map<String, Object> shard = new HashMap<>();
        // shard id
        shard.put(ApiMetadata.SHARD_ID, rs.getString(1));
        // parent shard id
        if (rs.getString(2) != null) {
            shard.put(ApiMetadata.PARENT_SHARD_ID, rs.getString(2));
        }
        // start sequence number
        Map<String, Object> seqNumRange = new HashMap<>();
        seqNumRange.put(ApiMetadata.STARTING_SEQUENCE_NUMBER, String.valueOf(rs.getLong(3) * MAX_NUM_CHANGES_AT_TIMESTAMP));
        // end sequence number
        if (rs.getLong(4) > 0) {
            seqNumRange.put(ApiMetadata.ENDING_SEQUENCE_NUMBER, String.valueOf(((rs.getLong(4)+1) * MAX_NUM_CHANGES_AT_TIMESTAMP) - 1));
        }
        shard.put(ApiMetadata.SEQUENCE_NUMBER_RANGE, seqNumRange);
        return shard;
    }
}
