package org.apache.phoenix.ddb.utils;

import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.CDCUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_NAME;
import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;

/**
 * Utility methods to implement DynamoDB Streams abstractions.
 * See <a href="https://salesforce.quip.com/vunDA0Fwedt5">DynamoDB Streams using Phoenix CDC</a>
 */
public class DDBShimCDCUtils {

    /**
     * Support these many different change records at the same timestamp with unique sequence number.
     */
    public static final int OFFSET_LENGTH = 5;
    public static final int MAX_NUM_CHANGES_AT_TIMESTAMP = (int) Math.pow(10, OFFSET_LENGTH);

    // shardIterator/<tableName>/<cdcObject>/<streamType>/<partitionID>/<startSeqNum>
    public static String SHARD_ITERATOR_FORMAT = "shardIterator/%s/%s/%s/%s/%s";
    public static String SHARD_ITERATOR_DELIM = "/";
    public static int SHARD_ITERATOR_NUM_PARTS = 6;
    // phoenix/cdc/stream/{tableName}/{cdc object name}/{cdc index timestamp}
    public static String STREAM_NAME_DELIM = "/";
    public static int STREAM_NAME_NUM_PARTS = 6;

    private static final String STREAM_NAME_QUERY
            = "SELECT STREAM_NAME FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_STATUS IN ('"
            + CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue() + "', '"
            + CDCUtil.CdcStreamStatus.ENABLING.getSerializedValue() + "')";

    private static final String STREAM_STATUS_QUERY
            = "SELECT STREAM_STATUS FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s'";

    private static final String PARTITION_START_TIME_QUERY
            = "SELECT PARTITION_START_TIME FROM " + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s' AND PARTITION_ID = '%s'";

    private static final String PARTITION_CLOSED_QUERY
            = "SELECT PARTITION_END_TIME FROM " + SYSTEM_CDC_STREAM_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_NAME = '%s' AND  PARTITION_ID = '%s'";

    /**
     * Return the KeySchema for the given PTable.
     */
    public static List<KeySchemaElement> getKeySchema(PTable table) {
        List<KeySchemaElement> keySchema = new ArrayList<>();
        List<PColumn> pkCols = table.getPKColumns();
        keySchema.add(KeySchemaElement.builder().attributeName(pkCols.get(0).getName().getString()).keyType(KeyType.HASH).build());
        if (pkCols.size() == 2) {
            keySchema.add(KeySchemaElement.builder().attributeName(pkCols.get(1).getName().getString()).keyType(KeyType.RANGE).build());
        }
        return keySchema;
    }

    public static List<Map<String, Object>> getKeySchemaForRest(PTable table) {
        List<Map<String, Object>> keySchemaList = new ArrayList<>();
        List<PColumn> pkCols = table.getPKColumns();
        Map<String, Object> hashKeyMap = new HashMap<>();
        hashKeyMap.put("AttributeName", pkCols.get(0).getName().getString());
        hashKeyMap.put("KeyType", KeyType.HASH);
        keySchemaList.add(hashKeyMap);
        if (pkCols.size() == 2) {
            Map<String, Object> sortKeyMap = new HashMap<>();
            sortKeyMap.put("AttributeName", pkCols.get(1).getName().getString());
            sortKeyMap.put("KeyType", KeyType.RANGE);
            keySchemaList.add(sortKeyMap);
        }
        return keySchemaList;
    }


    /**
     * Get the STREAM_STATUS from SYSTEM.CDC_STREAM_STATUS for the given tableName and streamName.
     */
    public static String getStreamStatus(Connection conn, String tableName, String streamName)
            throws SQLException {
        String query = String.format(STREAM_STATUS_QUERY, tableName, streamName);
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getString(1);
        } else {
            throw new SQLException("No stream was found with streamName = " + streamName);
        }
    }

    /**
     * Get the STREAM_NAME in ENABLED/ENABLING status from SYSTEM.CDC_STREAM_STATUS for the given tableName.
     */
    public static String getEnabledStreamName(Connection conn, String tableName) throws SQLException {
        String query = String.format(STREAM_NAME_QUERY, tableName);
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getString(1);
        } else {
            return null;
        }
    }

    /**
     * Return human-readable format for index creation timestamp as Stream Label.
     */
    public static String getStreamLabel(long timestamp) {
        Date date = new Date(timestamp);
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz");
        format.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
        return format.format(date);
    }

    /**
     * Parse tableName from streamName.
     */
    public static String getTableNameFromStreamName(String streamName) {
        // phoenix/cdc/stream/{tableName}/{cdc object name}/{cdc index timestamp}
        return getStreamNameComponent(streamName, 3);
    }

    /**
     * Parse CDC Object name from streamName.
     */
    public static String getCDCObjectNameFromStreamName(String streamName) {
        // phoenix/cdc/stream/{tableName}/{cdc object name}/{cdc index timestamp}
        return getStreamNameComponent(streamName, 4);
    }

    /**
     * Parse CDC index creation time from streamName.
     */
    public static long getCDCIndexTimestampFromStreamName(String streamName) {
        // phoenix/cdc/stream/{tableName}/{cdc object name}/{cdc index timestamp}
        return Long.parseLong(getStreamNameComponent(streamName, 5));
    }

    /**
     * Return the stream type for the given table stored in the SCHEMA_VERSION column of the ptable.
     */
    public static String getStreamType(Connection conn, String tableName) throws SQLException {
        PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
        PTable table = phoenixConnection.getTable(
                new PTableKey(phoenixConnection.getTenantId(), tableName));
        return table.getSchemaVersion();
    }

    /**
     * Get the start time for the given partition from SYSTEM.CDC_STREAM
     */
    public static long getPartitionStartTime(Connection conn, String tableName,
                                             String streamName, String partitionId)
            throws SQLException {
        String query = String.format(PARTITION_START_TIME_QUERY, tableName, streamName, partitionId);
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getLong(1);
        } else {
            throw new SQLException("Could not find partition for id: " + partitionId);
        }
    }

    /**
     * Return true if a partition was closed after a split.
     */
    public static long getPartitionEndTime(Connection conn, PhoenixShardIterator pIter)
            throws SQLException {
        String streamName = getEnabledStreamName(conn, pIter.getTableName());
        String query = String.format(PARTITION_CLOSED_QUERY, pIter.getTableName(), streamName, pIter.getPartitionId());
        ResultSet rs = conn.createStatement().executeQuery(query);
        if (rs.next()) {
            return rs.getLong(1);
        } else {
            throw new SQLException("Could not find partition for id: " + pIter.getPartitionId());
        }
    }

    /**
     * Build a sequence number of the form <timestamp><offset>.
     * offset should be 0-padded for OFFSET_LENGTH
     */
    public static String getSequenceNumber(long timestamp, int offset) {
        return timestamp + String.format("%0" + OFFSET_LENGTH + "d", offset);
    }

    /**
     * Parse the provided stream name and return a particular component.
     * @param streamName stream name
     * @param index zero based index
     * @return
     */
    private static String getStreamNameComponent(String streamName, int index) {
        String[] parts = streamName.split(STREAM_NAME_DELIM);
        if (parts.length != STREAM_NAME_NUM_PARTS) {
            throw new IllegalArgumentException("Stream Name format is not correct: " + streamName);
        }
        return parts[index];
    }
}
