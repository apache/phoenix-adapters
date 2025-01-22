package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.util.CDCUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;

/**
 * Utility methods to implement DynamoDB Streams abstractions.
 * See <a href="https://salesforce.quip.com/vunDA0Fwedt5">DynamoDB Streams using Phoenix CDC</a>
 */
public class DDBShimCDCUtils {

    private static final String STREAM_NAME_QUERY
            = "SELECT STREAM_NAME FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE TABLE_NAME = '%s' AND STREAM_STATUS IN ('"
            + CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue() + "', '"
            + CDCUtil.CdcStreamStatus.ENABLING.getSerializedValue() + "')";

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
    public static String getStreamLabel(String streamName) {
        String timestamp = getCDCIndexTimestampFromStreamName(streamName);
        Date date = new Date(Long.parseLong(timestamp));
        DateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz");
        format.setTimeZone(TimeZone.getTimeZone("Etc/UTC"));
        return format.format(date);
    }

    /**
     * Parse CDC index creation time from streamName.
     */
    private static String getCDCIndexTimestampFromStreamName(String streamName) {
        // phoenix-cdc-stream-{tableName}-{cdc object name}-{cdc index timestamp}
        return streamName.substring(streamName.lastIndexOf("-")+1);
    }
}
