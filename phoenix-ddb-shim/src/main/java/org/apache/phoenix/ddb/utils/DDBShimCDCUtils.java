package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;

import java.sql.SQLException;

/**
 * Utility methods to implement DynamoDB Streams abstractions.
 * See <a href="https://salesforce.quip.com/vunDA0Fwedt5">DynamoDB Streams using Phoenix CDC</a>
 *
 * Each 24h time window starting from the time CDC is enabled will represent a stream.
 * Each 2h sub-window within a stream will represent a shard.
 * stream arn format --> stream-<tableName>-<streamType>-<startTime>-<endTime>
 * shardId format --> shard-<tableName>-<streamType>-<startTime>-<endTime>
 */
public class DDBShimCDCUtils {

    private static final String STREAM_ARN_FORMAT = "stream-%s-%s-%d-%d";
    public static final long DAY_MS = 24 * 60 * 60 * 1000L; //24h in ms

    /**
     * Return the ARN of the latest/active stream on the given table.
     */
    public static String getLatestStreamARN(PhoenixConnection pconn, PTable table)
            throws SQLException {
        String cdcIndexName = CDCUtil.getCDCIndexName("CDC_" + table.getName());
        PTable cdcIndexTable = pconn.getTable(cdcIndexName);
        long[] window = getTimeWindow(cdcIndexTable.getTimeStamp(),
                                                System.currentTimeMillis());
        return String.format(STREAM_ARN_FORMAT,
                table.getName(), table.getSchemaVersion(), window[0], window[1]);
    }

    /**
     * Return the Label of the latest/active stream on the given table.
     * We will use the start timestamp of the stream's time window as the Label.
     */
    public static String getLatestStreamLabel(PhoenixConnection pconn, PTable table)
            throws SQLException {
        String cdcIndexName = CDCUtil.getCDCIndexName("CDC_" + table.getName());
        PTable cdcIndexTable = pconn.getTable(cdcIndexName);
        long[] window = getTimeWindow(cdcIndexTable.getTimeStamp(),
                                                System.currentTimeMillis());
        return String.valueOf(window[0]);
    }

    /**
     * Calculate the 24h time window [start, end] for given timestamp based on a CDC creation time.
     * Both start and end timestamps are inclusive in this window.
     */
    public static long[] getTimeWindow(long cdcEnabledTime, long time) {
        long start, end;
        long n = (time - cdcEnabledTime) / DAY_MS;
        long rem = (time - cdcEnabledTime) % DAY_MS;
        n = (rem == 0) ? n-1 : n;
        start = cdcEnabledTime + (n * DAY_MS) + 1;
        end = cdcEnabledTime + ((n+1) * DAY_MS);
        return new long[]{start, end};
    }
}
