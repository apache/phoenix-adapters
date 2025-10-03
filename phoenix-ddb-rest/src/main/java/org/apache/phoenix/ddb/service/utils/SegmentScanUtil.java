package org.apache.phoenix.ddb.service.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.hbase.util.Pair;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;

/**
 * Helper methods for the Segment Scan functionality.
 */
public class SegmentScanUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SegmentScanUtil.class);
    public static final String SEGMENT_SCAN_METADATA_TABLE_NAME = "PHOENIX_DDB_SEGMENT_RANGE";
    private static final String PHOENIX_DDB_SEGMENT_RANGE_DDL =
            "CREATE TABLE IF NOT EXISTS " + SEGMENT_SCAN_METADATA_TABLE_NAME + "\n("
                    + "TABLE_NAME VARCHAR NOT NULL,\n" + "TOTAL_SEGMENTS INTEGER NOT NULL,\n"
                    + "SEGMENT_START_KEYS VARCHAR ARRAY,\n" + "SEGMENT_END_KEYS VARCHAR ARRAY,\n"
                    + "SCAN_COUNT INTEGER\n"
                    + "CONSTRAINT PK PRIMARY KEY (TABLE_NAME, TOTAL_SEGMENTS))\n"
                    + "TTL=5400, UPDATE_CACHE_FREQUENCY=172800000";
    private static final String PHOENIX_TOTAL_SEGMENTS_QUERY =
            "SELECT SCAN_START_KEY(), SCAN_END_KEY() FROM %s WHERE TOTAL_SEGMENTS() = %d";
    private static final String SEGMENT_SCAN_RANGE_METADATA_QUERY =
            "SELECT SEGMENT_START_KEYS, SEGMENT_END_KEYS FROM " + SEGMENT_SCAN_METADATA_TABLE_NAME
                    + " WHERE TABLE_NAME = ? AND TOTAL_SEGMENTS = ?";
    private static final String SEGMENT_SCAN_RANGE_METADATA_UPDATE =
            "UPSERT INTO " + SEGMENT_SCAN_METADATA_TABLE_NAME
                    + " VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE SCAN_COUNT = SCAN_COUNT + 1";

    /**
     * Initialize the metadata table for Segment Scans.
     */
    public static void createSegmentScanMetadataTable(String url) throws SQLException {
        try (Connection connection = DriverManager.getConnection(url)) {
            connection.createStatement().execute(PHOENIX_DDB_SEGMENT_RANGE_DDL);
        }
    }

    /**
     * Query the segment scan metadata table and return the start and end keys for the provided segment.
     * Throws SQLException if no row is found for (tableName, totalSegments)
     *
     * @param connection    Connection to Phoenix
     * @param tableName     Table for which scan is being performed
     * @param totalSegments Total number of segments for the given table
     * @param segmentNumber Segment number for which start and end keys are required
     * @return
     */
    public static ScanSegmentInfo getSegmentScanRange(Connection connection, String tableName,
            int totalSegments, int segmentNumber) throws SQLException {
        PreparedStatement pstmt =
                connection.prepareStatement(SEGMENT_SCAN_RANGE_METADATA_QUERY);
        pstmt.setString(1, tableName);
        pstmt.setInt(2, totalSegments);
        ResultSet rs = pstmt.executeQuery();
        if (rs.next()) {
            String[] startKeys = (String[]) rs.getArray(1).getArray();
            String[] endKeys = (String[]) rs.getArray(2).getArray();
            return extractSegmentBoundaryFromArray(startKeys, endKeys, tableName, totalSegments,
                    segmentNumber);
        } else {
            String err = "No segment scan ranges found for table: " + tableName
                    + ", total segments: " + totalSegments;
            LOGGER.error(err);
            throw new SQLException(err);
        }
    }

    /**
     * Query the total segments for the provided table from Phoenix.
     * Colaesce the segment ranges to array and try to update the segment scan metadata table atomically.
     * Use the returned segment ranges from atomic update to extract the boundaries for the requested segmentNumber.
     *
     * @param connection    Connection to Phoenix
     * @param tableName     Table for which scan is being performed
     * @param totalSegments Total number of segments for the given table
     * @param segmentNumber Segment number for which start and end keys are required
     * @return
     */
    public static ScanSegmentInfo updateAndGetSegmentScanRange(Connection connection, String tableName,
            int totalSegments, int segmentNumber) throws SQLException {
        connection.setAutoCommit(true);
        Pair<String[], String[]> segmentStartKeysAndEndKeys =
                executeTotalSegmentsQuery(connection, tableName, totalSegments);
        String[] segmentStartKeys = segmentStartKeysAndEndKeys.getFirst();
        String[] segmentEndKeys = segmentStartKeysAndEndKeys.getSecond();
        PreparedStatement pstmt =
                connection.prepareStatement(SEGMENT_SCAN_RANGE_METADATA_UPDATE);
        pstmt.setString(1, tableName);
        pstmt.setInt(2, totalSegments);
        pstmt.setArray(3, connection.createArrayOf("VARCHAR", segmentStartKeys));
        pstmt.setArray(4, connection.createArrayOf("VARCHAR", segmentEndKeys));
        pstmt.setInt(5, 0);
        Pair<Integer, ResultSet> resultPair =
                pstmt.unwrap(PhoenixPreparedStatement.class).executeAtomicUpdateReturnRow();
        ResultSet rs = resultPair.getSecond();
        if (rs == null) {
            throw new SQLException(
                    "No segment scan ranges were returned after metadata update for table: "
                            + tableName + ", total segments: " + totalSegments);
        }
        String[] startKeys = (String[]) rs.getArray(3).getArray();
        String[] endKeys = (String[]) rs.getArray(4).getArray();
        return extractSegmentBoundaryFromArray(startKeys, endKeys, tableName, totalSegments,
                segmentNumber);
    }

    /**
     * Execute the TOTAL_SEGMENTS query and return the start and end keys for each segment as String arrays.
     */
    private static Pair<String[], String[]> executeTotalSegmentsQuery(Connection connection,
            String tableName, int totalSegments) throws SQLException {
        List<String> segmentStartKeys = new ArrayList<>();
        List<String> segmentEndKeys = new ArrayList<>();
        int i = 0;
        String totalSegmentsQuery =
                String.format(PHOENIX_TOTAL_SEGMENTS_QUERY, "DDB.\"" + tableName + "\"", totalSegments);
        ResultSet rs = connection.createStatement().executeQuery(totalSegmentsQuery);
        while (rs.next()) {
            byte[] startKey = rs.getBytes(1);
            byte[] endKey = rs.getBytes(2);
            startKey = startKey == null ? new byte[0] : startKey;
            endKey = endKey == null ? new byte[0] : endKey;
            segmentStartKeys.add(Base64.getEncoder().encodeToString(startKey));
            segmentEndKeys.add(Base64.getEncoder().encodeToString(endKey));
            i++;
        }
        if (i == 0) {
            throw new SQLException("No segment scan ranges generated for table: " + tableName
                    + ", total segments: " + totalSegments);
        }
        return new Pair<>(segmentStartKeys.toArray(new String[0]),
                segmentEndKeys.toArray(new String[0]));
    }

    /**
     * Extract the start and end keys byte array for the given segment number from the given String arrays.
     */
    private static ScanSegmentInfo extractSegmentBoundaryFromArray(String[] startKeys,
            String[] endKeys, String tableName, int totalSegments, int segmentNumber) {
        // phoenix created fewer segments, maybe because number of regions were lesser than total_segments.
        if (segmentNumber >= startKeys.length && segmentNumber >= endKeys.length) {
            return new ScanSegmentInfo(tableName, totalSegments, segmentNumber, null, null);
        }
        byte[] startKey = startKeys[segmentNumber] == null
                ? new byte[0] : Base64.getDecoder().decode(startKeys[segmentNumber]);
        byte[] endKey = endKeys[segmentNumber] == null
                ? new byte[0] : Base64.getDecoder().decode(endKeys[segmentNumber]);
        return new ScanSegmentInfo(tableName, totalSegments, segmentNumber, startKey, endKey);
    }
}
