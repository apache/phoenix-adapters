package org.apache.phoenix.ddb.service;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.ddb.utils.PhoenixShardIterator;
import org.apache.phoenix.ddb.utils.PhoenixStreamRecord;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.schema.PColumn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.ddb.utils.CommonServiceUtils.isCauseMessageAvailable;

public class GetRecordsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsService.class);

    private static String GET_RECORDS_QUERY = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * " +
            " FROM %s.\"%s\" WHERE PARTITION_ID() = ? " +
            " AND PHOENIX_ROW_TIMESTAMP() >= CAST(CAST(? AS BIGINT) AS TIMESTAMP) LIMIT ? ";

    private static final int MAX_GET_RECORDS_LIMIT = 100;
    private static final String OLD_IMAGE = "OLD_IMAGE";
    private static final String NEW_IMAGE = "NEW_IMAGE";
    private static final String NEW_AND_OLD_IMAGES = "NEW_AND_OLD_IMAGES";
    private static final String SERVICE = "Service";
    private static final String PRINCIPAL_ID = "phoenix/hbase";

    /**
     * Notes:
     * 1. Keep track of previous timestamp and offset. If current record has the same timestamp,
     * increment offset to create new sequence number, otherwise use new timestamp with 0 offset.
     *
     * 2. Query 1 more than the limit set on the request. Even if the partition has split, do not
     * return null for nextShardIterator if there are more records to return.
     */
    public static Map<String, Object> getRecords(Map<String, Object> request, String connectionUrl) {
        PhoenixShardIterator pIter
                = new PhoenixShardIterator((String) request.get(ApiMetadata.SHARD_ITERATOR));
        Integer requestLimit = (Integer) request.get(ApiMetadata.LIMIT);
        List<Map<String, Object>> records = new ArrayList<>();
        long lastTs = pIter.getTimestamp();
        int lastOffset = pIter.getOffset() - 1;
        long partitionEndTime = 0L;
        boolean hasMore = false;
        Map<String, Object> record = new HashMap<>();
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            String tableName = pIter.getTableName();
            List<PColumn> pkCols = PhoenixUtils.getPKColumns(conn,
                    tableName.startsWith("DDB.") ? tableName.split("DDB.")[1] : tableName);
            int limit = (requestLimit != null && requestLimit > 0)
                    ? Math.min(requestLimit, MAX_GET_RECORDS_LIMIT)
                    : MAX_GET_RECORDS_LIMIT;
            // fetch an extra row in case we need to decide later whether partition is closed
            // and if there are more rows to be returned
            PreparedStatement ps = getPreparedStatement(conn, pIter, limit+1);
            ResultSet rs = ps.executeQuery();
            int count = 0;
            int bytesSize = 0;
            while (count < limit && rs.next()) {
                long ts = rs.getDate(1).getTime();
                if (ts == lastTs) {
                    // change at same timestamp as previous one, increment offset
                    lastOffset += 1;
                } else {
                    lastTs = ts;
                    lastOffset=0;
                }
                record = getStreamRecord(rs, pIter.getStreamType(), pkCols,
                        DDBShimCDCUtils.getSequenceNumber(lastTs, lastOffset));
                records.add(record);
                count++;
                bytesSize +=
                        (int) rs.unwrap(PhoenixResultSet.class).getCurrentRow().getSerializedSize();
                if (bytesSize >= ApiMetadata.MAX_BYTES_SIZE) {
                    break;
                }
            }
            partitionEndTime = DDBShimCDCUtils.getPartitionEndTime(conn, pIter);
            hasMore = rs.next();
            rs.close();
        } catch (Exception e) {
            // TODO: remove when phoenix bug is fixed
            if ((e.getMessage() != null && e.getMessage().contains(
                    "Index 0 out of bounds for length 0"))
                    || (e.getMessage() != null && e.getMessage().contains(
                            "Index: 0, Size: 0"))
                    || (isCauseMessageAvailable(e) && e.getCause().getMessage().contains(
                            "Index 0 out of bounds for length 0"))
                    || (isCauseMessageAvailable(e) && e.getCause().getMessage().contains(
                            "Index: 0, Size: 0"))) {
                LOGGER.debug("Hit end of region, avoiding offset bug.");
            } else {
                throw new RuntimeException(e);
            }
        }
        // set next shard iterator by incrementing offset on the timestamp of the last record
        pIter.setNewSeqNum(lastTs, lastOffset+1);

        // if partition has closed and we returned all records, set nextShardIterator to null
        Map<String, Object> result = new HashMap<>();
        result.put(ApiMetadata.RECORDS, records);
        result.put(ApiMetadata.NEXT_SHARD_ITERATOR, (partitionEndTime > 0 && !hasMore) ? null : pIter.toString());
        return result;
    }

    /**
     * Build the CDC query using the phoenix shard iterator
     * and return a PreparedStatement with values set.
     */
    private static PreparedStatement getPreparedStatement(Connection conn,
                                                          PhoenixShardIterator phoenixShardIterator,
                                                          Integer limit) throws SQLException {
        StringBuilder sb = new StringBuilder(String.format(
                GET_RECORDS_QUERY, "DDB", phoenixShardIterator.getCdcObject()));
        if (phoenixShardIterator.getOffset() > 0) {
            sb.append(" OFFSET ? ");
        }
        PreparedStatement ps = conn.prepareStatement(sb.toString());
        ps.setString(1, phoenixShardIterator.getPartitionId());
        ps.setLong(2, phoenixShardIterator.getTimestamp());
        ps.setInt(3, limit);
        if (phoenixShardIterator.getOffset() > 0) {
            ps.setInt(4, phoenixShardIterator.getOffset());
        }
        LOGGER.debug("Query for getRecords: {}, Parameters: {}, {}, {}, {}",
                ps, phoenixShardIterator.getPartitionId(), phoenixShardIterator.getTimestamp(),
                limit, phoenixShardIterator.getOffset());
        return ps;
    }

    /**
     * Build a Record object using a ResultSet cursor from a CDC query.
     * rs --> timestamp, pk1, (pk2), cdcJson
     */
    private static Map<String, Object> getStreamRecord(ResultSet rs, String streamType, List<PColumn> pkCols,
                                          String seqNum) throws SQLException, JsonProcessingException {
        Map<String, Object> streamRecord = new HashMap<>();
        streamRecord.put(ApiMetadata.STREAM_VIEW_TYPE, streamType);
        streamRecord.put(ApiMetadata.SEQUENCE_NUMBER, seqNum);

        // creation DateTime
        long timestamp = rs.getDate(1).getTime();
        streamRecord.put(ApiMetadata.APPROXIMATE_CREATION_DATE_TIME,
                BigDecimal.valueOf(timestamp).movePointLeft(3));

        //images
        String cdcJson = rs.getString(pkCols.size() + 2);
        PhoenixStreamRecord phoenixStreamRecord = new PhoenixStreamRecord(cdcJson, pkCols);
        switch (streamType) {
            case OLD_IMAGE:
                streamRecord.put(ApiMetadata.OLD_IMAGE, phoenixStreamRecord.getPreImage());
                break;
            case NEW_IMAGE:
                streamRecord.put(ApiMetadata.NEW_IMAGE, phoenixStreamRecord.getPostImage());
                break;
            case NEW_AND_OLD_IMAGES:
                streamRecord.put(ApiMetadata.OLD_IMAGE, phoenixStreamRecord.getPreImage());
                streamRecord.put(ApiMetadata.NEW_IMAGE, phoenixStreamRecord.getPostImage());
                break;
        }
        //always set keys
        streamRecord.put(ApiMetadata.KEYS, phoenixStreamRecord.getKeys());

        //size
        streamRecord.put(ApiMetadata.SIZE_BYTES, rs.unwrap(PhoenixResultSet.class).getCurrentRow().getSerializedSize());

        // Record Name
        Map<String, Object> record = new HashMap<>();
        record.put(ApiMetadata.DYNAMODB, streamRecord);
        if (!phoenixStreamRecord.hasPreImage()) {
            record.put(ApiMetadata.EVENT_NAME, "INSERT");
        } else if (!phoenixStreamRecord.hasPostImage()) {
            record.put(ApiMetadata.EVENT_NAME, "REMOVE");
            if (phoenixStreamRecord.isTTLDeleteEvent()) {
                Map<String, Object> userIdentity = new HashMap<>();
                userIdentity.put(ApiMetadata.TYPE, SERVICE);
                userIdentity.put(ApiMetadata.PRINCIPAL_ID, PRINCIPAL_ID);
                record.put(ApiMetadata.USER_IDENTITY, userIdentity);
            }
        } else {
            record.put(ApiMetadata.EVENT_NAME, "MODIFY");
        }
        return record;
    }
}
