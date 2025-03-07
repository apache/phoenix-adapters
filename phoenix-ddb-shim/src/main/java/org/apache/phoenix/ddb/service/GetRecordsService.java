package org.apache.phoenix.ddb.service;

import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.Record;
import software.amazon.awssdk.services.dynamodb.model.StreamRecord;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.phoenix.ddb.bson.BsonDocumentToDdbAttributes;
import org.apache.phoenix.ddb.bson.CDCBsonUtil;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.ddb.utils.DQLUtils;
import org.apache.phoenix.ddb.utils.PhoenixShardIterator;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;
import java.util.ArrayList;

import static software.amazon.awssdk.services.dynamodb.model.OperationType.INSERT;
import static software.amazon.awssdk.services.dynamodb.model.OperationType.MODIFY;
import static software.amazon.awssdk.services.dynamodb.model.OperationType.REMOVE;

public class GetRecordsService {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetRecordsService.class);

    private static String GET_RECORDS_QUERY = "SELECT /*+ CDC_INCLUDE(PRE, POST) */ * " +
            " FROM %s WHERE PARTITION_ID() = ? " +
            " AND PHOENIX_ROW_TIMESTAMP() >= CAST(CAST(? AS BIGINT) AS TIMESTAMP) LIMIT ? ";

    private static final int MAX_GET_RECORDS_LIMIT = 1000;
    private static final String OLD_IMAGE = "OLD_IMAGE";
    private static final String NEW_IMAGE = "NEW_IMAGE";
    private static final String NEW_AND_OLD_IMAGES = "NEW_AND_OLD_IMAGES";

    /**
     * Notes:
     * 1. Keep track of previous timestamp and offset. If current record has the same timestamp,
     * increment offset to create new sequence number, otherwise use new timestamp with 0 offset.
     *
     * 2. Query 1 more than the limit set on the request. Even if the partition has split, do not
     * return null for nextShardIterator if there are more records to return.
     */
    public static GetRecordsResponse getRecords(GetRecordsRequest request, String connectionUrl) {
        PhoenixShardIterator pIter
                = new PhoenixShardIterator(request.shardIterator());
        List<Record> records = new ArrayList<>();
        long lastTs = pIter.getTimestamp();
        int lastOffset = pIter.getOffset() - 1;
        long partitionEndTime = 0L;
        boolean hasMore = false;
        Record record;
        try (Connection conn = DriverManager.getConnection(connectionUrl)) {
            List<PColumn> pkCols = PhoenixUtils.getPKColumns(conn, pIter.getTableName());
            int limit = (request.limit() != null && request.limit() > 0)
                    ? Math.min(request.limit(), MAX_GET_RECORDS_LIMIT)
                    : MAX_GET_RECORDS_LIMIT;
            // fetch an extra row in case we need to decide later whether partition is closed
            // and if there are more rows to be returned
            PreparedStatement ps = getPreparedStatement(conn, pIter, limit+1);
            ResultSet rs = ps.executeQuery();
            int count = 0;
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
            }
            partitionEndTime = DDBShimCDCUtils.getPartitionEndTime(conn, pIter);
            hasMore = rs.next();
            rs.close();
        } catch (Exception e) {
            // TODO: remove when phoenix bug is fixed
            if (e.getMessage().contains("Index 0 out of bounds for length 0")
                    || e.getCause().getMessage().contains("Index 0 out of bounds for length 0")) {
                LOGGER.info("Hit end of region, avoiding offset bug.");
            } else {
                throw new RuntimeException(e);
            }
        }
        // set next shard iterator by incrementing offset on the timestamp of the last record
        pIter.setNewSeqNum(lastTs, lastOffset+1);

        // if partition has closed and we returned all records, set nextShardIterator to null
        return GetRecordsResponse.builder()
                .records(records)
                .nextShardIterator((partitionEndTime > 0 && !hasMore)
                        ? null : pIter.toString()).build();
    }

    /**
     * Build the CDC query using the phoenix shard iterator
     * and return a PreparedStatement with values set.
     */
    private static PreparedStatement getPreparedStatement(Connection conn,
                                                   PhoenixShardIterator phoenixShardIterator,
                                                   Integer limit) throws SQLException {
        StringBuilder sb = new StringBuilder(String.format(
                GET_RECORDS_QUERY, phoenixShardIterator.getCdcObject()));
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
        LOGGER.info("Query for getRecords: {}", ps);
        LOGGER.info("Query Parameters: {}, {}, {}, {}", phoenixShardIterator.getPartitionId(),
                phoenixShardIterator.getTimestamp(), limit, phoenixShardIterator.getOffset());
        return ps;
    }

    /**
     * Build a Record object using a ResultSet cursor from a CDC query.
     * rs --> timestamp, pk1, (pk2), cdcJson
     */
    private static Record getStreamRecord(ResultSet rs, String streamType, List<PColumn> pkCols,
                                          String seqNum) throws SQLException, JsonProcessingException {
        StreamRecord.Builder streamRecord
                = StreamRecord.builder().streamViewType(streamType).sequenceNumber(seqNum);

        // creation DateTime
        long timestamp = rs.getDate(1).getTime();
        streamRecord.approximateCreationDateTime(new Date(timestamp).toInstant());

        //images
        String cdcJson = rs.getString(pkCols.size() + 2);
        RawBsonDocument[] imagesBsonDoc = CDCBsonUtil.getBsonDocsForCDCImages(cdcJson);
        switch (streamType) {
            case OLD_IMAGE:
                if (imagesBsonDoc[0] != null)
                    streamRecord.oldImage(BsonDocumentToDdbAttributes.getFullItem(imagesBsonDoc[0]));
                break;
            case NEW_IMAGE:
                if (imagesBsonDoc[1] != null)
                    streamRecord.newImage(BsonDocumentToDdbAttributes.getFullItem(imagesBsonDoc[1]));
                break;
            case NEW_AND_OLD_IMAGES:
                if (imagesBsonDoc[0] != null)
                    streamRecord.oldImage(BsonDocumentToDdbAttributes.getFullItem(imagesBsonDoc[0]));
                if (imagesBsonDoc[1] != null)
                    streamRecord.newImage(BsonDocumentToDdbAttributes.getFullItem(imagesBsonDoc[1]));
                break;
        }
        //always set keys
        RawBsonDocument image = (imagesBsonDoc[0] != null) ? imagesBsonDoc[0] : imagesBsonDoc[1];
        streamRecord.keys(DQLUtils.getKeyFromDoc(image, false, pkCols, null));

        // Record Name
        Record.Builder record = Record.builder().dynamodb(streamRecord.build());
        if (imagesBsonDoc[0] == null) {
            record.eventName(INSERT);
        } else if (imagesBsonDoc[1] == null) {
            record.eventName(REMOVE);
        } else {
            record.eventName(MODIFY);
        }
        return record.build();
    }
}
