package org.apache.phoenix.ddb.utils;


import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.OFFSET_LENGTH;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SHARD_ITERATOR_DELIM;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SHARD_ITERATOR_FORMAT;
import static org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils.SHARD_ITERATOR_NUM_PARTS;

/**
 * Class to represent a shard iterator for Phoenix CDC queries.
 * Format: shardIterator-<tableName>-<cdcObject>-<streamType>-<partitionID>-<startSeqNum>
 */
public class PhoenixShardIterator {

    private final String tableName;
    private final String cdcObject;

    private final String streamType;
    private final String partitionId;
    private String seqNum;
    private long timestamp;
    private int offset;

    public PhoenixShardIterator(String shardIterator) {
        String[] shardIteratorComponents = shardIterator.split(SHARD_ITERATOR_DELIM);
        if (shardIteratorComponents.length != SHARD_ITERATOR_NUM_PARTS) {
            throw new IllegalArgumentException(shardIterator
                    + ": Provided shard iterator is not of the right format.");
        }
        this.tableName = shardIteratorComponents[1];
        this.cdcObject = shardIteratorComponents[2];
        this.streamType = shardIteratorComponents[3];
        this.partitionId = shardIteratorComponents[4];
        this.seqNum = shardIteratorComponents[5];
        setTimestampAndOffset();
    }

    public String getTableName() {
        return tableName;
    }

    public String getCdcObject() {
        return cdcObject;
    }

    public String getStreamType() {
        return streamType;
    }

    public String getPartitionId() {
        return partitionId;
    }

    public String getSeqNum() {
        return seqNum;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getOffset() {
        return offset;
    }

    public void setNewSeqNum(long timestamp, int offset) {
        this.timestamp = timestamp;
        this.offset = offset;
        this.seqNum = DdbAdapterCdcUtils.getSequenceNumber(timestamp, offset);
    }

    @Override
    public String toString() {
        return String.format(SHARD_ITERATOR_FORMAT,
                tableName, cdcObject, streamType, partitionId, seqNum);
    }

    private void setTimestampAndOffset() {
        String timestampStr = this.seqNum.substring(0, this.seqNum.length() - OFFSET_LENGTH);
        String offsetStr = this.seqNum.substring(this.seqNum.length() - OFFSET_LENGTH);
        this.timestamp = Long.parseLong(timestampStr);
        this.offset = Integer.parseInt(offsetStr);
    }
}
