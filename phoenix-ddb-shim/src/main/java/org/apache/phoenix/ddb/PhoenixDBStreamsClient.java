package org.apache.phoenix.ddb;

import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeStreamResult;
import com.amazonaws.services.dynamodbv2.model.GetRecordsRequest;
import com.amazonaws.services.dynamodbv2.model.GetRecordsResult;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorRequest;
import com.amazonaws.services.dynamodbv2.model.GetShardIteratorResult;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import org.apache.phoenix.ddb.service.DescribeStreamService;
import org.apache.phoenix.ddb.service.GetRecordsService;
import org.apache.phoenix.ddb.service.ListStreamsService;
import org.apache.phoenix.ddb.service.GetShardIteratorService;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PhoenixDB Client to convert DDB Streams requests into Phoenix Streams SQL queries and execute.
 */
public class PhoenixDBStreamsClient extends AbstractAmazonDynamoDBStreams {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixDBStreamsClient.class);

    private final String connectionUrl;

    public PhoenixDBStreamsClient(String connectionUrl) {
        PhoenixUtils.checkConnectionURL(connectionUrl);
        this.connectionUrl = connectionUrl;
        PhoenixUtils.registerDriver();
    }

    /**
     * {@inheritDoc}
     */
    public ListStreamsResult listStreams(ListStreamsRequest request) {
        return ListStreamsService.listStreams(request, connectionUrl);
    }

    /**
     * {@inheritDoc}
     */
    public DescribeStreamResult describeStream(DescribeStreamRequest request) {
        return DescribeStreamService.describeStream(request, connectionUrl);
    }

    /**
     * {@inheritDoc}
     */
    public GetShardIteratorResult getShardIterator(GetShardIteratorRequest request) {
        return GetShardIteratorService.getShardIterator(request, connectionUrl);
    }

    /**
     * {@inheritDoc}
     */
    public GetRecordsResult getRecords(GetRecordsRequest request) {
        return GetRecordsService.getRecords(request, connectionUrl);
    }
}
