package org.apache.phoenix.ddb;

import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDBStreams;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeStreamResponse;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsRequest;
import software.amazon.awssdk.services.dynamodb.model.GetRecordsResponse;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorRequest;
import software.amazon.awssdk.services.dynamodb.model.GetShardIteratorResponse;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import org.apache.phoenix.ddb.service.DescribeStreamService;
import org.apache.phoenix.ddb.service.GetRecordsService;
import org.apache.phoenix.ddb.service.ListStreamsService;
import org.apache.phoenix.ddb.service.GetShardIteratorService;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.streams.DynamoDbStreamsClient;

/**
 * PhoenixDB Client to convert DDB Streams requests into Phoenix Streams SQL queries and execute.
 */
public class PhoenixDBStreamsClientV2 implements DynamoDbStreamsClient {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixDBStreamsClientV2.class);

    private final String connectionUrl;

    public PhoenixDBStreamsClientV2(String connectionUrl) {
        PhoenixUtils.checkConnectionURL(connectionUrl);
        this.connectionUrl = connectionUrl;
        PhoenixUtils.registerDriver();
    }

    @Override
    public String serviceName() {
        return "phoenix-db-streams-client-v2";
    }

    @Override
    public void close() {

    }

    /**
     * {@inheritDoc}
     */
    public ListStreamsResponse listStreams(ListStreamsRequest request) {
        return ListStreamsService.listStreams(request, connectionUrl);
    }

    /**
     * {@inheritDoc}
     */
    public DescribeStreamResponse describeStream(DescribeStreamRequest request) {
        return DescribeStreamService.describeStream(request, connectionUrl);
    }

    /**
     * {@inheritDoc}
     */
    public GetShardIteratorResponse getShardIterator(GetShardIteratorRequest request) {
        return GetShardIteratorService.getShardIterator(request, connectionUrl);
    }

    /**
     * {@inheritDoc}
     */
    public GetRecordsResponse getRecords(GetRecordsRequest request) {
        return GetRecordsService.getRecords(request, connectionUrl);
    }
}
