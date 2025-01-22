package org.apache.phoenix.ddb;

import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import org.apache.phoenix.ddb.service.StreamsService;
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
        return StreamsService.listStreams(request, connectionUrl);
    }
}
