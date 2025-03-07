package org.apache.phoenix.ddb.service;

import software.amazon.awssdk.services.dynamodb.model.ListStreamsRequest;
import software.amazon.awssdk.services.dynamodb.model.ListStreamsResponse;
import software.amazon.awssdk.services.dynamodb.model.Stream;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ListStreamsService {

    public static ListStreamsResponse listStreams(ListStreamsRequest request, String connectionUrl) {
        String tableName = request.tableName();
        Preconditions.checkNotNull(tableName, "Table Name should be provided.");

        ListStreamsResponse.Builder result = ListStreamsResponse.builder();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
            PTable table = pconn.getTable(
                    new PTableKey(pconn.getTenantId(), request.tableName()));
            List<Stream> streams = new ArrayList<>();
            // For now, we will only return the currently enabled or enabling stream
            // TODO: once phoenix can handle disable stream, we will also return historical streams.
            String streamName
                    = DDBShimCDCUtils.getEnabledStreamName(pconn, table.getName().getString());
            if (streamName != null && table.getSchemaVersion() != null) {
                long creationTS = DDBShimCDCUtils.getCDCIndexTimestampFromStreamName(streamName);
                streams.add(Stream.builder()
                        .tableName(table.getName().getString())
                        .streamArn(streamName)
                        .streamLabel(DDBShimCDCUtils.getStreamLabel(creationTS)).build());
            }
            result.streams(streams);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result.build();
    }
}
