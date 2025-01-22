package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.Stream;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.EnvironmentEdgeManager;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class StreamsService {

    public static ListStreamsResult listStreams(ListStreamsRequest request, String connectionUrl) {
        String tableName = request.getTableName();
        Preconditions.checkNotNull(tableName, "Table Name should be provided.");

        ListStreamsResult result = new ListStreamsResult();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
            PTable table = pconn.getTable(
                    new PTableKey(pconn.getTenantId(), request.getTableName()));
            List<Stream> streams = new ArrayList<>();
            // For now, we will only return the currently enabled or enabling stream
            // TODO: once phoenix can handle disable stream, we will also return historical streams.
            String streamName
                    = DDBShimCDCUtils.getEnabledStreamName(pconn, table.getName().getString());
            if (streamName != null && table.getSchemaVersion() != null) {
                streams.add(new Stream()
                        .withTableName(table.getName().getString())
                        .withStreamArn(streamName)
                        .withStreamLabel(DDBShimCDCUtils.getStreamLabel(streamName)));
            }
            result.setStreams(streams);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }
}
