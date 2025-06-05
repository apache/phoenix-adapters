package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.service.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ListStreamsService {

    public static Map<String, Object> listStreams(Map<String, Object> request, String connectionUrl) {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        Preconditions.checkNotNull(tableName, "Table Name should be provided.");

        Map<String, Object> result = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
            PTable table = pconn.getTable(new PTableKey(pconn.getTenantId(), "DDB." + tableName));
            List<Map<String, Object>> streams = new ArrayList<>();
            // For now, we will only return the currently enabled or enabling stream
            // TODO: once phoenix can handle disable stream, we will also return historical streams.
            String streamName
                    = DDBShimCDCUtils.getEnabledStreamName(pconn, table.getName().getString());
            if (streamName != null && table.getSchemaVersion() != null) {
                long creationTS = DDBShimCDCUtils.getCDCIndexTimestampFromStreamName(streamName);
                Map<String, Object> stream = new HashMap<>();
                stream.put(ApiMetadata.TABLE_NAME, table.getName().getString());
                stream.put(ApiMetadata.STREAM_ARN, streamName);
                stream.put(ApiMetadata.STREAM_LABEL, DDBShimCDCUtils.getStreamLabel(creationTS));
                streams.add(stream);
            }
            result.put(ApiMetadata.STREAMS, streams);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

}
