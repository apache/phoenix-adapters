package org.apache.phoenix.ddb.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.DdbAdapterCdcUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.util.CDCUtil;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.phoenix.jdbc.PhoenixDatabaseMetaData.SYSTEM_CDC_STREAM_STATUS_NAME;

public class ListStreamsService {

    // For now, we will only return the currently enabled or enabling stream
    // TODO: once phoenix can handle disable stream, we will also return historical streams.
    private static final String STREAM_QUERY
            = "SELECT TABLE_NAME, STREAM_NAME FROM " + SYSTEM_CDC_STREAM_STATUS_NAME
            + " WHERE STREAM_STATUS IN ('"
            + CDCUtil.CdcStreamStatus.ENABLED.getSerializedValue() + "', '"
            + CDCUtil.CdcStreamStatus.ENABLING.getSerializedValue() + "')";

    private static final int MAX_LIMIT = 100;

    public static Map<String, Object> listStreams(Map<String, Object> request, String connectionUrl) {
        Map<String, Object> result = new HashMap<>();
        String requestTableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String exclusiveStartStreamArn = (String) request.get(ApiMetadata.EXCLUSIVE_START_STREAM_ARN);
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            List<Map<String, Object>> streams = new ArrayList<>();
            StringBuilder query = new StringBuilder(STREAM_QUERY);
            if (StringUtils.isEmpty(requestTableName)) {
                query.append("AND SUBSTR(TABLE_NAME, 0, 4) = '" + PhoenixUtils.SCHEMA_NAME_AND_DELIM + "'");
            } else {
                query.append(" AND TABLE_NAME = '" + PhoenixUtils.getFullTableName(requestTableName, false) + "'");
            }
            if (!StringUtils.isEmpty(exclusiveStartStreamArn)) {
                query.append(" AND STREAM_NAME > '")
                     .append(exclusiveStartStreamArn)
                     .append("'");
            }
            int limit = (int) request.getOrDefault(ApiMetadata.LIMIT, MAX_LIMIT);
            query.append(" LIMIT ").append(limit);
            ResultSet rs = connection.createStatement().executeQuery(query.toString());
            String lastStreamArn = null;
            while (rs.next()) {
                String tableName = rs.getString(1);
                String streamName = rs.getString(2);
                Map<String, Object> stream = new HashMap<>();
                stream.put(ApiMetadata.TABLE_NAME, PhoenixUtils.getTableNameFromFullName(tableName, false));
                stream.put(ApiMetadata.STREAM_ARN, streamName);
                stream.put(ApiMetadata.STREAM_LABEL, DdbAdapterCdcUtils.getStreamLabel(streamName));
                streams.add(stream);
                lastStreamArn = streamName;
            }
            result.put(ApiMetadata.STREAMS, streams);
            result.put(ApiMetadata.LAST_EVALUATED_STREAM_ARN, lastStreamArn);
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
        return result;
    }

}
