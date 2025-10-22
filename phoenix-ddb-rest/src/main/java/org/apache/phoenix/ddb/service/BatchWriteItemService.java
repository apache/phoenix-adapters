package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;

public class BatchWriteItemService {

    public static Map<String, Object> batchWriteItem(Map<String, Object> request,
            String connectionUrl) {
        ValidationUtil.validateBatchWriteItemRequest(request);
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            connection.setAutoCommit(false);
            Map<String, List<Map<String, Object>>> requestItems =
                    (Map<String, List<Map<String, Object>>>) request.get(ApiMetadata.REQUEST_ITEMS);
            for (Map.Entry<String, List<Map<String, Object>>> requestItemEntry
                    : requestItems.entrySet()) {
                List<Map<String, Object>> writeRequests = requestItemEntry.getValue();
                for (int i = 0; i < writeRequests.size(); i++) {
                    Map<String, Object> wr = writeRequests.get(i);
                    if (wr.containsKey(ApiMetadata.PUT_REQUEST)) {
                        Map<String, Object> putRequest = new HashMap<>();
                        putRequest.put(ApiMetadata.ITEM,
                                ((Map<String, Object>) wr.get(ApiMetadata.PUT_REQUEST)).get(ApiMetadata.ITEM));
                        putRequest.put(ApiMetadata.TABLE_NAME, requestItemEntry.getKey());
                        PutItemService.putItemWithConn(connection, putRequest);
                    } else if (wr.containsKey(ApiMetadata.DELETE_REQUEST)) {
                        Map<String, Object> deleteRequest = new HashMap<>();
                        deleteRequest.put(ApiMetadata.KEY,
                                ((Map<String, Object>) wr.get(ApiMetadata.DELETE_REQUEST)).get(ApiMetadata.KEY));
                        deleteRequest.put(ApiMetadata.TABLE_NAME, requestItemEntry.getKey());
                        DeleteItemService.deleteItemWithConn(connection, deleteRequest);
                    } else {
                        throw new ValidationException(
                                "WriteRequest should have either a PutRequest or a DeleteRequest.");
                    }
                }
            }
            connection.commit();
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
        Map<String, Object> responseMap = new HashMap<>();
        responseMap.put(ApiMetadata.UNPROCESSED_ITEMS,  Collections.emptyMap());
        return responseMap;
    }
}
