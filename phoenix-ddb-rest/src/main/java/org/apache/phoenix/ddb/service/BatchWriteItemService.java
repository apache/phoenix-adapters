package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.utils.ApiMetadata;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteItemService {

    private static final int NUM_ITEMS_LIMIT_PER_TABLE = 25;

    public static Map<String, Object> batchWriteItem(Map<String, Object> request,
            String connectionUrl) {
        Map<String, Object> unprocessedItems = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(false);
            Map<String, List<Map<String, Object>>> requestItems =
                    (Map<String, List<Map<String, Object>>>) request.get(ApiMetadata.REQUEST_ITEMS);
            for (Map.Entry<String, List<Map<String, Object>>> requestItemEntry
                    : requestItems.entrySet()) {
                List<Map<String, Object>> writeRequests = requestItemEntry.getValue();
                for (int i = 0;
                     i < Integer.min(writeRequests.size(), NUM_ITEMS_LIMIT_PER_TABLE); i++) {
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
                        throw new RuntimeException(
                                "WriteRequest should have either a PutRequest or a DeleteRequest.");
                    }
                }
                if (writeRequests.size() > NUM_ITEMS_LIMIT_PER_TABLE) {
                    unprocessedItems.put(requestItemEntry.getKey(),
                            writeRequests.subList(NUM_ITEMS_LIMIT_PER_TABLE, writeRequests.size()));
                }
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        Map<String, Object> unprocessedItemsMap = new HashMap<>();
        unprocessedItemsMap.put(ApiMetadata.UNPROCESSED_ITEMS, unprocessedItems);
        return unprocessedItemsMap;
    }
}
