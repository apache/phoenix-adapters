package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteItemService {

    private static final int NUM_ITEMS_LIMIT_PER_TABLE = 25;

    public static BatchWriteItemResult batchWriteItem(BatchWriteItemRequest request,
                                                      String connectionUrl) {
        Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(false);
            for (String tableName : request.getRequestItems().keySet()) {
                List<WriteRequest> writeRequests = request.getRequestItems().get(tableName);
                for (int i=0; i<Integer.min(writeRequests.size(), NUM_ITEMS_LIMIT_PER_TABLE); i++) {
                    WriteRequest wr = writeRequests.get(i);
                    if (wr.getPutRequest() != null) {
                        PutItemRequest pir = new PutItemRequest(tableName, wr.getPutRequest().getItem());
                        PutItemService.putItemWithConn(connection, pir);
                    } else if (wr.getDeleteRequest() != null) {
                        DeleteItemRequest dir = new DeleteItemRequest(tableName, wr.getDeleteRequest().getKey());
                        DeleteItemService.deleteItemWithConn(connection, dir);
                    } else {
                        throw new RuntimeException("WriteRequest should have either a PutRequest or a DeleteRequest.");
                    }
                }
                if (writeRequests.size() > NUM_ITEMS_LIMIT_PER_TABLE) {
                    unprocessedItems.put(tableName,
                            writeRequests.subList(NUM_ITEMS_LIMIT_PER_TABLE, writeRequests.size()));
                }
            }
            connection.commit();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return new BatchWriteItemResult().withUnprocessedItems(unprocessedItems);
    }
}
