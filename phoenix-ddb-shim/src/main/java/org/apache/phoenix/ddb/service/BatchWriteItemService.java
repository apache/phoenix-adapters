package org.apache.phoenix.ddb.service;

import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BatchWriteItemService {

    private static final int NUM_ITEMS_LIMIT_PER_TABLE = 25;

    public static BatchWriteItemResponse batchWriteItem(BatchWriteItemRequest request,
                                                      String connectionUrl) {
        Map<String, List<WriteRequest>> unprocessedItems = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(false);
            for (String tableName : request.requestItems().keySet()) {
                List<WriteRequest> writeRequests = request.requestItems().get(tableName);
                for (int i=0; i<Integer.min(writeRequests.size(), NUM_ITEMS_LIMIT_PER_TABLE); i++) {
                    WriteRequest wr = writeRequests.get(i);
                    if (wr.putRequest() != null) {
                        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(wr.putRequest().item()).build();
                        PutItemService.putItemWithConn(connection, pir);
                    } else if (wr.deleteRequest() != null) {
                        DeleteItemRequest dir = DeleteItemRequest.builder().tableName(tableName).key(wr.deleteRequest().key()).build();
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
        return BatchWriteItemResponse.builder().unprocessedItems(unprocessedItems).build();
    }
}
