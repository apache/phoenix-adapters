package org.apache.phoenix.ddb;

import org.apache.phoenix.ddb.service.BatchGetItemService;
import org.apache.phoenix.ddb.service.BatchWriteItemService;
import org.apache.phoenix.ddb.service.CreateTableService;
import org.apache.phoenix.ddb.service.DeleteItemService;
import org.apache.phoenix.ddb.service.DeleteTableService;
import org.apache.phoenix.ddb.service.GetItemService;
import org.apache.phoenix.ddb.service.PutItemService;
import org.apache.phoenix.ddb.service.QueryService;
import org.apache.phoenix.ddb.service.ScanService;
import org.apache.phoenix.ddb.service.TTLService;
import org.apache.phoenix.ddb.service.UpdateItemService;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.ddb.utils.TableDescriptorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTimeToLiveResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryResponse;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTimeToLiveResponse;

public class PhoenixDBClientV2 implements DynamoDbClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixDBClientV2.class);

    private final String connectionUrl;

    public PhoenixDBClientV2(String connectionUrl) {
        PhoenixUtils.checkConnectionURL(connectionUrl);
        this.connectionUrl = connectionUrl;
        PhoenixUtils.registerDriver();
    }

    @Override
    public String serviceName() {
        return "phoenix-db-client-v2";
    }

    @Override
    public void close() {}

    public BatchGetItemResponse batchGetItem(BatchGetItemRequest batchGetItemRequest) {
        return BatchGetItemService.batchGetItem(batchGetItemRequest, connectionUrl);
    }

    public BatchWriteItemResponse batchWriteItem(BatchWriteItemRequest batchWriteItemRequest) {
        return BatchWriteItemService.batchWriteItem(batchWriteItemRequest, connectionUrl);
    }

    public CreateTableResponse createTable(CreateTableRequest createTableRequest) {
        return CreateTableService.createTable(createTableRequest, connectionUrl);
    }

    public DeleteItemResponse deleteItem(DeleteItemRequest deleteItemRequest) {
        return DeleteItemService.deleteItem(deleteItemRequest, connectionUrl);
    }

    public DeleteTableResponse deleteTable(DeleteTableRequest deleteTableRequest) {
        return DeleteTableService.deleteTable(deleteTableRequest, connectionUrl);
    }

    public DescribeTableResponse describeTable(DescribeTableRequest describeTableRequest) {
        TableDescription tableDescription
                = TableDescriptorUtils.getTableDescription(describeTableRequest.tableName(), connectionUrl);
        return DescribeTableResponse.builder().table(tableDescription).build();
    }

    public DescribeTimeToLiveResponse describeTimeToLive(DescribeTimeToLiveRequest describeTimeToLiveRequest) {
        return TTLService.describeTimeToLive(describeTimeToLiveRequest, connectionUrl);
    }

    public GetItemResponse getItem(GetItemRequest getItemRequest) {
        return GetItemService.getItem(getItemRequest, connectionUrl);
    }

    public PutItemResponse putItem(PutItemRequest putItemRequest) {
        return PutItemService.putItem(putItemRequest, connectionUrl);
    }

    public QueryResponse query(QueryRequest queryRequest) {
        return QueryService.query(queryRequest, connectionUrl);
    }

    public ScanResponse scan(ScanRequest scanRequest) {
        return ScanService.scan(scanRequest, connectionUrl);
    }

    public UpdateItemResponse updateItem(UpdateItemRequest updateItemRequest) {
        return UpdateItemService.updateItem(updateItemRequest, connectionUrl);
    }

    public UpdateTimeToLiveResponse updateTimeToLive(UpdateTimeToLiveRequest updateTimeToLiveRequest) {
        return TTLService.updateTimeToLive(updateTimeToLiveRequest, connectionUrl);
    }
}
