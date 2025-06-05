package org.apache.phoenix.ddb.service.utils;

/**
 * Request and Response parameters for DynamoDB APIs.
 */
public class ApiMetadata {

    public static final String TABLE_NAME = "TableName";
    public static final String TABLE_STATUS = "TableStatus";
    public static final String TABLE_NAMES = "TableNames";
    public static final String TABLE_DESCRIPTION = "TableDescription";
    public static final String EXCLUSIVE_START_TABLE_NAME = "ExclusiveStartTableName";
    public static final String LAST_EVALUATED_TABLE_NAME = "LastEvaluatedTableName";
    public static final String LIMIT = "Limit";
    public static final String CREATION_DATE_TIME = "CreationDateTime";

    // ---------- Attribute Definitions and Schema ----------
    public static final String ATTRIBUTE_DEFINITIONS = "AttributeDefinitions";
    public static final String ATTRIBUTE_NAME = "AttributeName";
    public static final String ATTRIBUTE_TYPE = "AttributeType";
    public static final String KEY_SCHEMA = "KeySchema";
    public static final String KEY_TYPE = "KeyType";

    // ---------- Indexes ----------
    public static final String LOCAL_SECONDARY_INDEXES = "LocalSecondaryIndexes";
    public static final String GLOBAL_SECONDARY_INDEXES = "GlobalSecondaryIndexes";
    public static final String GLOBAL_SECONDARY_INDEX_UPDATES = "GlobalSecondaryIndexUpdates";

    // ---------- UpdateTable ----------
    public static final String CREATE = "Create";
    public static final String DELETE = "Delete";
    public static final String UPDATE = "Update";
    public static final String INDEX_NAME = "IndexName";
    public static final String PROJECTION = "Projection";
    public static final String PROJECTION_TYPE = "ProjectionType";
    public static final String INDEX_STATUS = "IndexStatus";


    // ---------- Time To Live ----------
    public static final String TIME_TO_LIVE_SPECIFICATION = "TimeToLiveSpecification";
    public static final String TIME_TO_LIVE_DESCRIPTION = "TimeToLiveDescription";
    public static final String TIME_TO_LIVE_STATUS = "TimeToLiveStatus";
    public static final String TIME_TO_LIVE_ENABLED = "Enabled";

    // ---------- Query / Scan ----------
    public static final String SELECT = "Select";
    public static final String KEY_CONDITION_EXPRESSION = "KeyConditionExpression";
    public static final String FILTER_EXPRESSION = "FilterExpression";
    public static final String EXPRESSION_ATTRIBUTE_NAMES = "ExpressionAttributeNames";
    public static final String EXPRESSION_ATTRIBUTE_VALUES = "ExpressionAttributeValues";
    public static final String SCAN_INDEX_FORWARD = "ScanIndexForward";
    public static final String CONSISTENT_READ = "ConsistentRead";
    public static final String EXCLUSIVE_START_KEY = "ExclusiveStartKey";
    public static final String ATTRIBUTES_TO_GET = "AttributesToGet";
    public static final String PROJECTION_EXPRESSION = "ProjectionExpression";
    public static final String SEGMENT = "Segment";
    public static final String TOTAL_SEGMENTS = "TotalSegments";
    public static final String LAST_EVALUATED_KEY = "LastEvaluatedKey";
    public static final String ITEMS = "Items";
    public static final String COUNT = "Count";
    public static final String SCANNED_COUNT = "ScannedCount";

    // ---------- PutItem / UpdateItem / DeleteItem ----------
    public static final String ITEM = "Item";
    public static final String KEY = "Key";
    public static final String CONDITION_EXPRESSION = "ConditionExpression";
    public static final String RETURN_VALUES = "ReturnValues";
    public static final String RETURN_VALUES_ON_CONDITION_CHECK_FAILURE = "ReturnValuesOnConditionCheckFailure";
    public static final String UPDATE_EXPRESSION = "UpdateExpression";
    public static final String ATTRIBUTES = "Attributes";

    // ---------- BatchGetItem ----------
    public static final String REQUEST_ITEMS = "RequestItems";
    public static final String RESPONSES = "Responses";
    public static final String UNPROCESSED_KEYS = "UnprocessedKeys";

    // ---------- BatchWriteItem ----------
    public static final String PUT_REQUEST = "PutRequest";
    public static final String DELETE_REQUEST = "DeleteRequest";
    public static final String UNPROCESSED_ITEMS = "UnprocessedItems";

    // ---------- Streams ----------
    public static final String STREAM_SPECIFICATION = "StreamSpecification";
    public static final String STREAM_ENABLED = "StreamEnabled";
    public static final String STREAM_VIEW_TYPE = "StreamViewType";
    public static final String LATEST_STREAM_ARN = "LatestStreamArn";
    public static final String LATEST_STREAM_LABEL = "LatestStreamLabel";
    public static final String STREAMS = "Streams";
    public static final String STREAM_ARN = "StreamArn";
    public static final String EXCLUSIVE_START_STREAM_ARN = "ExclusiveStartStreamArn";
    public static final String SHARD_ID = "ShardId";
    public static final String PARENT_SHARD_ID = "ParentShardId";
    public static final String SHARD_ITERATOR_TYPE = "ShardIteratorType";
    public static final String SEQUENCE_NUMBER = "SequenceNumber";
    public static final String STARTING_SEQUENCE_NUMBER = "StartingSequenceNumber";
    public static final String ENDING_SEQUENCE_NUMBER = "EndingSequenceNumber";
    public static final String SEQUENCE_NUMBER_RANGE = "SequenceNumberRange";
    public static final String SHARD_ITERATOR = "ShardIterator";
    public static final String RECORDS = "Records";
    public static final String NEXT_SHARD_ITERATOR = "NextShardIterator";
    public static final String EXCLUSIVE_START_SHARD_ID = "ExclusiveStartShardId";
    public static final String LAST_EVALUATED_STREAM_ARN = "LastEvaluatedStreamArn";
    public static final String LAST_EVALUATED_SHARD_ID = "LastEvaluatedShardId";
    public static final String SHARDS = "Shards";
    public static final String STREAM_DESCRIPTION = "StreamDescription";
    public static final String STREAM_STATUS = "StreamStatus";
    public static final String STREAM_LABEL = "StreamLabel";
    public static final String CREATION_REQUEST_DATE_TIME = "CreationRequestDateTime";
    public static final String DYNAMODB = "dynamodb";
    public static final String EVENT_NAME = "eventName";
    public static final String APPROXIMATE_CREATION_DATE_TIME = "ApproximateCreationDateTime";
    public static final String NEW_IMAGE = "NewImage";
    public static final String OLD_IMAGE = "OldImage";
    public static final String KEYS = "Keys";
}
