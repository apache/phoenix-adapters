# Phoenix DynamoDB REST Service

The Phoenix DynamoDB REST Service provides a DynamoDB-compatible REST API that uses Apache Phoenix (built on HBase) as the underlying storage engine. This allows applications built for DynamoDB to work with Phoenix without any code changes.

## Overview

This service acts as a translation layer between DynamoDB API calls and Phoenix SQL operations, enabling:

- **Zero code changes** for existing DynamoDB applications
- **Familiar DynamoDB semantics** while leveraging Phoenix's scalability
- **Full AWS SDK compatibility** with simple endpoint configuration

## Supported Operations

For a complete list of supported DynamoDB operations, see the [Supported APIs section](../README.md#supported-apis) in the main README.

## Connecting with AWS SDK

The Phoenix DynamoDB REST service is fully compatible with AWS SDKs. You can connect to it by simply configuring the endpoint URL to point to your Phoenix REST service instead of the standard DynamoDB endpoint.

### Java AWS SDK V2

Here's how to configure the DynamoDB client to use Phoenix REST service:

```java
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class PhoenixDynamoDbClient {
    
    public static DynamoDbClient createClient() {
        // Configure credentials (can be dummy values for Phoenix REST)
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create("dummy", "dummy");
        
        return DynamoDbClient.builder()
                .endpointOverride(URI.create("http://phoenixrest.hbase.cluster:8842"))
                .region(Region.US_EAST_1) // Can be any region
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .build();
    }
    
    public static void main(String[] args) {
        DynamoDbClient dynamoDb = createClient();
        
        // Example: Create a table
        CreateTableRequest createTableRequest = CreateTableRequest.builder()
                .tableName("MyTable")
                .keySchema(
                    KeySchemaElement.builder()
                        .attributeName("id")
                        .keyType(KeyType.HASH)
                        .build()
                )
                .attributeDefinitions(
                    AttributeDefinition.builder()
                        .attributeName("id")
                        .attributeType(ScalarAttributeType.S)
                        .build()
                )
                .billingMode(BillingMode.PAY_PER_REQUEST)
                .build();
        
        try {
            CreateTableResponse response = dynamoDb.createTable(createTableRequest);
            System.out.println("Table created: " + response.tableDescription().tableName());
        } catch (Exception e) {
            System.err.println("Error creating table: " + e.getMessage());
        }
        
        // Example: Put an item
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("id", AttributeValue.builder().s("123").build());
        item.put("name", AttributeValue.builder().s("John Doe").build());
        item.put("age", AttributeValue.builder().n("30").build());
        
        PutItemRequest putItemRequest = PutItemRequest.builder()
                .tableName("MyTable")
                .item(item)
                .build();
        
        try {
            dynamoDb.putItem(putItemRequest);
            System.out.println("Item added successfully");
        } catch (Exception e) {
            System.err.println("Error putting item: " + e.getMessage());
        }
        
        // Example: Get an item
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("id", AttributeValue.builder().s("123").build());
        
        GetItemRequest getItemRequest = GetItemRequest.builder()
                .tableName("MyTable")
                .key(key)
                .build();
        
        try {
            GetItemResponse response = dynamoDb.getItem(getItemRequest);
            if (response.hasItem()) {
                System.out.println("Retrieved item: " + response.item());
            } else {
                System.out.println("Item not found");
            }
        } catch (Exception e) {
            System.err.println("Error getting item: " + e.getMessage());
        }
        
        dynamoDb.close();
    }
}
```

### Python AWS SDK (boto3)

Here's how to configure the DynamoDB client using Python boto3:

```python
import boto3
from botocore.exceptions import ClientError

def create_dynamodb_client():
    """Create a DynamoDB client configured for Phoenix REST service"""
    return boto3.client(
        'dynamodb',
        endpoint_url='http://phoenixrest.hbase.cluster:8842',
        region_name='us-east-1',  # Can be any region
        aws_access_key_id='dummy',  # Can be dummy values for Phoenix REST
        aws_secret_access_key='dummy'
    )

def create_dynamodb_resource():
    """Create a DynamoDB resource configured for Phoenix REST service"""
    return boto3.resource(
        'dynamodb',
        endpoint_url='http://phoenixrest.hbase.cluster:8842',
        region_name='us-east-1',
        aws_access_key_id='dummy',
        aws_secret_access_key='dummy'
    )

def main():
    # Using low-level client
    dynamodb_client = create_dynamodb_client()
    
    # Example: Create a table
    try:
        response = dynamodb_client.create_table(
            TableName='MyTable',
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                }
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        print(f"Table created: {response['TableDescription']['TableName']}")
    except ClientError as e:
        print(f"Error creating table: {e}")
    
    # Example: Put an item
    try:
        dynamodb_client.put_item(
            TableName='MyTable',
            Item={
                'id': {'S': '123'},
                'name': {'S': 'John Doe'},
                'age': {'N': '30'}
            }
        )
        print("Item added successfully")
    except ClientError as e:
        print(f"Error putting item: {e}")
    
    # Example: Get an item
    try:
        response = dynamodb_client.get_item(
            TableName='MyTable',
            Key={
                'id': {'S': '123'}
            }
        )
        if 'Item' in response:
            print(f"Retrieved item: {response['Item']}")
        else:
            print("Item not found")
    except ClientError as e:
        print(f"Error getting item: {e}")
    
    # Using high-level resource (more Pythonic)
    dynamodb_resource = create_dynamodb_resource()
    table = dynamodb_resource.Table('MyTable')
    
    # Example: Put item using resource
    try:
        table.put_item(
            Item={
                'id': '456',
                'name': 'Jane Smith',
                'age': 25,
                'email': 'jane@example.com'
            }
        )
        print("Item added using resource")
    except ClientError as e:
        print(f"Error putting item with resource: {e}")
    
    # Example: Query items
    try:
        response = table.get_item(
            Key={
                'id': '456'
            }
        )
        if 'Item' in response:
            print(f"Retrieved item using resource: {response['Item']}")
        else:
            print("Item not found")
    except ClientError as e:
        print(f"Error getting item with resource: {e}")

if __name__ == "__main__":
    main()
```

### Node.js AWS SDK V3

Here's how to configure the DynamoDB client using Node.js AWS SDK V3:

```javascript
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { 
    CreateTableCommand, 
    PutItemCommand, 
    GetItemCommand,
    DeleteTableCommand 
} from "@aws-sdk/client-dynamodb";
import { 
    DynamoDBDocumentClient, 
    PutCommand, 
    GetCommand 
} from "@aws-sdk/lib-dynamodb";

// Create DynamoDB client
const client = new DynamoDBClient({
    endpoint: "http://phoenixrest.hbase.cluster:8842",
    region: "us-east-1", // Can be any region
    credentials: {
        accessKeyId: "dummy", // Can be dummy values for Phoenix REST
        secretAccessKey: "dummy"
    }
});

// Create document client for easier data handling
const docClient = DynamoDBDocumentClient.from(client);

async function main() {
    try {
        // Example: Create a table
        const createTableCommand = new CreateTableCommand({
            TableName: "MyTable",
            KeySchema: [
                {
                    AttributeName: "id",
                    KeyType: "HASH"
                }
            ],
            AttributeDefinitions: [
                {
                    AttributeName: "id",
                    AttributeType: "S"
                }
            ],
            BillingMode: "PAY_PER_REQUEST"
        });
        
        const createResponse = await client.send(createTableCommand);
        console.log("Table created:", createResponse.TableDescription.TableName);
        
        // Example: Put an item using document client (easier)
        const putCommand = new PutCommand({
            TableName: "MyTable",
            Item: {
                id: "123",
                name: "John Doe",
                age: 30,
                email: "john@example.com"
            }
        });
        
        await docClient.send(putCommand);
        console.log("Item added successfully");
        
        // Example: Get an item using document client
        const getCommand = new GetCommand({
            TableName: "MyTable",
            Key: {
                id: "123"
            }
        });
        
        const getResponse = await docClient.send(getCommand);
        if (getResponse.Item) {
            console.log("Retrieved item:", getResponse.Item);
        } else {
            console.log("Item not found");
        }
        
    } catch (error) {
        console.error("Error:", error);
    }
}

main();
```

## Configuration Notes

### 1. Endpoint URL
Replace `http://phoenixrest.hbase.cluster:8842` with your actual Phoenix REST service endpoint. The secure SSL/TLS configured connection could also start with `https://` while connecting from other data centers or regions.

### 2. Credentials
Phoenix REST service doesn't require real AWS credentials, so you can use dummy values. However, the AWS SDK still requires credentials to be provided for initialization.

### 3. Region
You can specify any AWS region as Phoenix REST service doesn't enforce region-specific behavior. Common choices:
- `us-east-1`
- `us-west-2`
- `eu-west-1`

### 4. HTTP Connection Timeout
For long-running operations (like complex DDL operations), you may need to configure the HTTP connection idle timeout.
The default is 30 seconds (similar to `jetty.http.idleTimeout`).

**Configuration Property:**
```
phoenix.ddb.rest.http.idle.timeout=30000  # 30 seconds
```

## Testing Your Connection

You can test your connection by running a simple operation like `ListTables`:

**Java:**
```java
ListTablesResponse response = dynamoDb.listTables();
System.out.println("Available tables: " + response.tableNames());
```

**Python:**
```python
response = dynamodb_client.list_tables()
print("Available tables:", response['TableNames'])
```

**Node.js:**
```javascript
import { ListTablesCommand } from "@aws-sdk/client-dynamodb";

const command = new ListTablesCommand({});
const response = await client.send(command);
console.log("Available tables:", response.TableNames);
```
