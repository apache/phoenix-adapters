# Phoenix-DynamoDB Adapter: Project Overview with API Reference

## Table of Contents

- [1. Project Overview](#1-project-overview)
- [2. Architecture](#2-architecture)
- [3. How the REST API Works](#3-how-the-rest-api-works)
- [4. Data Type Mapping](#4-data-type-mapping)
- [5. Cross-Cutting Features](#5-cross-cutting-features)
- [6. DDL APIs (Table Management)](#6-ddl-apis-table-management)
  - [6.1 CreateTable](#61-createtable)
  - [6.2 DeleteTable](#62-deletetable)
  - [6.3 DescribeTable](#63-describetable)
  - [6.4 ListTables](#64-listtables)
  - [6.5 UpdateTable](#65-updatetable)
  - [6.6 UpdateTimeToLive](#66-updatetimetolive)
  - [6.7 DescribeTimeToLive](#67-describetimetolive)
  - [6.8 DescribeContinuousBackups](#68-describecontinuousbackups)
- [7. DML APIs (Data Modification)](#7-dml-apis-data-modification)
  - [7.1 PutItem](#71-putitem)
  - [7.2 UpdateItem](#72-updateitem)
  - [7.3 DeleteItem](#73-deleteitem)
  - [7.4 BatchWriteItem](#74-batchwriteitem)
- [8. DQL APIs (Data Query/Read)](#8-dql-apis-data-queryread)
  - [8.1 GetItem](#81-getitem)
  - [8.2 BatchGetItem](#82-batchgetitem)
  - [8.3 Query](#83-query)
  - [8.4 Scan](#84-scan)
- [9. Change Stream APIs](#9-change-stream-apis)
  - [9.1 ListStreams](#91-liststreams)
  - [9.2 DescribeStream](#92-describestream)
  - [9.3 GetShardIterator](#93-getsharditerator)
  - [9.4 GetRecords](#94-getrecords)
- [10. Authentication](#10-authentication)
- [11. Error Handling](#11-error-handling)
- [12. Server Configuration](#12-server-configuration)
- [13. Metrics & Monitoring](#13-metrics--monitoring)
- [14. Limitations & Differences from AWS DynamoDB](#14-limitations--differences-from-aws-dynamodb)

---

## 1. Project Overview

**Phoenix-Adapters** is a compatibility layer that allows applications written for **Amazon DynamoDB** to run against **Apache Phoenix** (on HBase) as the underlying storage engine -- with **zero code changes** on the application side.

### The Problems it solves:

Organizations that use DynamoDB on AWS face challenges when they need to:
- Migrate to a different infrastructure (e.g., on-premise, GCP, etc)
- Maintain a single codebase across multiple substrates/cloud providers
- Avoid vendor lock-in while keeping familiar NoSQL semantics
- Reduce costs associated with DynamoDB's pricing model (read/write capacity units, storage, data transfer)

### How does it solve the problems:

Phoenix-DynamoDB Adapter provides a **RESTful API server** that:
1. Accepts JSON payloads in the same format as DynamoDB's API
2. Translates DynamoDB operations into Phoenix operations under the hood
3. Returns responses in the same JSON format that DynamoDB clients expect

Client applications using **any AWS SDK** (Java, Python, Node.js, Go, etc.) only need to change the **endpoint URL** to point to the Phoenix REST server instead of the AWS DynamoDB service -- no code changes are required.

### Module Structure

| Module | Purpose |
|---|---|
| `phoenix-ddb-rest` | REST server (Jetty-based), API routing, service implementations |
| `phoenix-ddb-utils` | Shared utilities: BSON conversion, CDC/stream utils, Phoenix helpers |
| `phoenix-ddb-assembly` | Distribution packaging (tarball) |
| `coverage-report` | Code coverage aggregation |

---

## 2. Architecture

```
┌───────────────────────────────┐
│   Client Application          │
│  (AWS SDK: Java/Python/JS)    │
└──────────────┬────────────────┘
               │ HTTP POST (JSON)
               │ X-Amz-Target: DynamoDB_20120810.<Operation>
               ▼
┌───────────────────────────────┐
│  Phoenix DynamoDB REST Server │
│   (Jetty + Jersey JAX-RS)     │
│                               │
│  ┌─────────────────────────┐  │
│  │  AccessKeyAuthFilter    │  │  ← Optional authentication
│  │  (if configured)        │  │
│  └────────────┬────────────┘  │
│               ▼               │
│  ┌─────────────────────────┐  │
│  │  RootResource (Router)  │  │  ← Single POST endpoint at /
│  │  Routes by X-Amz-Target │  │
│  └────────────┬────────────┘  │
│               ▼               │
│  ┌─────────────────────────┐  │
│  │  Service Layer          │  │  ← CreateTableService, PutItemService, etc.
│  └────────────┬────────────┘  │
│               ▼               │
│  ┌─────────────────────────┐  │
│  │  BSON Conversion Layer  │  │  ← DDB attributes ↔ BSON documents
│  └────────────┬────────────┘  │
│               ▼               │
│  ┌─────────────────────────┐  │
│  │  Phoenix JDBC Driver    │  │  ← SQL execution
│  └────────────┬────────────┘  │
└───────────────┼───────────────┘
                ▼
┌───────────────────────────────┐
│   Apache Phoenix / HBase      │
│    (Persistent Storage)       │
└───────────────────────────────┘
```

### Key Design Decisions

1. **Single POST Endpoint**: All operations hit `POST /`. The operation is determined by the `X-Amz-Target` header (e.g., `DynamoDB_20120810.CreateTable`).
2. **BSON Column Storage**: Each DynamoDB item is stored as a single BSON document in a Phoenix column named `COL`. Primary key and Secondary key columns are stored separately for indexing.
3. **Phoenix Functions**: The system uses Phoenix built-in functions like `BSON_VALUE()`, `BSON_CONDITION_EXPRESSION()`, and `BSON_UPDATE_EXPRESSION()` to operate on documents at the database level.
4. **CDC for Streams**: DynamoDB Streams are implemented using Phoenix's Change Data Capture (CDC) Stream feature.

---

## 3. How the REST API Works

### Request Format

Every API call is an HTTP `POST` to the root path `/` with:

| Component | Value | Example |
|---|---|---|
| **Method** | `POST` | |
| **URL** | `http://<host>:<port>/` | `http://localhost:8842/` |
| **Content-Type** | `application/x-amz-json-1.0` or `application/json` | |
| **X-Amz-Target** | `DynamoDB_20120810.<Operation>` | `DynamoDB_20120810.CreateTable` |
| **Body** | JSON request payload | `{"TableName": "MyTable", ...}` |

### Response Format

- **Success**: HTTP `200 OK` with JSON body
- **Validation Error**: HTTP `400 Bad Request` with error body
- **Table Not Found**: HTTP `400 Bad Request` with `ResourceNotFoundException`
- **Condition Check Failure**: HTTP `400 Bad Request` with `ConditionalCheckFailedException`
- **Resource In Use**: HTTP `400 Bad Request` with `ResourceInUseException`

Error response format:
```json
{
  "__type": "com.amazonaws.dynamodb.v20120810#ValidationException",
  "message": "Error description here"
}
```

---

## 4. Data Type Mapping

### Primary Key Attribute Types (Scalar)

These are the types allowed for primary key (partition key and sort key) attributes:

| DynamoDB Type | DynamoDB Code | Phoenix SQL Type | Description |
|---|---|---|---|
| String | `S` | `VARCHAR` | UTF-8 encoded string |
| Number | `N` | `DOUBLE` | Numeric values |
| Binary | `B` | `VARBINARY_ENCODED` | Binary data |

### Non-Key Attribute Types

Non-key attributes are stored inside the BSON `COL` column and support the full DynamoDB type system:

| DynamoDB Type | Code | Description |
|---|---|---|
| String | `S` | UTF-8 string |
| Number | `N` | Numeric value |
| Binary | `B` | Binary data (Base64-encoded) |
| Boolean | `BOOL` | `true` or `false` |
| Null | `NULL` | Null value |
| List | `L` | Ordered collection of values |
| Map | `M` | Unordered collection of key-value pairs |
| String Set | `SS` | Set of unique strings |
| Number Set | `NS` | Set of unique numbers |
| Binary Set | `BS` | Set of unique binary values |

---

## 5. Cross-Cutting Features

### 5.1 Conditional Expressions

Supported by: **PutItem**, **UpdateItem**, **DeleteItem**

Conditional expressions allow you to specify conditions that must be met for the operation to succeed.

**Modern syntax** (preferred):
```json
{
  "ConditionExpression": "attribute_exists(#pk) AND #status = :val",
  "ExpressionAttributeNames": {"#pk": "id", "#status": "status"},
  "ExpressionAttributeValues": {":val": {"S": "active"}}
}
```

**Legacy syntax** (auto-converted to modern):
```json
{
  "Expected": {
    "status": {
      "ComparisonOperator": "EQ",
      "AttributeValueList": [{"S": "active"}]
    }
  },
  "ConditionalOperator": "AND"
}
```

**Supported comparison operators in legacy `Expected`**:
`EQ`, `NE`, `LT`, `LE`, `GT`, `GE`, `BETWEEN`, `IN`, `BEGINS_WITH`, `CONTAINS`, `NOT_CONTAINS`, `NULL`, `NOT_NULL`

When a condition check fails, a `ConditionalCheckFailedException` is returned.

### 5.2 Projection Expressions

Supported by: **GetItem**, **BatchGetItem**, **Query**, **Scan**

Projection expressions specify which attributes to include in the response.

**Modern syntax** (preferred):
```json
{
  "ProjectionExpression": "#n, age, address.city",
  "ExpressionAttributeNames": {"#n": "name"}
}
```

**Legacy syntax** (auto-converted):
```json
{
  "AttributesToGet": ["name", "age"]
}
```

Note: `ProjectionExpression` and `AttributesToGet` are **mutually exclusive** -- using both in the same request throws 400.

### 5.3 Expression Attribute Names

`ExpressionAttributeNames` allows you to use `#alias` placeholders in expressions to reference attribute names that:
- Conflict with DynamoDB reserved words
- Contain special characters
- Need to be reused across multiple expressions

```json
{
  "ExpressionAttributeNames": {
    "#s": "status",
    "#d": "date"
  }
}
```

### 5.4 Expression Attribute Values

`ExpressionAttributeValues` provides typed value placeholders for use in expressions:

```json
{
  "ExpressionAttributeValues": {
    ":status": {"S": "active"},
    ":minAge": {"N": "18"},
    ":data": {"B": "base64encodeddata"}
  }
}
```

### 5.5 Return Values

Supported by: **PutItem**, **UpdateItem**, **DeleteItem**

Controls what data is returned after a write operation.

| ReturnValues Value | PutItem | UpdateItem | DeleteItem | Description |
|---|---|---|---|---|
| `NONE` | Yes (default) | Yes (default) | Yes (default) | Returns nothing (only `ConsumedCapacity`) |
| `ALL_OLD` | Yes | Yes | Yes | Returns the item as it was **before** the operation |
| `ALL_NEW` | No | Yes | No | Returns the item as it is **after** the operation |
| `UPDATED_OLD` | -- | **Not supported** (throws 400), use `ALL_OLD` instead | -- | Only applicable to UpdateItem |
| `UPDATED_NEW` | -- | **Not supported** (throws 400), use `ALL_NEW` instead | -- | Only applicable to UpdateItem |

### 5.6 ReturnValuesOnConditionCheckFailure

Supported by: **PutItem**, **UpdateItem**, **DeleteItem**

Controls whether the existing item is returned when a condition check fails.

| Value | Description |
|---|---|
| `NONE` | No item returned on failure (default) |
| `ALL_OLD` | Returns the existing item that caused the condition to fail |

### 5.7 Filter Expressions

Supported by: **Query**, **Scan**

Filter expressions are applied **after** items are read from the database but **before** they are returned to the client. They do not reduce the amount of data scanned.

**Modern syntax**:
```json
{
  "FilterExpression": "#status = :active AND age > :minAge",
  "ExpressionAttributeNames": {"#status": "status"},
  "ExpressionAttributeValues": {":active": {"S": "active"}, ":minAge": {"N": "21"}}
}
```

**Legacy syntax** (auto-converted):

For Query:
```json
{
  "QueryFilter": {
    "status": {
      "ComparisonOperator": "EQ",
      "AttributeValueList": [{"S": "active"}]
    }
  },
  "ConditionalOperator": "AND"
}
```

For Scan:
```json
{
  "ScanFilter": {
    "status": {
      "ComparisonOperator": "EQ",
      "AttributeValueList": [{"S": "active"}]
    }
  }
}
```

### 5.8 Pagination

All list/query/scan operations support pagination:

| API | Cursor Parameter (Request) | Cursor Parameter (Response) |
|---|---|---|
| ListTables | `ExclusiveStartTableName` | `LastEvaluatedTableName` |
| Query | `ExclusiveStartKey` | `LastEvaluatedKey` |
| Scan | `ExclusiveStartKey` | `LastEvaluatedKey` |
| ListStreams | `ExclusiveStartStreamArn` | `LastEvaluatedStreamArn` |
| DescribeStream | `ExclusiveStartShardId` | `LastEvaluatedShardId` |
| BatchGetItem | (via `UnprocessedKeys`) | `UnprocessedKeys` |
| GetRecords | `ShardIterator` | `NextShardIterator` |

**Pagination rules:**
- When the response includes a cursor value, there are more results to fetch
- Pass the cursor value in the next request to get the next page
- When the cursor is absent/null, all results have been returned
- **Size limits**: Query/Scan/GetRecords enforce a **1 MB** response size limit; BatchGetItem enforces a **16 MB** limit

### 5.9 Size Limits

| Limit | Value | APIs Affected |
|---|---|---|
| Query/Scan response size | 1 MB | Query, Scan |
| ListTables response size | 1 MB | ListTables |
| BatchGetItem response size | 16 MB | BatchGetItem |
| GetRecords response size | 1 MB | GetRecords |
| Query result limit (max per page) | 100 items OR 1 MB, whichever comes first | Query |
| Scan result limit (max per page) | 100 items OR 1 MB, whichever comes first | Scan |
| GetRecords limit (max per page) | 50 records | GetRecords |
| ListTables default limit | 100 tables | ListTables |
| ListStreams default limit | 100 streams | ListStreams |
| DescribeStream shard limit | 100 shards | DescribeStream |
| BatchWriteItem max items | 25 items | BatchWriteItem |
| BatchGetItem max keys | 100 keys | BatchGetItem |

---

## 6. DDL APIs (Table Management)

### 6.1 CreateTable

Creates a new table in Phoenix with the specified key schema, attributes, optional indexes, and optional change streams.

**X-Amz-Target**: `DynamoDB_20120810.CreateTable`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Name of the table to create |
| `KeySchema` | List | Yes | Key elements (1 or 2 elements, see below) |
| `AttributeDefinitions` | List | Yes | Type definitions for key attributes |
| `GlobalSecondaryIndexes` | List | No | Global secondary indexes to create |
| `LocalSecondaryIndexes` | List | No | Local secondary indexes to create |
| `StreamSpecification` | Map | No | Enable change data capture stream |

**KeySchema element structure:**
```json
{
  "AttributeName": "id",
  "KeyType": "HASH"
}
```
- `KeyType` must be `HASH` (partition key) or `RANGE` (sort key)
- First element should be `HASH`; second (if present) should be `RANGE`
- A table can have **1 key** (HASH only) or **2 keys** (HASH + RANGE)

**AttributeDefinitions element structure:**
```json
{
  "AttributeName": "id",
  "AttributeType": "S"
}
```
- `AttributeType`: `S` (String/VARCHAR), `N` (Number/DOUBLE), `B` (Binary/VARBINARY_ENCODED)

**GlobalSecondaryIndexes / LocalSecondaryIndexes element structure:**
```json
{
  "IndexName": "status-index",
  "KeySchema": [
    {"AttributeName": "status", "KeyType": "HASH"},
    {"AttributeName": "created_at", "KeyType": "RANGE"}
  ]
}
```
- **GSI**: Hash key differs from the table's hash key
- **LSI**: Hash key is the same as the table's hash key (sort key differs)
- Indexes are created as `UNCOVERED INDEX` with `BSON_VALUE()` expressions

**StreamSpecification structure:**
```json
{
  "StreamEnabled": true,
  "StreamViewType": "NEW_AND_OLD_IMAGES"
}
```
- `StreamViewType` values: `NEW_IMAGE`, `OLD_IMAGE`, `NEW_AND_OLD_IMAGES`
- Required when `StreamEnabled` is `true`

#### Response

```json
{
  "TableDescription": {
    "TableName": "MyTable",
    "TableStatus": "ACTIVE",
    "KeySchema": [...],
    "AttributeDefinitions": [...],
    "CreationDateTime": 1700000000.000,
    "BillingModeSummary": {"BillingMode": "PROVISIONED"},
    "GlobalSecondaryIndexes": [...],
    "LocalSecondaryIndexes": [...],
    "StreamSpecification": {...},
    "LatestStreamArn": "...",
    "LatestStreamLabel": "..."
  }
}
```

#### Validations

- Hash key must be present in `KeySchema`
- All key attributes in `KeySchema` must have a matching `AttributeDefinitions` entry
- Attribute types must be `S`, `N`, or `B`
- If `StreamEnabled` is `true`, `StreamViewType` must be non-empty
- If the table already exists (created more than 5 seconds ago), throws 400 with `ResourceInUseException`

#### Phoenix SQL Generated

```sql
CREATE TABLE IF NOT EXISTS "SCHEMA"."MyTable" (
  "id" VARCHAR NOT NULL,
  "COL" BSON,
  CONSTRAINT pk PRIMARY KEY ("id")
) IS_STRICT_TTL=false, UPDATE_CACHE_FREQUENCY=1800000, ...
```

For tables with a sort key:
```sql
CREATE TABLE IF NOT EXISTS "SCHEMA"."MyTable" (
  "id" VARCHAR NOT NULL,
  "sort_key" DOUBLE NOT NULL,
  "COL" BSON,
  CONSTRAINT pk PRIMARY KEY ("id", "sort_key")
) ...
```

For indexes:
```sql
CREATE UNCOVERED INDEX IF NOT EXISTS "status-index"
  ON "SCHEMA"."MyTable" (BSON_VALUE("COL", 'status', 'VARCHAR'), BSON_VALUE("COL", 'created_at', 'DOUBLE'))
  WHERE BSON_VALUE("COL", 'status', 'VARCHAR') IS NOT NULL
```

---

### 6.2 DeleteTable

Drops a table and all its indexes (CASCADE).

**X-Amz-Target**: `DynamoDB_20120810.DeleteTable`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Name of the table to delete |

#### Response

```json
{
  "TableDescription": {
    "TableName": "MyTable",
    "TableStatus": "ACTIVE",
    "KeySchema": [...],
    "AttributeDefinitions": [...],
    "CreationDateTime": 1700000000.000,
    ...
  }
}
```

The response contains the table description as it was **before** deletion.

#### Phoenix SQL Generated

```sql
DROP TABLE "SCHEMA"."MyTable" CASCADE
```

---

### 6.3 DescribeTable

Returns the full description of a table including its schema, indexes, stream configuration, and status.

**X-Amz-Target**: `DynamoDB_20120810.DescribeTable`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Name of the table to describe |

#### Response

```json
{
  "Table": {
    "TableName": "MyTable",
    "TableStatus": "ACTIVE",
    "KeySchema": [
      {"AttributeName": "pk", "KeyType": "HASH"},
      {"AttributeName": "sk", "KeyType": "RANGE"}
    ],
    "AttributeDefinitions": [
      {"AttributeName": "pk", "AttributeType": "S"},
      {"AttributeName": "sk", "AttributeType": "N"}
    ],
    "CreationDateTime": 1700000000.000,
    "BillingModeSummary": {"BillingMode": "PROVISIONED"},
    "ProvisionedThroughput": {
      "ReadCapacityUnits": 0,
      "WriteCapacityUnits": 0
    },
    "GlobalSecondaryIndexes": [
      {
        "IndexName": "gsi-name",
        "KeySchema": [...],
        "IndexStatus": "ACTIVE",
        "Projection": {"ProjectionType": "ALL"}
      }
    ],
    "LocalSecondaryIndexes": [...],
    "StreamSpecification": {
      "StreamEnabled": true,
      "StreamViewType": "NEW_AND_OLD_IMAGES"
    },
    "LatestStreamArn": "phoenix/cdc/stream/...",
    "LatestStreamLabel": "2024-01-15T10:30:00Z"
  }
}
```

**Index Status values**: `ACTIVE`, `CREATING`, `DELETING`

---

### 6.4 ListTables

Returns a list of all table names. Supports pagination.

**X-Amz-Target**: `DynamoDB_20120810.ListTables`

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `ExclusiveStartTableName` | String | No | `null` | Pagination cursor: returns tables after this name (lexicographic order) |
| `Limit` | Integer | No | `100` | Max table names to return |

#### Response

```json
{
  "TableNames": ["Table1", "Table2", "Table3"],
  "LastEvaluatedTableName": "Table3"
}
```

- `LastEvaluatedTableName` is only present when there are more results (limit reached or 1 MB size limit hit)

---

### 6.5 UpdateTable

Modifies an existing table: add/remove indexes or enable change streams.

**X-Amz-Target**: `DynamoDB_20120810.UpdateTable`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Table to update |
| `GlobalSecondaryIndexUpdates` | List | No | Index operations (Create or Delete) |
| `AttributeDefinitions` | List | Conditional | Required when creating a new index |
| `StreamSpecification` | Map | No | Enable streams (only Disabled -> Enabled supported) |

**GlobalSecondaryIndexUpdates element structure:**

To **create** an index:
```json
{
  "Create": {
    "IndexName": "new-index",
    "KeySchema": [
      {"AttributeName": "field1", "KeyType": "HASH"}
    ]
  }
}
```

To **delete** (disable) an index:
```json
{
  "Delete": {
    "IndexName": "old-index"
  }
}
```

#### Response

```json
{
  "TableDescription": { ... }
}
```

#### Validations

- `Create` and `Delete` operations are supported to create or drop indexes
- Stream transitions:
  - Disabled -> Enabled: **Allowed** (the only valid transition)
  - Disabled -> Disabled: throws 400 ("Stream is already disabled.")
  - Enabled -> Disabled: throws 400 ("Disabling a stream is not yet supported.")
  - Enabled -> Enabled: throws 400 ("Table already has an enabled stream.")

#### Special Behaviors

- Index **deletion** uses `ALTER INDEX ... DISABLE` (disables rather than drops)
- New indexes are created **asynchronously** with initial state `CREATE_DISABLE`
- When enabling streams, additionally sets `MERGE_ENABLED=false` on the table

---

### 6.6 UpdateTimeToLive

Enables or disables Time To Live (TTL) on a table.

**X-Amz-Target**: `DynamoDB_20120810.UpdateTimeToLive`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Table name |
| `TimeToLiveSpecification` | Map | Yes | TTL configuration (see below) |

**TimeToLiveSpecification structure:**
```json
{
  "AttributeName": "expiry_time",
  "Enabled": true
}
```

#### Response

```json
{
  "TimeToLiveSpecification": {
    "AttributeName": "expiry_time",
    "Enabled": true
  }
}
```

#### Phoenix SQL Generated

Enable TTL:
```sql
ALTER TABLE "SCHEMA"."MyTable" SET TTL = '<ttl_expression based on attribute>'
```

Disable TTL:
```sql
ALTER TABLE "SCHEMA"."MyTable" SET TTL = 'FOREVER'
```

---

### 6.7 DescribeTimeToLive

Returns the TTL configuration for a table.

**X-Amz-Target**: `DynamoDB_20120810.DescribeTimeToLive`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Table name |

#### Response

When TTL is enabled:
```json
{
  "TimeToLiveDescription": {
    "TimeToLiveStatus": "ENABLED",
    "AttributeName": "expiry_time"
  }
}
```

When TTL is disabled:
```json
{
  "TimeToLiveDescription": {
    "TimeToLiveStatus": "DISABLED"
  }
}
```

---

### 6.8 DescribeContinuousBackups

Returns the continuous backup/PITR configuration. This is a **stub** -- Phoenix does not support this feature, so it always returns `DISABLED`.

**X-Amz-Target**: `DynamoDB_20120810.DescribeContinuousBackups`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Table name (validated for existence) |

#### Response (always)

```json
{
  "ContinuousBackupsDescription": {
    "ContinuousBackupsStatus": "DISABLED",
    "PointInTimeRecoveryDescription": {
      "PointInTimeRecoveryStatus": "DISABLED"
    }
  }
}
```

---

## 7. DML APIs (Data Modification)

### 7.1 PutItem

Creates a new item or replaces an existing item with the same primary key. Supports conditional writes and returning the old item.

**X-Amz-Target**: `DynamoDB_20120810.PutItem`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Target table |
| `Item` | Map | Yes | The full item to write (must include PK attributes) |
| `ConditionExpression` | String | No | Condition that must be satisfied |
| `ExpressionAttributeNames` | Map | No | Name aliases for the expression |
| `ExpressionAttributeValues` | Map | No | Typed value placeholders |
| `ReturnValues` | String | No | `NONE` (default) or `ALL_OLD` |
| `ReturnValuesOnConditionCheckFailure` | String | No | `NONE` or `ALL_OLD` |
| `Expected` | Map | No | Legacy conditional (auto-converted if `ConditionExpression` is null) |
| `ConditionalOperator` | String | No | `AND` or `OR` (used with `Expected`) |

**Item structure:**
```json
{
  "Item": {
    "id": {"S": "user-123"},
    "name": {"S": "John Doe"},
    "age": {"N": "30"},
    "active": {"BOOL": true},
    "tags": {"SS": ["admin", "user"]},
    "metadata": {"M": {"key1": {"S": "value1"}}}
  }
}
```

#### Response

Without `ReturnValues`:
```json
{
  "ConsumedCapacity": {
    "ReadCapacityUnits": 1.0,
    "WriteCapacityUnits": 1.0,
    "CapacityUnits": 2.0
  }
}
```

With `ReturnValues: ALL_OLD` (when old item existed):
```json
{
  "ConsumedCapacity": { ... },
  "Attributes": {
    "id": {"S": "user-123"},
    "name": {"S": "Old Name"},
    ...
  }
}
```

#### Validations

- `ReturnValues` must be `NONE` or `ALL_OLD`
- `ConditionExpression` and `Expected` are mutually exclusive (throws 400; use one or the other)

#### Conditional Write Behavior

When a `ConditionExpression` is provided, the service evaluates whether the condition can be satisfied on an empty/non-existing item:
- **If yes** (e.g., `attribute_not_exists(id)`): Uses `ON DUPLICATE KEY UPDATE` -- allows both insert and conditional update
- **If no** (e.g., `attribute_exists(id)`): Uses `ON DUPLICATE KEY UPDATE_ONLY` -- only updates existing items

---

### 7.2 UpdateItem

Modifies specific attributes of an existing item (or creates it if using `SET` operations without a restricting condition).

**X-Amz-Target**: `DynamoDB_20120810.UpdateItem`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Target table |
| `Key` | Map | Yes | Primary key of the item to update |
| `UpdateExpression` | String | No* | Modern update expression |
| `AttributeUpdates` | Map | No* | Legacy update format (*mutually exclusive with `UpdateExpression`*) |
| `ConditionExpression` | String | No | Condition that must be satisfied |
| `ExpressionAttributeNames` | Map | No | Name aliases |
| `ExpressionAttributeValues` | Map | No | Value placeholders |
| `ReturnValues` | String | No | `NONE`, `ALL_OLD`, or `ALL_NEW` |
| `ReturnValuesOnConditionCheckFailure` | String | No | `NONE` or `ALL_OLD` |
| `Expected` | Map | No | Legacy conditional |
| `ConditionalOperator` | String | No | `AND` or `OR` |

**Key structure:**
```json
{
  "Key": {
    "id": {"S": "user-123"},
    "sort_key": {"N": "1"}
  }
}
```

**UpdateExpression syntax:**
```
SET #name = :newName, age = :newAge
REMOVE obsolete_field
ADD view_count :increment
DELETE tags :tagsToRemove
```

Supported clauses:
- `SET` -- Set attribute values
- `REMOVE` -- Remove attributes
- `ADD` -- Add to number or add elements to a set
- `DELETE` -- Remove elements from a set

**Legacy AttributeUpdates format:**
```json
{
  "AttributeUpdates": {
    "name": {"Action": "PUT", "Value": {"S": "New Name"}},
    "counter": {"Action": "ADD", "Value": {"N": "1"}},
    "old_field": {"Action": "DELETE"}
  }
}
```

| Legacy Action | BSON Equivalent | Description |
|---|---|---|
| `PUT` | `$SET` | Set attribute value |
| `ADD` | `$ADD` | Add to number or set |
| `DELETE` (with value) | `$DELETE_FROM_SET` | Remove elements from set |
| `DELETE` (no value) | `$UNSET` | Remove the attribute |

#### Response

```json
{
  "ConsumedCapacity": { ... },
  "Attributes": { ... }
}
```
- `Attributes` is present only when `ReturnValues` is `ALL_OLD` or `ALL_NEW`

#### Validations

- `UpdateExpression` and `AttributeUpdates` are mutually exclusive (throws 400; use one or the other)
- `ConditionExpression` and `Expected` are mutually exclusive (throws 400; use one or the other)
- `ReturnValues` must be `NONE`, `ALL_OLD`, or `ALL_NEW` (`UPDATED_OLD` and `UPDATED_NEW` throw 400; use `ALL_OLD` or `ALL_NEW` instead)
- Invalid update expression paths throw 400 with `ValidationException("Invalid document path used for update")`

---

### 7.3 DeleteItem

Deletes a single item by primary key. Supports conditional deletes and returning the old item.

**X-Amz-Target**: `DynamoDB_20120810.DeleteItem`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Target table |
| `Key` | Map | Yes | Primary key of the item to delete |
| `ConditionExpression` | String | No | Condition that must be satisfied |
| `ExpressionAttributeNames` | Map | No | Name aliases |
| `ExpressionAttributeValues` | Map | No | Value placeholders |
| `ReturnValues` | String | No | `NONE` (default) or `ALL_OLD` |
| `ReturnValuesOnConditionCheckFailure` | String | No | `NONE` or `ALL_OLD` |
| `Expected` | Map | No | Legacy conditional |
| `ConditionalOperator` | String | No | `AND` or `OR` |

#### Response

Without `ReturnValues`:
```json
{
  "ConsumedCapacity": { ... }
}
```

With `ReturnValues: ALL_OLD`:
```json
{
  "ConsumedCapacity": { ... },
  "Attributes": {
    "id": {"S": "user-123"},
    "name": {"S": "Deleted User"},
    ...
  }
}
```

#### Validations

- `ReturnValues` must be `NONE` or `ALL_OLD`
- `ConditionExpression` and `Expected` are mutually exclusive (throws 400; use one or the other)

#### Phoenix SQL Generated

Simple delete:
```sql
DELETE FROM "SCHEMA"."MyTable" WHERE "id" = ?
```

Conditional delete:
```sql
DELETE FROM "SCHEMA"."MyTable" WHERE "id" = ? AND BSON_CONDITION_EXPRESSION(COL, ?)
```

---

### 7.4 BatchWriteItem

Performs up to 25 put or delete operations across one or more tables in a single call. All operations are committed atomically.

**X-Amz-Target**: `DynamoDB_20120810.BatchWriteItem`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `RequestItems` | Map | Yes | Map of table name to list of write requests |

**RequestItems structure:**
```json
{
  "RequestItems": {
    "Table1": [
      {
        "PutRequest": {
          "Item": {
            "id": {"S": "1"},
            "name": {"S": "Alice"}
          }
        }
      },
      {
        "DeleteRequest": {
          "Key": {
            "id": {"S": "2"}
          }
        }
      }
    ],
    "Table2": [
      {
        "PutRequest": {
          "Item": {
            "pk": {"S": "A"},
            "data": {"S": "hello"}
          }
        }
      }
    ]
  }
}
```

Each write request must contain exactly one of:
- `PutRequest` with `Item` (full item to put)
- `DeleteRequest` with `Key` (primary key to delete)

#### Response

```json
{
  "UnprocessedItems": {}
}
```

`UnprocessedItems` is always empty (all items succeed or the entire batch fails atomically).

#### Validations

- Maximum **25** items total across all tables (throws 400 if exceeded)
- No duplicate primary keys within the same table across both puts and deletes (throws 400 if duplicates found)
- Each write request must be either `PutRequest` or `DeleteRequest` (throws 400 otherwise)

#### Special Behaviors

- **Transactional**: Uses `connection.setAutoCommit(false)` + `connection.commit()`. All operations succeed or fail together.
- **No conditional expressions**: Individual put/delete requests do not support `ConditionExpression` or `ReturnValues`

---

## 8. DQL APIs (Data Query/Read)

### 8.1 GetItem

Retrieves a single item by its primary key.

**X-Amz-Target**: `DynamoDB_20120810.GetItem`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Target table |
| `Key` | Map | Yes | Primary key attributes |
| `ProjectionExpression` | String | No | Attributes to return |
| `ExpressionAttributeNames` | Map | No | Name aliases for projection |
| `AttributesToGet` | List | No | Legacy projection (mutually exclusive with `ProjectionExpression`; throws 400 if both specified) |

#### Response

Item found:
```json
{
  "Item": {
    "id": {"S": "user-123"},
    "name": {"S": "John Doe"},
    "age": {"N": "30"}
  },
  "ConsumedCapacity": { ... }
}
```

Item not found (no `Item` key in response):
```json
{
  "ConsumedCapacity": { ... }
}
```

---

### 8.2 BatchGetItem

Retrieves multiple items by primary key across one or more tables.

**X-Amz-Target**: `DynamoDB_20120810.BatchGetItem`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `RequestItems` | Map | Yes | Map of table name to keys-and-attributes config |

**RequestItems structure:**
```json
{
  "RequestItems": {
    "Table1": {
      "Keys": [
        {"id": {"S": "1"}},
        {"id": {"S": "2"}},
        {"id": {"S": "3"}}
      ],
      "ProjectionExpression": "name, #s",
      "ExpressionAttributeNames": {"#s": "status"}
    },
    "Table2": {
      "Keys": [
        {"pk": {"S": "A"}, "sk": {"N": "1"}}
      ]
    }
  }
}
```

Each table's configuration supports:
| Field | Type | Description |
|---|---|---|
| `Keys` | List | List of primary key maps (required) |
| `ProjectionExpression` | String | Attributes to return |
| `ExpressionAttributeNames` | Map | Name aliases |
| `AttributesToGet` | List | Legacy projection |

#### Response

```json
{
  "Responses": {
    "Table1": [
      {"id": {"S": "1"}, "name": {"S": "Alice"}, "status": {"S": "active"}},
      {"id": {"S": "2"}, "name": {"S": "Bob"}, "status": {"S": "active"}}
    ],
    "Table2": [
      {"pk": {"S": "A"}, "sk": {"N": "1"}, "data": {"S": "hello"}}
    ]
  },
  "UnprocessedKeys": {
    "Table1": {
      "Keys": [{"id": {"S": "3"}}],
      "ProjectionExpression": "name, #s",
      "ExpressionAttributeNames": {"#s": "status"}
    }
  }
}
```

#### Validations

- Maximum **100** keys total across all tables (throws 400 if exceeded)

#### Special Behaviors

- **16 MB response limit**: As items are retrieved, serialized sizes are tracked. When the cumulative size exceeds 16 MB, remaining keys are moved to `UnprocessedKeys` along with their original projection/expression metadata.
- All keys for a given table are fetched in a single SQL query using `WHERE pk IN (?, ?, ...)`

---

### 8.3 Query

Retrieves items from a table or index based on primary key conditions. Items are always returned sorted by sort key.

**X-Amz-Target**: `DynamoDB_20120810.Query`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Target table |
| `IndexName` | String | No | Secondary index to query |
| `KeyConditionExpression` | String | Yes* | Key condition expression |
| `ExpressionAttributeNames` | Map | No | Name aliases |
| `ExpressionAttributeValues` | Map | No | Value placeholders |
| `FilterExpression` | String | No | Post-read filter |
| `ProjectionExpression` | String | No | Attributes to return |
| `Select` | String | No | What to return (see below) |
| `Limit` | Integer | No | Max items to return (capped at 100 items OR 1 MB, whichever comes first) |
| `ScanIndexForward` | Boolean | No | `true` (default) = ASC, `false` = DESC |
| `ExclusiveStartKey` | Map | No | Pagination cursor from previous response |
| `KeyConditions` | Map | No | Legacy key conditions (*mutually exclusive with `KeyConditionExpression`*) |
| `QueryFilter` | Map | No | Legacy filter (*mutually exclusive with `FilterExpression`*) |
| `ConditionalOperator` | String | No | Used with `QueryFilter` |

**Select values:**

| Value | Description |
|---|---|
| `ALL_ATTRIBUTES` | Return all attributes (default) |
| `SPECIFIC_ATTRIBUTES` | Return only projected attributes (requires `ProjectionExpression`) |
| `COUNT` | Return only the count, no items |

**KeyConditionExpression patterns supported:**

| Pattern | Example |
|---|---|
| Partition key only | `pk = :pk_val` |
| Partition + sort key equality | `pk = :pk_val AND sk = :sk_val` |
| Partition + sort key comparison | `pk = :pk_val AND sk > :sk_val` |
| Partition + sort key range | `pk = :pk_val AND sk BETWEEN :lo AND :hi` |
| Partition + sort key prefix | `pk = :pk_val AND begins_with(sk, :prefix)` |

Supported sort key operators: `=`, `<`, `>`, `<=`, `>=`, `BETWEEN`, `begins_with`

#### Response

```json
{
  "Items": [
    {"id": {"S": "user-1"}, "sort": {"N": "1"}, "name": {"S": "Alice"}},
    {"id": {"S": "user-1"}, "sort": {"N": "2"}, "name": {"S": "Bob"}}
  ],
  "Count": 2,
  "ScannedCount": 5,
  "ConsumedCapacity": { ... },
  "LastEvaluatedKey": {
    "id": {"S": "user-1"},
    "sort": {"N": "2"}
  }
}
```

- `Count`: Number of items returned (after filtering)
- `ScannedCount`: Total items scanned (before filtering)
- `LastEvaluatedKey`: Present when there are more results; use as `ExclusiveStartKey` in the next request
- When `Select: COUNT`, the response omits `Items` and only has `Count` and `ScannedCount`

#### Validations

- Cannot use both `KeyConditionExpression` and `KeyConditions` (throws 400; use one or the other)
- Cannot use both `FilterExpression` and `QueryFilter` (throws 400; use one or the other)
- Cannot use both `ProjectionExpression` and `AttributesToGet` (throws 400; use one or the other)
- `Select: SPECIFIC_ATTRIBUTES` requires `ProjectionExpression` (throws 400 if missing)
- `Select: ALL_ATTRIBUTES` is incompatible with `ProjectionExpression` (throws 400 if both specified)

---

### 8.4 Scan

Returns all items from a table or index (full table scan). Supports filtering and parallel segment scanning.

**X-Amz-Target**: `DynamoDB_20120810.Scan`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `TableName` | String | Yes | Target table |
| `IndexName` | String | No | Secondary index to scan |
| `FilterExpression` | String | No | Post-scan filter |
| `ExpressionAttributeNames` | Map | No | Name aliases |
| `ExpressionAttributeValues` | Map | No | Value placeholders |
| `ProjectionExpression` | String | No | Attributes to return |
| `Select` | String | No | `ALL_ATTRIBUTES`, `SPECIFIC_ATTRIBUTES`, or `COUNT` |
| `Limit` | Integer | No | Max items per page (capped at 100 items OR 1 MB, whichever comes first) |
| `ExclusiveStartKey` | Map | No | Pagination cursor |
| `Segment` | Integer | No | Segment number for parallel scan |
| `TotalSegments` | Integer | No | Total segments for parallel scan |
| `ScanFilter` | Map | No | Legacy filter (*mutually exclusive with `FilterExpression`*) |
| `ConditionalOperator` | String | No | Used with `ScanFilter` |

#### Response

Same structure as Query response:
```json
{
  "Items": [...],
  "Count": 10,
  "ScannedCount": 50,
  "ConsumedCapacity": { ... },
  "LastEvaluatedKey": { ... }
}
```

#### Parallel Scan

To scan a table in parallel, split the work across multiple threads/workers:

```json
// Worker 0
{"TableName": "MyTable", "Segment": 0, "TotalSegments": 4}

// Worker 1
{"TableName": "MyTable", "Segment": 1, "TotalSegments": 4}

// Worker 2
{"TableName": "MyTable", "Segment": 2, "TotalSegments": 4}

// Worker 3
{"TableName": "MyTable", "Segment": 3, "TotalSegments": 4}
```

**How segments work:**
- The system uses HBase region split boundaries to assign data ranges to segments
- Segment metadata is cached in a Phoenix table (`PHOENIX_DDB_SEGMENT_RANGE`) with TTL = 5400 seconds

#### Pagination with Composite Keys

When scanning a table with composite primary key (HASH + RANGE) and an `ExclusiveStartKey` is provided, the scan executes **two queries**:
1. Items in the same partition after the cursor: `pk1 = ? AND pk2 > ?`
2. Items in subsequent partitions: `pk1 > ?`

Results from both queries are merged into a single response.

#### Validations

- `Segment` must be >= 0 and < `TotalSegments` (throws 400 if out of range)
- Cannot use both `FilterExpression` and `ScanFilter` (throws 400; use one or the other)
- Cannot use both `ProjectionExpression` and `AttributesToGet` (throws 400; use one or the other)

---

## 9. Change Stream APIs

Change streams allow you to capture item-level changes (inserts, updates, deletes) from a table. This is implemented using Phoenix's CDC (Change Data Capture) feature.

### Workflow

```
1. Enable streams on a table (CreateTable or UpdateTable with StreamSpecification)
2. List available streams (ListStreams)
3. Describe a stream to get its shards (DescribeStream)
4. Get a shard iterator for a specific shard (GetShardIterator)
5. Read records using the shard iterator (GetRecords)
6. Continue reading with NextShardIterator until null
```

### Stream View Types

| Type | OldImage | NewImage | Description |
|---|---|---|---|
| `NEW_IMAGE` | No | Yes | Post-modification state only |
| `OLD_IMAGE` | Yes | No | Pre-modification state only |
| `NEW_AND_OLD_IMAGES` | Yes | Yes | Both pre- and post-modification states |

---

### 9.1 ListStreams

Returns a list of all streams, optionally filtered by table name.

**X-Amz-Target**: `DynamoDB_20120810.ListStreams`

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `TableName` | String | No | `null` | Filter streams for a specific table |
| `ExclusiveStartStreamArn` | String | No | `null` | Pagination cursor |
| `Limit` | Integer | No | `100` | Max streams to return |

#### Response

```json
{
  "Streams": [
    {
      "TableName": "MyTable",
      "StreamArn": "phoenix/cdc/stream/MyTable/...",
      "StreamLabel": "2024-01-15T10:30:00Z"
    }
  ],
  "LastEvaluatedStreamArn": "phoenix/cdc/stream/MyTable/..."
}
```

`LastEvaluatedStreamArn` is present only when the result count equals the limit.

---

### 9.2 DescribeStream

Returns detailed information about a stream including its shards.

**X-Amz-Target**: `DynamoDB_20120810.DescribeStream`

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `StreamArn` | String | Yes | | Stream ARN from ListStreams |
| `ExclusiveStartShardId` | String | No | `null` | Pagination cursor for shards |
| `Limit` | Integer | No | `100` | Max shards to return |

#### Response

```json
{
  "StreamDescription": {
    "StreamArn": "phoenix/cdc/stream/MyTable/...",
    "TableName": "MyTable",
    "StreamLabel": "2024-01-15T10:30:00Z",
    "StreamViewType": "NEW_AND_OLD_IMAGES",
    "CreationRequestDateTime": 1700000000.000,
    "KeySchema": [
      {"AttributeName": "id", "KeyType": "HASH"}
    ],
    "StreamStatus": "ENABLED",
    "Shards": [
      {
        "ShardId": "partition-1",
        "ParentShardId": "parent-partition-0",
        "SequenceNumberRange": {
          "StartingSequenceNumber": "170000000000000",
          "EndingSequenceNumber": "170100000099999"
        }
      }
    ],
    "LastEvaluatedShardId": "partition-1"
  }
}
```

- Shards are only listed when `StreamStatus` is `ENABLED`
- `EndingSequenceNumber` is only present for closed shards (after a split)
- `LastEvaluatedShardId` is present only when shard count equals the limit

---

### 9.3 GetShardIterator

Gets a shard iterator for reading records from a specific position in a shard.

**X-Amz-Target**: `DynamoDB_20120810.GetShardIterator`

#### Request Parameters

| Parameter | Type | Required | Description |
|---|---|---|---|
| `StreamArn` | String | Yes | Stream ARN |
| `ShardId` | String | Yes | Shard/partition ID |
| `ShardIteratorType` | String | Yes | Where to start reading (see below) |
| `SequenceNumber` | String | Conditional | Required for `AT_SEQUENCE_NUMBER` and `AFTER_SEQUENCE_NUMBER` |

**ShardIteratorType values:**

| Type | Description |
|---|---|
| `TRIM_HORIZON` | Start from the oldest available record in the shard |
| `LATEST` | Start from new records written after this call |
| `AT_SEQUENCE_NUMBER` | Start at the exact sequence number specified |
| `AFTER_SEQUENCE_NUMBER` | Start at the record after the specified sequence number |

#### Response

```json
{
  "ShardIterator": "shardIterator/tableName/cdcObject/streamType/shardId/12345"
}
```

The shard iterator is an encoded string containing: table name, CDC object name, stream type, shard ID, and starting sequence number.

---

### 9.4 GetRecords

Reads change records from a shard using a shard iterator.

**X-Amz-Target**: `DynamoDB_20120810.GetRecords`

#### Request Parameters

| Parameter | Type | Required | Default | Description |
|---|---|---|---|---|
| `ShardIterator` | String | Yes | | Shard iterator from GetShardIterator or previous GetRecords |
| `Limit` | Integer | No | `50` | Max records to return (capped at 50 records OR 1 MB, whichever comes first) |

#### Response

```json
{
  "Records": [
    {
      "eventName": "INSERT",
      "dynamodb": {
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "SequenceNumber": "170000000000001",
        "ApproximateCreationDateTime": 1700000000.123,
        "Keys": {
          "id": {"S": "user-123"}
        },
        "NewImage": {
          "id": {"S": "user-123"},
          "name": {"S": "John Doe"},
          "age": {"N": "30"}
        },
        "SizeBytes": 256
      }
    },
    {
      "eventName": "MODIFY",
      "dynamodb": {
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "SequenceNumber": "170000000000002",
        "ApproximateCreationDateTime": 1700000001.456,
        "Keys": {"id": {"S": "user-123"}},
        "OldImage": {"id": {"S": "user-123"}, "name": {"S": "John Doe"}, "age": {"N": "30"}},
        "NewImage": {"id": {"S": "user-123"}, "name": {"S": "John Doe"}, "age": {"N": "31"}},
        "SizeBytes": 512
      }
    },
    {
      "eventName": "REMOVE",
      "dynamodb": {
        "StreamViewType": "NEW_AND_OLD_IMAGES",
        "SequenceNumber": "170000000000003",
        "ApproximateCreationDateTime": 1700000002.789,
        "Keys": {"id": {"S": "user-456"}},
        "OldImage": {"id": {"S": "user-456"}, "name": {"S": "Jane"}},
        "SizeBytes": 128
      },
      "userIdentity": {
        "Type": "Service",
        "PrincipalId": "phoenix/hbase"
      }
    }
  ],
  "NextShardIterator": "shardIterator/tableName/cdcObject/streamType/shardId/170000000000004"
}
```

**Event types:**

| eventName | Meaning | OldImage | NewImage |
|---|---|---|---|
| `INSERT` | New item created | Absent | Present |
| `MODIFY` | Existing item updated | Present | Present |
| `REMOVE` | Item deleted | Present | Absent |

**Special fields:**
- `userIdentity`: Only present for TTL-based deletions (automatic expiry). Indicates the deletion was performed by the system rather than a user.
- `NextShardIterator`: `null` when the shard is closed and all records have been consumed. Use this to detect the end of a shard.

**Image inclusion depends on StreamViewType:**
- `NEW_IMAGE`: Only `NewImage` is included
- `OLD_IMAGE`: Only `OldImage` is included
- `NEW_AND_OLD_IMAGES`: Both are included (when applicable based on event type)

---

## 10. Authentication

Authentication is **optional** and can be enabled by configuring a credential store implementation.

### Configuration

Set the credential store class in the server configuration:
```
phoenix.ddb.rest.auth.credential.store.class=com.example.MyCredentialStore
```

### Supported Authentication Methods

The `AccessKeyAuthFilter` supports three ways to provide credentials (checked in order):

| Method | Header | Format |
|---|---|---|
| AWS SigV4 | `Authorization` | `AWS4-HMAC-SHA256 Credential=AKID/...` (extracts access key ID only) |
| AccessKeyId format | `Authorization` | `AccessKeyId=AKID` |
| Custom header | `X-Access-Key-Id` | `AKID` |

### How It Works

1. The filter extracts the access key ID from the request
2. Looks up the key in the configured `CredentialStore`
3. If valid, sets `userName` and `accessKeyId` as request attributes and proceeds
4. If invalid, returns HTTP `403 Forbidden`

### CredentialStore Interface

To implement custom authentication, create a class that implements:
```java
public interface CredentialStore {
    UserCredentials getCredentials(String accessKeyId);
}
```

The `CredentialStore` can use any storage mechanism: database, file, LDAP, Vault, etc.

### When Authentication Is Disabled

When `phoenix.ddb.rest.auth.credential.store.class` is not configured, the auth filter is not registered and all requests are allowed without authentication. The AWS SDK still requires credentials to be set (use dummy values).

---

## 11. Error Handling

### Error Response Format

All errors are returned as HTTP `400 Bad Request` with JSON body:

```json
{
  "__type": "com.amazonaws.dynamodb.v20120810#<ExceptionType>",
  "message": "<Error description>"
}
```

### Exception Types

| Exception Type | Constant | When Thrown |
|---|---|---|
| `ValidationException` | `com.amazonaws.dynamodb.v20120810#ValidationException` | Invalid request parameters, unsupported operations |
| `ResourceNotFoundException` | `com.amazonaws.dynamodb.v20120810#ResourceNotFoundException` | Table does not exist |
| `ConditionalCheckFailedException` | `com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException` | Condition expression evaluated to false |
| `ResourceInUseException` | `com.amazonaws.dynamodb.v20120810#ResourceInUseException` | Table already exists (on CreateTable) |

### ConditionalCheckFailedException with Item

When `ReturnValuesOnConditionCheckFailure` is set to `ALL_OLD`, the error response includes the existing item:

```json
{
  "__type": "com.amazonaws.dynamodb.v20120810#ConditionalCheckFailedException",
  "message": "The conditional request failed",
  "Item": {
    "id": {"S": "user-123"},
    "name": {"S": "Existing Name"},
    ...
  }
}
```

---

## 12. Server Configuration

### Command Line

```bash
bin/phoenix-adapters rest start -p <port> -z <zk-quorum>
```

| Flag | Description | Default |
|---|---|---|
| `-p <port>` | HTTP listen port | `8842` |
| `-z <zk-quorum>` | ZooKeeper quorum for Phoenix connection | From HBase config or `ZOO_KEEPER_QUORUM` env var |

### Configuration Properties

| Property | Default | Description |
|---|---|---|
| `phoenix.ddb.rest.port` | `8842` | HTTP listen port |
| `phoenix.ddb.rest.host` | `0.0.0.0` | Bind address |
| `phoenix.ddb.zk.quorum` | (from HBase config) | ZooKeeper quorum string |
| `phoenix.ddb.rest.threads.max` | `125` | Max Jetty thread pool size |
| `phoenix.ddb.rest.threads.min` | `2` | Min Jetty thread pool size |
| `phoenix.ddb.rest.task.queue.size` | `-1` (unbounded) | Thread pool task queue size |
| `phoenix.ddb.rest.thread.idle.timeout` | `60000` (60s) | Thread idle timeout (ms) |
| `phoenix.ddb.rest.http.idle.timeout` | `30000` (30s) | HTTP connection idle timeout (ms) |
| `phoenix.ddb.rest.http.header.cache.size` | `65534` | HTTP header cache size |
| `phoenix.ddb.rest.connector.accept.queue.size` | `-1` (default) | TCP accept queue size |
| `phoenix.ddb.rest.http.allow.options.method` | `true` | Allow HTTP OPTIONS method |
| `phoenix.ddb.rest.connection.cleanup-interval` | `10000` (10s) | Connection cache cleanup interval (ms) |
| `phoenix.ddb.rest.connection.max-idletime` | `600000` (10min) | Max connection idle time (ms) |
| `phoenix.ddb.rest.support.proxyuser` | `false` | Enable proxy user support |
| `phoenix.ddb.rest.auth.credential.store.class` | (none) | Auth credential store class |
| `phoenix.ddb.rest.dns.interface` | `default` | DNS interface for hostname resolution |
| `phoenix.ddb.rest.dns.nameserver` | `default` | DNS nameserver |

### Phoenix Table Configuration (phoenix-table-options.properties)

```properties
IS_STRICT_TTL=false
UPDATE_CACHE_FREQUENCY=1800000
phoenix.max.lookback.age.seconds=97200
hbase.hregion.majorcompaction=172800000
org.apache.hadoop.hbase.index.lazy.post_batch.write=true
```

### Phoenix Index Configuration (phoenix-index-options.properties)

```properties
hbase.hregion.majorcompaction=172800000
```

### Environment Variables

| Variable | Description |
|---|---|
| `ZOO_KEEPER_QUORUM` | ZooKeeper quorum (alternative to `-z` flag) |
| `JAVA_HOME` | Path to Java installation |
| `PHOENIX_ADAPTERS_HOME` | Installation root directory |
| `PHOENIX_ADAPTERS_CONF_DIR` | Configuration directory |
| `PHOENIX_ADAPTERS_LOG_DIR` | Log directory |
| `PHOENIX_REST_HEAPSIZE` | JVM max heap size (e.g., `2g`) |
| `PHOENIX_REST_OFFHEAPSIZE` | JVM max off-heap size (e.g., `1g`) |
| `PHOENIX_REST_OPTS` | Additional JVM options |
| `PHOENIX_DDB_REST_OPTS` | Additional JVM options for REST server |

---

## 13. Metrics & Monitoring

### JMX Endpoint

The server exposes JMX metrics at `http://<host>:<port>/jmx` (no authentication required even when auth is enabled).

### Per-API Metrics

Each API operation tracks:
- **Success time**: `<Operation>SuccessTime` -- duration in milliseconds for successful calls
- **Failure time**: `<Operation>FailureTime` -- duration in milliseconds for failed calls
- **Request count**: Total number of incoming requests

### Operations Tracked

- CreateTable
- DeleteTable
- DescribeTable
- DescribeContinuousBackups
- ListTables
- UpdateTable
- PutItem
- UpdateItem
- DeleteItem
- GetItem
- BatchGetItem
- BatchWriteItem
- Query
- Scan
- UpdateTimeToLive
- DescribeTimeToLive
- ListStreams
- DescribeStream
- GetShardIterator
- GetRecords

---

## 14. Limitations & Differences from AWS DynamoDB

### Unsupported Features

| Feature | Status                                             |
|---|----------------------------------------------------|
| `UPDATED_OLD` return value (UpdateItem) | Not supported (throws 400), use `ALL_OLD` instead. |
| `UPDATED_NEW` return value (UpdateItem) | Not supported (throws 400), use `ALL_NEW` instead. |
| Disabling streams | Not supported once enabled                         |
| Continuous Backups / PITR | Stub only (always returns DISABLED)                |
| Transactions (`TransactWriteItems`, `TransactGetItems`) | Not implemented                                    |
| PartiQL (`ExecuteStatement`, `BatchExecuteStatement`) | Not implemented                                    |
| Table auto-scaling | Not applicable                                     |
| Global Tables | Not applicable                                     |
| DynamoDB Accelerator (DAX) | Not applicable                                     |
| On-demand backup/restore | Not applicable                                     |
| Export to S3 | Not applicable                                     |

### Behavioral Differences

| Aspect | AWS DynamoDB                          | Phoenix-Adapters                                                                            |
|---|---------------------------------------|---------------------------------------------------------------------------------------------|
| **Table status on create** | Transitions CREATING -> ACTIVE        | Immediately ACTIVE                                                                          |
| **Index deletion** | Index is dropped                      | Index is **disabled** (ALTER INDEX ... DISABLE), it is **dropped** eventually asynchronously |
| **Billing mode** | PAY_PER_REQUEST or PROVISIONED        | Always reports `PROVISIONED` (no actual billing)                                            |
| **Consumed capacity** | Actual capacity units                 | Always hardcoded `{ReadCapacityUnits: 1.0, WriteCapacityUnits: 1.0, CapacityUnits: 2.0}`    |
| **Query/Scan limit** | Up to 1 MB per page                   | Capped at 100 items OR 1 MB, whichever comes first                                  |
| **Stream shard iterators** | Expire after 15 minutes               | No automatic expiry                                                                         |
| **Item storage** | Native DynamoDB format                | BSON document in a single Phoenix column                                                    |
| **Consistency** | Eventual + (Strong for local indexes) | Depends on Phoenix/HBase configuration                                                      |

### Key Schema Constraints

- Primary key supports 1 (HASH only) or 2 (HASH + RANGE) key attributes
- Key attributes must be typed as `S` (String), `N` (Number), or `B` (Binary)
- Non-key attributes are schemaless (stored in BSON) and support all DynamoDB types

---

## Quick Reference:

| # | Category | Operation | X-Amz-Target Suffix |
|---|---|---|---|
| 1 | DDL | CreateTable | `DynamoDB_20120810.CreateTable` |
| 2 | DDL | DeleteTable | `DynamoDB_20120810.DeleteTable` |
| 3 | DDL | DescribeTable | `DynamoDB_20120810.DescribeTable` |
| 4 | DDL | ListTables | `DynamoDB_20120810.ListTables` |
| 5 | DDL | UpdateTable | `DynamoDB_20120810.UpdateTable` |
| 6 | DDL | UpdateTimeToLive | `DynamoDB_20120810.UpdateTimeToLive` |
| 7 | DDL | DescribeTimeToLive | `DynamoDB_20120810.DescribeTimeToLive` |
| 8 | DDL | DescribeContinuousBackups | `DynamoDB_20120810.DescribeContinuousBackups` |
| 9 | DML | PutItem | `DynamoDB_20120810.PutItem` |
| 10 | DML | UpdateItem | `DynamoDB_20120810.UpdateItem` |
| 11 | DML | DeleteItem | `DynamoDB_20120810.DeleteItem` |
| 12 | DML | BatchWriteItem | `DynamoDB_20120810.BatchWriteItem` |
| 13 | DQL | GetItem | `DynamoDB_20120810.GetItem` |
| 14 | DQL | BatchGetItem | `DynamoDB_20120810.BatchGetItem` |
| 15 | DQL | Query | `DynamoDB_20120810.Query` |
| 16 | DQL | Scan | `DynamoDB_20120810.Scan` |
| 17 | Stream | ListStreams | `DynamoDB_20120810.ListStreams` |
| 18 | Stream | DescribeStream | `DynamoDB_20120810.DescribeStream` |
| 19 | Stream | GetShardIterator | `DynamoDB_20120810.GetShardIterator` |
| 20 | Stream | GetRecords | `DynamoDB_20120810.GetRecords` |
