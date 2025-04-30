# 🔄 Phoenix-Shim: A Unified NoSQL Frontend for Apache Phoenix


A frontend layer that mimics NoSQL APIs for different databases while always using **Salesforce Phoenix** (on HBase) as the persistent store.


## 📖 Overview
It can be challenging for Salesforce applications/services to maintain different codebases for different substrates if they use the substrate-native NoSQL database. These databases also do not have built-in SOR or Org Migration support.

This is where Phoenix-Shim comes in. It allows developers to write new services (or port their existing services with minimal code changes) using familiar NoSQL semantics while leveraging the scalability and SOR-ness of *Salesforce Phoenix*. 

Salesforce Phoenix, the combined solution of Apache HBase and Apache Phoenix, supported with significant additional tooling for meeting **Salesforce System of Record** requirements, is a horizontally scalable relational but non-transactional datastore, operating in both 1P and Hyperforce.

## 🧩 Supported Frontends

### DynamoDB
There are 2 ways someone can use Phoenix-Shim to port their DynamoDB based service to Salesforce Phoenix:
1. **Java thick client** which translates API calls into Phoenix SQL
    - [PhoenixDBClientV2](https://git.soma.salesforce.com/bigdata-packaging/phoenix-shim/blob/master/phoenix-ddb-shim/src/main/java/org/apache/phoenix/ddb/PhoenixDBClientV2.java) and [PhoenixDBStreamsClientV2](https://git.soma.salesforce.com/bigdata-packaging/phoenix-shim/blob/master/phoenix-ddb-shim/src/main/java/org/apache/phoenix/ddb/PhoenixDBStreamsClientV2.java) implement DynamoDB's Client SDK V2.

2. A **RESTful API Server** that accepts JSON payloads similar to DynamoDB.

By using REST Service, client applications already using any AWS SDKs to connect with DynamoDB
does not need to perform any code change. The client application only needs to update the REST
endpoint.

![Alt text](src/images/phoenix_dynamodb_rest.jpeg)

#### Supported APIs
- **DDL**: CreateTable, DeleteTable DescribeTable, UpdateTimeToLive, DescribeTimeToLive
- **DQL**: Query, Scan, BatchGetItem
- **DML**: PutItem, UpdateItem, BatchWriteItem, DeleteItem
- **Change Data Capture**: ListStreams, DescribeStreams, GetShardIterator, GetRecords

### MongoDB
TBD
