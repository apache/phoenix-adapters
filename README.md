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

### How to bring up REST Server in dev env?

1. Bring up HBase cluster locally. Refer to https://hbase.apache.org/book.html#quickstart
2. Bring up Phoenix system tables using sqlline (bin/sqlline.py). Refer to https://phoenix.apache.org/installation.html.
2. Clone phoenix-shim repo.
2. Build the project with: `mvn clean install -DskipTests`
3. Start the REST Server with: `bin/phoenix-shim rest start -p <port> -z <zk-quorum>`
   e.g. `bin/phoenix-shim rest start -p 8842 -z localhost:2181` to start the server at
   port 8842 with zk-quorum localhost:2181.
   Alternative to `-z <zk-quorum>` is env variable `ZOO_KEEPER_QUORUM`.
4. Optional step: To confirm the server is started and functional,
   run class `TestWithLocalRestService` by adjusting the endpoint.

