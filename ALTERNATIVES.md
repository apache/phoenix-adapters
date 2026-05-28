# DynamoDB-Compatible Alternatives

Several open-source projects provide DynamoDB-compatible APIs on non-AWS infrastructure. This document compares **Phoenix-Adapters** with the two most prominent alternatives — **ScyllaDB Alternator** and **ExtendDB** — across architecture, API surface, scalability, and design philosophy, to help you pick the right tool for your workload.

> _Analysis as of **2026-05-27**._

## At a Glance

| | **Phoenix-Adapters** | **ScyllaDB Alternator** | **ExtendDB** |
|---|---|---|---|
| **Storage Engine** | Apache Phoenix / HBase | ScyllaDB (embedded) | PostgreSQL |
| **Language** | Java | C++ | Rust |
| **Deployment** | Stateless REST servers + HBase cluster | Built into every ScyllaDB node | Single binary + PostgreSQL |

## API Coverage

| Operation | Phoenix-Adapters | Alternator | ExtendDB |
|---|:---:|:---:|:---:|
| CreateTable / DeleteTable / DescribeTable / ListTables / UpdateTable | ✅ | ✅ | ✅ |
| PutItem / GetItem / DeleteItem / UpdateItem | ✅ | ✅ | ✅ |
| Query / Scan | ✅ | ✅ | ✅ |
| BatchGetItem / BatchWriteItem | ✅ | ✅ | ✅ |
| TransactWriteItems / TransactGetItems | ❌ | ❌ | ✅ |
| DynamoDB Streams | ✅ | ✅ | ✅ |
| GSI / LSI | ✅ | ✅ | ✅ |
| TTL | ✅ | ✅ | ✅ |
| Backup / Restore | ❌ | ❌ | ✅ |
| Import / Export | ❌ | ❌ | ✅ |
| Tagging | ❌ | ✅ | ✅ |

## Expressions & Query Features

| Feature | Phoenix-Adapters | Alternator | ExtendDB |
|---|:---:|:---:|:---:|
| ConditionExpression | ✅ | ✅ | ✅ |
| FilterExpression | ✅ | ✅ | ✅ |
| ProjectionExpression | ✅ | ✅ | ✅ |
| UpdateExpression (SET/REMOVE/ADD/DELETE) | ✅ | ✅ | ✅ |
| KeyConditionExpression | ✅ | ✅ | ✅ |
| Legacy APIs (Expected, AttributesToGet, ScanFilter) | ✅ | ✅ | ❌ |

## Scalability & Operations

|                            | Phoenix-Adapters            | Alternator                            | ExtendDB |
|----------------------------|-----------------------------|---------------------------------------|---|
| **Horizontal scale**       | Petabytes (HBase regions)   | Petabytes (ScyllaDB vnodes)           | Single node server (PG vertical) |
| **Multi-instance safe**    | ✅ Stateless tier            | ✅ Native (peer-to-peer)               | ❌ No coordination (documented gap) |
| **Write distribution**     | Across HBase region servers | Across ScyllaDB nodes                 | Single PG writer |
| **Data Consistency**       | Configurable (HBase)        | Tunable (LOCAL_ONE / LOCAL_QUORUM)    | Strong (PG default) |
| **Index Consistency**      | Eventual GSI / Strong LSI   | Eventual GSI / Strong LSI | Strong (PG default) |
| **Operational complexity** | Medium (HBase + Phoenix)    | Medium (ScyllaDB cluster)             | Low (binary + PG) |

## Design Philosophy

| | Phoenix-Adapters                 | Alternator | ExtendDB |
|---|----------------------------------|---|---|
| **Approach** | Many APIs → one backend          | Embedded in storage engine | One API → pluggable backends |
| **Expression evaluation** | Server-side Phoenix functions       | Native C++ in storage layer | In-process Rust evaluator |
| **Index implementation** | Phoenix Global Secondary Indexes | ScyllaDB materialized views | Separate PG tables |
| **Stream implementation** | Phoenix CDC                      | ScyllaDB CDC | Atomic with data writes (PG txn) |

## When to Use What

| Scenario | Best Fit |
|---|---|
| Need DynamoDB compat at petabyte scale on HBase infrastructure | **Phoenix-Adapters** |
| Already running ScyllaDB or want DDB API as a Cassandra alternative | **Alternator** |
| Need high API fidelity, transactions, or a lightweight local setup | **ExtendDB** |
| Must support legacy DynamoDB APIs (Expected, QueryFilter) | **Phoenix-Adapters** or **Alternator** |
| Need multi-item atomic transactions (TransactWriteItems) | **ExtendDB** (only option) |

## Links

- [Phoenix-Adapters](https://github.com/apache/phoenix-adapters) — DynamoDB on Phoenix/HBase
- [ScyllaDB Alternator](https://www.scylladb.com/technologies/dynamodb-api/) — DynamoDB embedded in ScyllaDB
- [ExtendDB](https://github.com/ExtendDB/extenddb) — DynamoDB on PostgreSQL (pluggable)
