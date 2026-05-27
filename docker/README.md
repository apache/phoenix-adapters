# Local Docker Cluster for Phoenix Adapters

Brings up the full dependency stack (Hadoop / ZooKeeper / HBase / Phoenix)
required to run **phoenix-adapters** on your laptop. Uses upstream images
where they exist; custom only where they don't.

| Component | Version | Image |
| --- | --- | --- |
| Apache ZooKeeper | 3.8.4 | [`library/zookeeper:3.8.4`](https://hub.docker.com/_/zookeeper) (Docker Official) |
| Apache Hadoop (HDFS) | 3.3.6 | [`apache/hadoop:3.3.6`](https://hub.docker.com/r/apache/hadoop) (Apache convenience build) |
| Apache HBase | 2.5.14-hadoop3 | `phoenix-adapters/hbase-phoenix:latest` (custom) |
| Apache Phoenix | 5.3.1 (phoenix-hbase-2.5) | bundled into `phoenix-adapters/hbase-phoenix` |
| Phoenix Adapters REST | this repo | `phoenix-adapters/rest:latest` (custom) |

Versions are kept in lockstep with the top-level [`pom.xml`](../pom.xml).

> **Apple Silicon.** `apache/hadoop:3.3.6` is amd64-only; the compose file
> pins `platform: linux/amd64` so the NameNode/DataNode run under Rosetta
> emulation. Slower than native, but functional.

## Layout

```
docker/
├── Dockerfile.hbase-phoenix         # HBase 2.5.14 + Phoenix 5.3.1
├── Dockerfile.phoenix-adapters      # Multi-stage build of the REST server
├── docker-compose.yml
├── conf/
│   ├── hbase/{hbase-site.xml,hbase-env.sh}
│   └── phoenix-adapters/hbase-site.xml      # Client-side overrides
└── scripts/
    ├── hbase-entrypoint.sh                  # hbase-master, hbase-regionserver
    ├── phoenix-adapters-entrypoint.sh
    └── smoke.sh                             # End-to-end DDB validation suite
```

ZooKeeper and Hadoop config lives entirely in `docker-compose.yml` as env
vars that the upstream images template into XML.

## Quick start

**Prerequisites:** Docker Desktop running; `jq` and `curl` on `PATH`
(`brew install jq` on macOS).

From the **project root**:

```bash
# 1. Bring up the full stack (ZK + HDFS + HBase+Phoenix + REST) and BLOCK
#    until every service reports healthy (REST takes ~30-60s on a cold
#    start because Phoenix has to bootstrap SYSTEM.* tables).
#    First time: ~8-12 min (pulls upstream images + builds HBase/Phoenix + REST).
#    Subsequent runs: cached.
docker compose -f docker/docker-compose.yml up -d --build --wait

# 2. Validate it works end-to-end (CRUD + UpdateItem + BatchWriteItem + streams).
bash docker/scripts/smoke.sh
# -> "Result: 20 checks PASSED across 18 API calls"

# 3. Use it. The DynamoDB-compatible REST endpoint is at http://localhost:8842 .
#    Point any AWS SDK at it (Java/Python/Node.js snippets in
#    phoenix-ddb-rest/README.md), or hit it directly with curl:
curl -s -X POST http://localhost:8842/ \
    -H 'Content-Type: application/x-amz-json-1.0' \
    -H 'X-Amz-Target: DynamoDB_20120810.ListTables' -d '{}'

# 4. Tear down when you're done.
docker compose -f docker/docker-compose.yml down       # keep volumes
docker compose -f docker/docker-compose.yml down -v    # also wipe HDFS + ZK
```

### URLs

| URL | Service |
| --- | --- |
| http://localhost:8842 | **Phoenix Adapters REST (DynamoDB-compatible)** |
| http://localhost:9870 | HDFS NameNode UI |
| http://localhost:9864 | HDFS DataNode UI |
| http://localhost:16010 | HBase Master UI |
| http://localhost:16030 | HBase RegionServer UI |

Two host ports are remapped because their defaults often collide on dev
machines (macOS AirPlay on 9000, a locally installed Kafka/ZK on 2181):

| Service | Container | Host |
| --- | --- | --- |
| HDFS NameNode RPC | `namenode:9000` | `localhost:19000` |
| ZooKeeper client  | `zookeeper:2181` | `localhost:12181` |

Inter-container traffic still uses the standard ports.

### Bring up just the cluster (no REST)

```bash
docker compose -f docker/docker-compose.yml up -d --build \
    zookeeper namenode datanode hbase-master hbase-regionserver
```

## Validation suite

`docker/scripts/smoke.sh` exercises every supported DynamoDB API against
the running REST server and asserts the expected behaviour. It prints
each request, response, and assertion as it runs.

```bash
docker compose -f docker/docker-compose.yml up -d --build --wait
bash docker/scripts/smoke.sh
```

Exits `0` on full pass; exits non-zero on the first failed assertion and
prints the offending response.

| Step | API |
| --- | --- |
| 1  | `ListTables` (baseline) |
| 2  | `CreateTable` (with `StreamSpecification` enabled, `NEW_AND_OLD_IMAGES`) |
| 3  | `DescribeTable` |
| 4  | `PutItem` (`id=a`) |
| 5  | `UpdateItem` (`SET score, bonus`, `ReturnValues=ALL_NEW`) |
| 6  | `GetItem` |
| 7  | `PutItem` (`id=b`) |
| 8  | `Scan` |
| 9  | `Query` |
| 10 | `DeleteItem` |
| 11 | `Scan` (after delete) |
| 12 | `BatchWriteItem` (mixed put + delete) |
| 13 | `Scan` paginated (drains all pages) |
| 14 | `ListStreams` |
| 15 | `DescribeStream` (polls until `StreamStatus == ENABLED`) |
| 16 | `GetShardIterator` (`TRIM_HORIZON`) |
| 17 | `GetRecords` (drains all pages) |
| 18 | `DeleteTable` |

## Poking around the cluster

HBase shell:

```bash
docker compose -f docker/docker-compose.yml exec hbase-master hbase shell
```

```text
status
list
create 'demo', 'cf'
put 'demo', 'r1', 'cf:c1', 'hello'
scan 'demo'
```

Phoenix sqlline:

```bash
docker compose -f docker/docker-compose.yml exec hbase-master \
    /opt/phoenix/bin/sqlline.py zookeeper:2181
```

```sql
!tables
CREATE TABLE IF NOT EXISTS t1 (id BIGINT PRIMARY KEY, name VARCHAR);
UPSERT INTO t1 VALUES (1, 'phoenix-adapters');
SELECT * FROM t1;
```

## Developer inner loop: code change → live endpoint

```
phoenix-ddb-rest/src/**.java
        │  (1) edit on host
        ▼
docker compose ... up -d --build phoenix-adapters-rest
   ├── stage 1: mvn package -DskipTests   (BuildKit caches ~/.m2)
   ├── stage 1 output: phoenix-ddb-assembly/target/*-bin.tar.gz
   └── stage 2: temurin runtime extracts that tarball
        │
        ▼
http://localhost:8842/   (new code, live)
```

The cluster (ZK + HDFS + HBase) keeps running across REST rebuilds, and
HBase data persists across full `down`/`up` cycles.

### The loop

1. Edit code in `phoenix-ddb-rest/src/...` or `phoenix-ddb-utils/src/...`.
2. *(Optional)* sanity-check the compile on the host:

   ```bash
   mvn -B -DskipTests -pl phoenix-ddb-rest -am package
   ```

3. Rebuild and recreate just the REST container:

   ```bash
   docker compose -f docker/docker-compose.yml up -d --build phoenix-adapters-rest
   ```

   No-dep-change rebuilds typically take 30-60 s on a warm cache.
4. Watch logs:

   ```bash
   docker compose -f docker/docker-compose.yml logs -f phoenix-adapters-rest
   ```
5. Hit the endpoint and verify.

### Quick reference

| Task | Command |
| --- | --- |
| Rebuild REST + restart it | `docker compose -f docker/docker-compose.yml up -d --build phoenix-adapters-rest` |
| Restart REST (no code change) | `docker compose -f docker/docker-compose.yml restart phoenix-adapters-rest` |
| Tail REST logs | `docker compose -f docker/docker-compose.yml logs -f phoenix-adapters-rest` |
| Tail HBase logs | `docker compose -f docker/docker-compose.yml logs -f hbase-master hbase-regionserver` |
| HBase shell | `docker compose -f docker/docker-compose.yml exec hbase-master hbase shell` |
| Phoenix sqlline | `docker compose -f docker/docker-compose.yml exec hbase-master /opt/phoenix/bin/sqlline.py zookeeper:2181` |
| List containers | `docker compose -f docker/docker-compose.yml ps` |
| Stop (keep data) | `docker compose -f docker/docker-compose.yml down` |
| Stop + wipe data | `docker compose -f docker/docker-compose.yml down -v` |

### Edge cases

| Situation | What to do |
| --- | --- |
| Changed `conf/hbase/hbase-site.xml` or `hbase-env.sh` | `docker compose ... up -d --build hbase-master hbase-regionserver`. Existing tables survive. |
| Bumped `hbase.version` / `phoenix.version` in `pom.xml` | Bump matching `ARG`s in `Dockerfile.hbase-phoenix`, then `--build hbase-master hbase-regionserver phoenix-adapters-rest`. Often pair with `down -v`. |
| Added a Maven dep to `phoenix-ddb-rest/pom.xml` | `--build phoenix-adapters-rest`. New dep downloads once; cache warms after. |
| Clean slate | `docker compose ... down -v` then `up -d --build`. |
| Code doesn't seem picked up | You ran `restart` instead of `up --build`. `restart` does not rebuild. |

### Pre-PR checklist

```bash
# 1. Host-side compile + unit tests (no cluster required).
mvn -B clean install -DskipITs

# 2. End-to-end validation: fresh stack + full DDB round-trip including streams.
docker compose -f docker/docker-compose.yml down -v
docker compose -f docker/docker-compose.yml up -d --build --wait
bash docker/scripts/smoke.sh

# 3. Tear it down.
docker compose -f docker/docker-compose.yml down -v
```

If `smoke.sh` finishes with `Result: 20 checks PASSED across 18 API calls`,
your change is wire-compatible end to end through Phoenix on dockerized
HBase across CRUD, batch, and the change-stream chain.

## Running the REST server outside Docker

1. Bring up only the cluster services.
2. Add cluster hostnames to `/etc/hosts` (HBase advertises hostnames over ZK):

   ```
   127.0.0.1 zookeeper namenode datanode hbase-master hbase-regionserver
   ```

3. Start the REST server pointing at the dockerized ZooKeeper:

   ```bash
   mvn -DskipTests clean package
   tar xzf phoenix-ddb-assembly/target/phoenix-adapters-*-bin.tar.gz -C /tmp
   cd /tmp/phoenix-adapters-*
   export JAVA_HOME=$(/usr/libexec/java_home -v 1.8)   # macOS example
   export PHOENIX_ADAPTERS_HOME=$(pwd)
   bin/phoenix-adapters rest foreground_start -p 8842 -z localhost:12181
   ```

## Phoenix tuning baked into the image

[`docker/conf/hbase/hbase-site.xml`](conf/hbase/hbase-site.xml) enables what
Phoenix 5.x needs for secondary indexes, DDL events, and the multi-priority
RPC controller:

| Property | Value |
| --- | --- |
| `hbase.coprocessor.master.classes` | `…PhoenixMasterObserver` |
| `hbase.coprocessor.regionserver.classes` | `…PhoenixRegionServerEndpoint` |
| `hbase.regionserver.wal.codec` | `…IndexedWALEditCodec` |
| `hbase.region.server.rpc.scheduler.factory.class` | `…PhoenixRpcSchedulerFactory` |
| `hbase.rpc.controllerfactory.class` | `…ServerRpcControllerFactory` |
| `phoenix.task.handling.interval.ms` | `10` |
| `phoenix.task.handling.initial.delay.ms` | `1` |

`phoenix-server-hbase-2.5-5.3.1.jar` is copied into `${HBASE_HOME}/lib/` so
the coprocessors and WAL codec are visible to master and every RegionServer.

## Why upstream images for ZK + Hadoop but not HBase?

| Component | Decision | Reason |
| --- | --- | --- |
| ZooKeeper 3.8.4 | Upstream `zookeeper:3.8.4` | Docker Official, exact version, multi-arch. |
| Hadoop 3.3.6 | Upstream `apache/hadoop:3.3.6` | Apache convenience build at the exact version. amd64-only, runs under emulation on Apple Silicon. |
| HBase 2.5.14-hadoop3 | Custom | No official Apache image; community images don't cover `2.5.14-hadoop3`. |
| Phoenix 5.3.1 | Custom (layered on HBase) | No Phoenix image anywhere; server JAR must be on HBase's classpath. |

## Troubleshooting

* **NameNode unhealthy on first start.** First start formats the NameNode
  via `ENSURE_NAMENODE_DIR`. Watch with `docker compose ... logs -f namenode`.
* **HBase Master `RegionTooBusyException` / `NotServingRegion`.** Wait ~30 s
  after RegionServer comes up; Phoenix bootstraps `SYSTEM.*` tables on its
  first connection and the REST server retries transparently.
* **REST exits with `NoClassDefFoundError: org/apache/hadoop/fs/WithErasureCoding`.**
  The phoenix-ddb-assembly tarball ships `hadoop-common:3.3.6` (from
  `pom.xml`) alongside `hadoop-hdfs:3.4.x` / `hadoop-yarn:3.4.x`
  (transitive from `phoenix-core-client`). The 3.4.x JARs register
  FileSystem impls that need `WithErasureCoding`, which only exists in
  hadoop-common 3.4+. When HBase returns a remote exception during
  bootstrap, the client tries to enumerate FileSystem impls, hits
  `NoClassDefFoundError`, and poisons the JVM. The REST image
  `Dockerfile.phoenix-adapters` strips the 3.4.x `hadoop-hdfs*`,
  `hadoop-yarn-*`, `hadoop-mapreduce-client-*`, and `hadoop-distcp-*`
  jars after extracting the tarball — the REST server only talks to
  HBase via RPC and never opens HDFS directly, so removing them is safe.
  If this error reappears, check that those `rm -f` lines in
  `Dockerfile.phoenix-adapters` weren't dropped.
* **`Datanode denied communication with namenode`.** Cluster ID mismatch.
  `docker compose down -v` and bring the stack back up.
* **`platform mismatch` warnings on Apple Silicon.** Expected for the
  Hadoop containers (amd64 image, emulated). No action needed.

## Customising versions

HBase / Phoenix versions are `ARG`s on `Dockerfile.hbase-phoenix`:

```bash
docker compose -f docker/docker-compose.yml build \
    --build-arg HBASE_VERSION=2.5.13 \
    --build-arg PHOENIX_VERSION=5.3.0 \
    hbase-master
```

Hadoop and ZooKeeper versions are pinned by tag in `docker-compose.yml`.
Keep all four in lockstep with `pom.xml`.
