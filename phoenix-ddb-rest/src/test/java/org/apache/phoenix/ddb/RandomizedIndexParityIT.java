/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.ddb;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.jdbc.PhoenixTestDriver;
import org.apache.phoenix.util.ServerUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;

/**
 * Randomized parity test that exercises the DynamoDB per-table index limit
 * (20 GSI + 5 LSI = 25 indexes) under mixed CRUD load and verifies Phoenix
 * matches LocalDynamoDB on Query and Scan against random indexes.
 *
 * Tunable via system properties:
 *   -DrandomIndexParity.scale=small|full   (default full: 15k items, 5+5 read rounds)
 *   -DrandomIndexParity.seed=<long>        (default 42; logged on test start)
 */
public class RandomizedIndexParityIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(RandomizedIndexParityIT.class);

    private static final int NUM_GSI = 20;
    private static final int NUM_LSI = 5;
    private static final int BATCH_SIZE = 25;

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    /**
     * Brings up LocalDynamoDB, an HBase mini-cluster, and the REST bridge so that both
     * clients accept identical SDK calls. {@code dynamoDbClient} talks straight to
     * LocalDDB; {@code phoenixDBClientV2} talks to LocalDDB's wire protocol served by
     * Phoenix-on-HBase via the REST shim — the parity asserts compare these two.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = TestUtils.getConfigForMiniCluster();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        DriverManager.registerDriver(new PhoenixTestDriver());

        String url = "jdbc:phoenix:localhost:" + utility.getZkCluster().getClientPort();
        TestUtils.awaitPhoenixReady(url);

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
    }

    @AfterClass
    public static void tearDown() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().stop();
        if (restServer != null) {
            restServer.stop();
        }
        ServerUtil.ConnectionFactory.shutdown();
        try {
            DriverManager.deregisterDriver(PhoenixDriver.INSTANCE);
        } finally {
            if (utility != null) {
                utility.shutdownMiniCluster();
            }
            ServerMetadataCacheTestImpl.resetCache();
        }
        System.setProperty("java.io.tmpdir", tmpDir);
    }

    // 30 min: small scale runs in ~2.5 min; full scale (15k items) plus index-convergence
    // retries (up to 6 attempts × 25 s backoff per round × 5+5 rounds) can approach this cap.
    @Test(timeout = 1800000)
    public void randomCrudParityAcross25Indexes() throws Exception {
        long seed = Long.parseLong(System.getProperty("randomIndexParity.seed", "42"));
        String scale = System.getProperty("randomIndexParity.scale", "full");
        int numItems;
        int numQueryRounds;
        int numScanRounds;
        if ("full".equalsIgnoreCase(scale)) {
            numItems = 15000;
            numQueryRounds = 5;
            numScanRounds = 5;
        } else {
            numItems = 2000;
            numQueryRounds = 3;
            numScanRounds = 3;
        }
        LOGGER.info("RandomizedIndexParityIT seed={} scale={} numItems={} queryRounds={} scanRounds={}",
                seed, scale, numItems, numQueryRounds, numScanRounds);

        Random rng = new Random(seed);
        String tableName = ("RIP_" + testName.getMethodName()).toUpperCase(Locale.ROOT);

        createTableWith25Indexes(tableName);

        // Phoenix's default UPDATE_CACHE_FREQUENCY is 60s; sleep past it so the
        // next client connection re-fetches the freshly-created indexes' metadata.
        Thread.sleep(61000);

        // Tracker of currently-live (pk, sk) keys. LinkedHashSet for deterministic
        // iteration order — combined with the seeded RNG, this makes sample() pick
        // the same key on every run for a given seed, so failures are reproducible.
        Set<KeyPair> liveKeys = new LinkedHashSet<>();

        bulkSeed(tableName, numItems, rng, liveKeys);

        int crudOps = numItems / 4;
        randomCrudPass(tableName, crudOps, rng, liveKeys);

        LOGGER.info("Workload complete; live keys = {}. Waiting for index consistency...",
                liveKeys.size());
        TestUtils.waitForEventualConsistentIndex();
        TestUtils.waitForEventualConsistentIndex();

        runQueryCampaign(tableName, numQueryRounds, rng);
        runScanCampaign(tableName, numScanRounds, rng);
    }

    // ---------- table & schema -----------------------------------------------

    /**
     * Creates the test table with the DynamoDB per-table maximum: 20 GSIs + 5 LSIs.
     * GSI sort-key types alternate S/N (and LSI alternates N/S) to cover both scalar
     * encodings on the index key path. After both clients return, asserts the
     * resulting table descriptions are identical so we know the schema landed
     * intact on both sides before any reads run.
     */
    private void createTableWith25Indexes(String tableName) throws Exception {
        CreateTableRequest req = DDLTestUtils.getCreateTableRequest(
                tableName, "pk", ScalarAttributeType.S, "sk", ScalarAttributeType.N);

        for (int i = 0; i < NUM_GSI; i++) {
            ScalarAttributeType skType =
                    (i % 2 == 0) ? ScalarAttributeType.S : ScalarAttributeType.N;
            req = DDLTestUtils.addIndexToRequest(true, req, gsiName(i),
                    gsiPk(i), ScalarAttributeType.S, gsiSk(i), skType);
        }
        for (int i = 0; i < NUM_LSI; i++) {
            ScalarAttributeType skType =
                    (i % 2 == 0) ? ScalarAttributeType.N : ScalarAttributeType.S;
            req = DDLTestUtils.addIndexToRequest(false, req, lsiName(i),
                    "pk", ScalarAttributeType.S, lsiSk(i), skType);
        }

        dynamoDbClient.createTable(req);
        phoenixDBClientV2.createTable(req);

        DescribeTableRequest dtr = DescribeTableRequest.builder().tableName(tableName).build();
        DDLTestUtils.assertTableDescriptions(
                dynamoDbClient.describeTable(dtr).table(),
                phoenixDBClientV2.describeTable(dtr).table());
        LOGGER.info("Created {} with {} GSIs and {} LSIs on both clients",
                tableName, NUM_GSI, NUM_LSI);
    }

    // ---------- workload -----------------------------------------------------

    /**
     * Initial population. Items are written to both clients via {@link
     * software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest} in batches
     * of {@link #BATCH_SIZE} (DynamoDB's per-request maximum) so seeding ~2k items
     * stays well under a minute. {@code liveKeys} is updated in lockstep to track
     * what exists on both sides for later update/delete ops.
     */
    private void bulkSeed(String tableName, int numItems, Random rng, Set<KeyPair> liveKeys) {
        List<WriteRequest> batch = new ArrayList<>(BATCH_SIZE);
        for (int i = 0; i < numItems; i++) {
            KeyPair key = newUniqueKey(rng, liveKeys);
            Map<String, AttributeValue> item = buildItem(key, rng);
            batch.add(WriteRequest.builder()
                    .putRequest(PutRequest.builder().item(item).build())
                    .build());
            liveKeys.add(key);
            if (batch.size() == BATCH_SIZE) {
                executeBatch(tableName, batch);
                batch.clear();
            }
        }
        if (!batch.isEmpty()) {
            executeBatch(tableName, batch);
        }
        LOGGER.info("Seeded {} items via BatchWriteItem", numItems);
    }

    /**
     * Drives an even mix of Put / Update / Delete chosen by the seeded RNG (Put
     * is forced when the live set is empty, but with seeded data that's rare).
     * Every op is sent to both clients in the same order, and {@code liveKeys}
     * is updated only after both sides have accepted the op — by construction it
     * mirrors what's on disk on both sides. Any divergence the parity asserts
     * later catch is a real bug, not test noise. Updates target only GSI keys;
     * LSI sort keys are not updatable in DynamoDB.
     */
    private void randomCrudPass(String tableName, int ops, Random rng, Set<KeyPair> liveKeys) {
        int puts = 0, updates = 0, deletes = 0;
        for (int i = 0; i < ops; i++) {
            int roll = rng.nextInt(3);
            if (roll == 0 || liveKeys.isEmpty()) {
                KeyPair key = newUniqueKey(rng, liveKeys);
                Map<String, AttributeValue> item = buildItem(key, rng);
                PutItemRequest put = PutItemRequest.builder()
                        .tableName(tableName).item(item).build();
                phoenixDBClientV2.putItem(put);
                dynamoDbClient.putItem(put);
                liveKeys.add(key);
                puts++;
            } else if (roll == 1) {
                KeyPair key = sample(liveKeys, rng);
                UpdateItemRequest update = buildRandomUpdate(tableName, key, rng);
                phoenixDBClientV2.updateItem(update);
                dynamoDbClient.updateItem(update);
                updates++;
            } else {
                KeyPair key = sample(liveKeys, rng);
                DeleteItemRequest del = DeleteItemRequest.builder()
                        .tableName(tableName)
                        .key(key.toKeyMap())
                        .build();
                phoenixDBClientV2.deleteItem(del);
                dynamoDbClient.deleteItem(del);
                liveKeys.remove(key);
                deletes++;
            }
        }
        LOGGER.info("Random CRUD pass: puts={} updates={} deletes={} liveAfter={}",
                puts, updates, deletes, liveKeys.size());
    }

    /**
     * Builds a randomized {@link UpdateItemRequest} that mutates 1-3 GSI key pairs
     * (forcing those GSIs to reindex the row) plus the non-indexed payload. LSI
     * sort keys are deliberately untouched — DynamoDB rejects updates that change
     * an LSI sort key, and we want both clients to accept the same request.
     */
    private UpdateItemRequest buildRandomUpdate(String tableName, KeyPair key, Random rng) {
        Map<String, String> names = new HashMap<>();
        Map<String, AttributeValue> values = new HashMap<>();
        StringBuilder set = new StringBuilder("SET ");
        int mutations = 1 + rng.nextInt(3);
        Set<Integer> chosen = new HashSet<>();
        for (int m = 0; m < mutations; m++) {
            int idx;
            do {
                idx = rng.nextInt(NUM_GSI);
            } while (!chosen.add(idx));
            String pkAttr = gsiPk(idx);
            String skAttr = gsiSk(idx);
            String pkPh = "#gpk" + idx, pkVp = ":gpk" + idx;
            String skPh = "#gsk" + idx, skVp = ":gsk" + idx;
            names.put(pkPh, pkAttr);
            names.put(skPh, skAttr);
            values.put(pkVp, AttributeValue.builder().s(randStr(rng, "g" + idx, 6)).build());
            values.put(skVp,
                    (idx % 2 == 0)
                            ? AttributeValue.builder().s(randStr(rng, "s", 4)).build()
                            : AttributeValue.builder().n(Integer.toString(rng.nextInt(10000))).build());
            if (m > 0) set.append(", ");
            set.append(pkPh).append(" = ").append(pkVp)
                    .append(", ").append(skPh).append(" = ").append(skVp);
        }
        // payload always updated
        names.put("#p", "payload");
        values.put(":p", AttributeValue.builder().s(randStr(rng, "p", 12)).build());
        set.append(", #p = :p");

        return UpdateItemRequest.builder()
                .tableName(tableName)
                .key(key.toKeyMap())
                .updateExpression(set.toString())
                .expressionAttributeNames(names)
                .expressionAttributeValues(values)
                .build();
    }

    // ---------- read campaigns ----------------------------------------------

    /**
     * For each round, picks a random index (GSI or LSI weighted by count), probes
     * the base table for an item that has the index hash-key populated so the
     * Query has a real target, and then asserts Phoenix and LocalDDB return the
     * same items. Randomly toggles {@code scanIndexForward} and a payload
     * {@code FilterExpression} to widen the surface area covered per run.
     */
    private void runQueryCampaign(String tableName, int rounds, Random rng) {
        for (int r = 0; r < rounds; r++) {
            boolean useGsi = rng.nextDouble() < ((double) NUM_GSI / (NUM_GSI + NUM_LSI));
            int idx = useGsi ? rng.nextInt(NUM_GSI) : rng.nextInt(NUM_LSI);
            String indexName = useGsi ? gsiName(idx) : lsiName(idx);

            // Find any item with the index hash-key set so the query has a target.
            // Probed once per round and reused across assertParityEventually retries —
            // safe today because randomCrudPass has already finished. If anyone adds
            // writes inside the read campaign, refresh the probe inside the lambda.
            Map<String, AttributeValue> probe = findItemWithIndex(tableName, useGsi, idx);
            if (probe == null) {
                LOGGER.info("Query round {}: no items present for {}; skipping", r, indexName);
                continue;
            }

            String hashAttr = useGsi ? gsiPk(idx) : "pk";
            Map<String, String> names = new HashMap<>();
            names.put("#h", hashAttr);
            Map<String, AttributeValue> values = new HashMap<>();
            values.put(":h", probe.get(hashAttr));

            QueryRequest.Builder qr = QueryRequest.builder()
                    .tableName(tableName)
                    .indexName(indexName)
                    .keyConditionExpression("#h = :h");
            if (rng.nextBoolean()) {
                qr.scanIndexForward(false);
            }
            if (rng.nextBoolean() && probe.containsKey("payload")) {
                // Filter narrows results to the probed item's payload — both sides
                // should agree on the (small) result set; this is filter-codepath coverage.
                names.put("#p", "payload");
                values.put(":p", probe.get("payload"));
                qr.filterExpression("#p = :p");
            }
            qr.expressionAttributeNames(names).expressionAttributeValues(values);
            LOGGER.info("Query round {} index={} hash={}", r, indexName, probe.get(hashAttr));

            // Wrap the parity comparison in a lambda so assertParityEventually can
            // re-invoke it on each retry. Each call rebuilds the same QueryRequest
            // and runs it against both clients, throwing AssertionError on mismatch
            // — that's the signal assertParityEventually waits on.
            String label = "Query round " + r + " index=" + indexName;
            assertParityEventually(label,
                    () -> {
                        // compareQueryOutputs paginates by mutating exclusiveStartKey on the
                        // builder; on retry we must reset it so we don't resume from the prior
                        // run's last page.
                        qr.exclusiveStartKey(null);
                        TestUtils.compareQueryOutputs(qr, phoenixDBClientV2, dynamoDbClient);
                    });
        }
    }

    /**
     * For each round, picks a random index and runs a paginated Scan with a small
     * {@code Limit} to force multiple round-trips, asserting Phoenix and LocalDDB
     * return the same set of items. Underlying compare uses sorted equality so
     * non-deterministic Scan ordering across the two implementations doesn't
     * cause spurious failures.
     */
    private void runScanCampaign(String tableName, int rounds, Random rng) {
        for (int r = 0; r < rounds; r++) {
            boolean useGsi = rng.nextDouble() < ((double) NUM_GSI / (NUM_GSI + NUM_LSI));
            int idx = useGsi ? rng.nextInt(NUM_GSI) : rng.nextInt(NUM_LSI);
            String indexName = useGsi ? gsiName(idx) : lsiName(idx);

            ScanRequest.Builder sr = ScanRequest.builder()
                    .tableName(tableName)
                    .indexName(indexName)
                    .limit(50 + rng.nextInt(450));

            String hashAttr = useGsi ? gsiPk(idx) : "pk";
            String sortAttr = useGsi ? gsiSk(idx) : lsiSk(idx);
            ScalarAttributeType hashType = ScalarAttributeType.S;
            ScalarAttributeType sortType;
            if (useGsi) {
                sortType = (idx % 2 == 0) ? ScalarAttributeType.S : ScalarAttributeType.N;
            } else {
                sortType = (idx % 2 == 0) ? ScalarAttributeType.N : ScalarAttributeType.S;
            }
            LOGGER.info("Scan round {} index={} limit={}", r, indexName, sr.build().limit());

            // Wrap the parity comparison in a lambda so assertParityEventually can
            // re-invoke it on each retry. Each call rebuilds the same ScanRequest
            // and runs it against both clients, throwing AssertionError on mismatch
            // — that's the signal assertParityEventually waits on.
            // finalSortType: lambdas can only capture effectively-final locals, but
            // sortType is assigned in an if/else above, so we copy it into a final.
            ScalarAttributeType finalSortType = sortType;
            String label = "Scan round " + r + " index=" + indexName;
            assertParityEventually(label,
                    () -> {
                        // compareScanOutputs paginates by mutating exclusiveStartKey on the
                        // builder; on retry we must reset it so we don't resume from the prior
                        // run's last page.
                        sr.exclusiveStartKey(null);
                        TestUtils.compareScanOutputs(sr, phoenixDBClientV2, dynamoDbClient,
                                hashAttr, sortAttr, hashType, finalSortType);
                    });
        }
    }

    /**
     * Runs a parity comparison against eventually-consistent indexes, retrying
     * with linear backoff if the two sides briefly disagree.
     *
     * Sleeps between attempts: 5s, 10s, 15s, 20s, 25s — total ~75s of grace
     * before declaring failure across {@code maxAttempts} = 6.
     *
     * Catches both {@link AssertionError} (parity mismatch) and {@link
     * RuntimeException} (transient SDK / HBase failures during pagination) so
     * a flake on either side gets the same backoff window instead of failing
     * the round on the first throw.
     *
     * @param label       human-readable identifier for logs and the final error
     * @param parityCheck a JUnit assertion that throws on mismatch
     */
    private void assertParityEventually(String label, Runnable parityCheck) {
        final int maxAttempts = 6;
        final long backoffStepMs = 5000;
        Throwable lastFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                parityCheck.run();
                if (attempt > 1) {
                    LOGGER.info("{} converged on attempt {}", label, attempt);
                }
                return;
            } catch (AssertionError | RuntimeException e) {
                lastFailure = e;
                LOGGER.warn("{} failed on attempt {}/{}: {}: {}",
                        label, attempt, maxAttempts, e.getClass().getSimpleName(), e.getMessage());
                if (attempt == maxAttempts) break;
                try {
                    Thread.sleep(backoffStepMs * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError("Interrupted during retry of " + label, ie);
                }
            }
        }
        throw new AssertionError("Parity failure on " + label, lastFailure);
    }

    // ---------- helpers ------------------------------------------------------

    /**
     * Returns one base-table item that has the chosen index's hash-key attribute
     * present, so the Query round can build a {@code KeyConditionExpression} that
     * actually targets a row. Probes via LocalDDB (not Phoenix) on purpose: any
     * difference in what Phoenix sees for that key is what the parity check is
     * meant to surface. Scans up to 500 items so sparse GSIs (~10% omission rate)
     * still find a hit; returns null only if every probed item lacks the attr.
     */
    private Map<String, AttributeValue> findItemWithIndex(String tableName, boolean useGsi,
            int idx) {
        ScanRequest scan = ScanRequest.builder().tableName(tableName).limit(500).build();
        List<Map<String, AttributeValue>> items = dynamoDbClient.scan(scan).items();
        String hashAttr = useGsi ? gsiPk(idx) : "pk";
        for (Map<String, AttributeValue> item : items) {
            if (item.containsKey(hashAttr)) {
                return item;
            }
        }
        return null;
    }

    private void executeBatch(String tableName, List<WriteRequest> batch) {
        Map<String, List<WriteRequest>> requestItems = new HashMap<>();
        requestItems.put(tableName, new ArrayList<>(batch));
        BatchWriteItemRequest req = BatchWriteItemRequest.builder().requestItems(requestItems).build();
        // Fail fast on partial acceptance: if either side leaves UnprocessedItems,
        // liveKeys would silently drift ahead of actual storage and later parity
        // failures would be hard to attribute back to seed-time write loss.
        BatchWriteItemResponse phoenixResp = phoenixDBClientV2.batchWriteItem(req);
        Assert.assertTrue("Phoenix returned UnprocessedItems: " + phoenixResp.unprocessedItems(),
                phoenixResp.unprocessedItems().isEmpty());
        BatchWriteItemResponse ddbResp = dynamoDbClient.batchWriteItem(req);
        Assert.assertTrue("LocalDDB returned UnprocessedItems: " + ddbResp.unprocessedItems(),
                ddbResp.unprocessedItems().isEmpty());
    }

    /**
     * Constructs a randomized item populated for all 25 indexes. ~10% of GSI key
     * attributes are randomly omitted to exercise sparse-GSI behavior. LSI sort
     * keys are always populated because DynamoDB rejects items that omit them.
     */
    private Map<String, AttributeValue> buildItem(KeyPair key, Random rng) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk", AttributeValue.builder().s(key.pk).build());
        item.put("sk", AttributeValue.builder().n(Long.toString(key.sk)).build());
        item.put("payload", AttributeValue.builder().s(randStr(rng, "p", 12)).build());

        for (int i = 0; i < NUM_GSI; i++) {
            if (rng.nextInt(10) == 0) continue;
            item.put(gsiPk(i), AttributeValue.builder().s(randStr(rng, "g" + i, 6)).build());
            if (i % 2 == 0) {
                item.put(gsiSk(i), AttributeValue.builder().s(randStr(rng, "s", 4)).build());
            } else {
                item.put(gsiSk(i),
                        AttributeValue.builder().n(Integer.toString(rng.nextInt(10000))).build());
            }
        }
        for (int i = 0; i < NUM_LSI; i++) {
            if (i % 2 == 0) {
                item.put(lsiSk(i),
                        AttributeValue.builder().n(Integer.toString(rng.nextInt(10000))).build());
            } else {
                item.put(lsiSk(i), AttributeValue.builder().s(randStr(rng, "l", 4)).build());
            }
        }
        return item;
    }

    private KeyPair newUniqueKey(Random rng, Set<KeyPair> existing) {
        while (true) {
            KeyPair k = new KeyPair("pk_" + randStr(rng, "", 8), rng.nextInt(Integer.MAX_VALUE));
            if (!existing.contains(k)) return k;
        }
    }

    private static <T> T sample(Set<T> set, Random rng) {
        int target = rng.nextInt(set.size());
        int i = 0;
        for (T t : set) {
            if (i++ == target) return t;
        }
        throw new IllegalStateException();
    }

    private static String randStr(Random rng, String prefix, int len) {
        StringBuilder sb = new StringBuilder(prefix);
        for (int i = 0; i < len; i++) {
            sb.append((char) ('a' + rng.nextInt(26)));
        }
        return sb.toString();
    }

    private static String gsiName(int i) { return String.format(Locale.ROOT, "gsi_%02d", i); }
    private static String lsiName(int i) { return String.format(Locale.ROOT, "lsi_%02d", i); }
    private static String gsiPk(int i) { return String.format(Locale.ROOT, "gsi_pk_%02d", i); }
    private static String gsiSk(int i) { return String.format(Locale.ROOT, "gsi_sk_%02d", i); }
    private static String lsiSk(int i) { return String.format(Locale.ROOT, "lsi_sk_%02d", i); }

    private static final class KeyPair {
        final String pk;
        final long sk;

        KeyPair(String pk, long sk) {
            this.pk = pk;
            this.sk = sk;
        }

        Map<String, AttributeValue> toKeyMap() {
            Map<String, AttributeValue> m = new HashMap<>();
            m.put("pk", AttributeValue.builder().s(pk).build());
            m.put("sk", AttributeValue.builder().n(Long.toString(sk)).build());
            return m;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof KeyPair)) return false;
            KeyPair k = (KeyPair) o;
            return sk == k.sk && pk.equals(k.pk);
        }

        @Override
        public int hashCode() {
            return 31 * pk.hashCode() + Long.hashCode(sk);
        }
    }
}
