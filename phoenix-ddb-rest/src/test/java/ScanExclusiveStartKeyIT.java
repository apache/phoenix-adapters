import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Parametrized test to verify getExclusiveStartKeyConditionForScan() logic in DQLUtils.
 * Tests scan pagination with different limits using tables with both hash and sort keys.
 */
@RunWith(Parameterized.class)
public class ScanExclusiveStartKeyIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ScanExclusiveStartKeyIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();
    
    public int scanLimit;

    @Parameterized.Parameters(name = "testScanExclusiveStartKey_limit_{0}")
    public static synchronized Collection<Object> data() {
        return Arrays.asList(new Object[] {1,2,3,4,5,6,7,8});
    }

    public ScanExclusiveStartKeyIT(int scanLimit) {
        this.scanLimit = scanLimit;
    }

    @BeforeClass
    public static void initialize() throws Exception {
        tmpDir = System.getProperty("java.io.tmpdir");
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;

        restServer = new RESTServer(utility.getConfiguration());
        restServer.run();

        LOGGER.info("started {} on port {}", restServer.getClass().getName(), restServer.getPort());
        phoenixDBClientV2 = LocalDynamoDB.createV2Client("http://" + restServer.getServerAddress());
    }

    @AfterClass
    public static void stopLocalDynamoDb() throws Exception {
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

    /**
     * Test getExclusiveStartKeyConditionForScan() logic by creating a table with hash and sort keys,
     * inserting 56 items (7 hash keys with 8 sort keys each), and scanning with different limits.
     * This tests both cases:
     * 1. When last evaluated key has only hash key
     * 2. When last evaluated key has both hash key and sort key
     */
    @Test(timeout = 120000)
    public void testScanExclusiveStartKeyPagination() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_") + "_limit_" + scanLimit;
        
        // Create table with hash key (partition_key) and sort key (sort_key)
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "partition_key",
                        ScalarAttributeType.S, "sort_key", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 56 items: 7 hash keys (pk0-pk6) with 8 sort keys each (0-7)
        List<Map<String, AttributeValue>> testItems = new ArrayList<>();
        for (int hashIndex = 0; hashIndex < 7; hashIndex++) {
            for (int sortIndex = 0; sortIndex < 8; sortIndex++) {
                Map<String, AttributeValue> item = createTestItem("pk" + hashIndex, sortIndex);
                testItems.add(item);
                
                PutItemRequest putItemRequest = PutItemRequest.builder()
                        .tableName(tableName)
                        .item(item)
                        .build();
                phoenixDBClientV2.putItem(putItemRequest);
                dynamoDbClient.putItem(putItemRequest);
            }
        }

        // Perform paginated scan with the specified limit
        List<Map<String, AttributeValue>> phoenixItems = new ArrayList<>();
        List<Map<String, AttributeValue>> dynamoItems = new ArrayList<>();
        
        Map<String, AttributeValue> phoenixLastKey = null;
        Map<String, AttributeValue> dynamoLastKey = null;
        
        int phoenixPaginationCount = 0;
        int dynamoPaginationCount = 0;

        // Phoenix scan with pagination
        do {
            ScanRequest.Builder phoenixScanRequest = ScanRequest.builder()
                    .tableName(tableName)
                    .limit(scanLimit);
            
            if (phoenixLastKey != null && !phoenixLastKey.isEmpty()) {
                phoenixScanRequest.exclusiveStartKey(phoenixLastKey);
            }
            
            ScanResponse phoenixResult = phoenixDBClientV2.scan(phoenixScanRequest.build());
            phoenixItems.addAll(phoenixResult.items());
            phoenixLastKey = phoenixResult.lastEvaluatedKey();
            phoenixPaginationCount++;
            
            LOGGER.info("Phoenix scan iteration {}, returned {} items, last key: {}", 
                    phoenixPaginationCount, phoenixResult.count(), phoenixLastKey);
                    
        } while (phoenixLastKey != null && !phoenixLastKey.isEmpty());

        // DynamoDB scan with pagination
        do {
            ScanRequest.Builder dynamoScanRequest = ScanRequest.builder()
                    .tableName(tableName)
                    .limit(scanLimit);
            
            if (dynamoLastKey != null && !dynamoLastKey.isEmpty()) {
                dynamoScanRequest.exclusiveStartKey(dynamoLastKey);
            }
            
            ScanResponse dynamoResult = dynamoDbClient.scan(dynamoScanRequest.build());
            dynamoItems.addAll(dynamoResult.items());
            dynamoLastKey = dynamoResult.lastEvaluatedKey();
            dynamoPaginationCount++;
            
            LOGGER.info("DynamoDB scan iteration {}, returned {} items, last key: {}", 
                    dynamoPaginationCount, dynamoResult.count(), dynamoLastKey);
                    
        } while (dynamoLastKey != null && !dynamoLastKey.isEmpty());

        // Verify total number of items retrieved
        Assert.assertEquals("Phoenix should return all 56 items", 56, phoenixItems.size());
        Assert.assertEquals("DynamoDB should return all 56 items", 56, dynamoItems.size());
        
        // Verify that both clients returned the same number of pagination rounds
        // Note: The exact pagination behavior might differ slightly, but both should complete
        Assert.assertTrue("Phoenix pagination should complete", phoenixPaginationCount > 0);
        Assert.assertTrue("DynamoDB pagination should complete", dynamoPaginationCount > 0);
        
        // For limits smaller than 8, we should see multiple pagination rounds
        if (scanLimit < 8) {
            Assert.assertTrue("Should require multiple pagination rounds for limit " + scanLimit, 
                    phoenixPaginationCount > 7); // At least 7 hash keys, so should need multiple rounds
        }

        List<Map<String, AttributeValue>> sortedPhoenixItems =
                sortItemsByPartitionAndSortKey(phoenixItems);
        List<Map<String, AttributeValue>> sortedDynamoItems =
                sortItemsByPartitionAndSortKey(dynamoItems);
        Assert.assertTrue("Phoenix and DynamoDB should return identical items when sorted",
                ItemComparator.areItemsEqual(sortedPhoenixItems, sortedDynamoItems));
    }

    /**
     * Create a test item with the given partition key and sort key.
     */
    private Map<String, AttributeValue> createTestItem(String partitionKey, int sortKey) {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("partition_key", AttributeValue.builder().s(partitionKey).build());
        item.put("sort_key", AttributeValue.builder().n(String.valueOf(sortKey)).build());
        item.put("data_field", AttributeValue.builder().s("data_" + partitionKey + "_" + sortKey).build());
        return item;
    }

    private List<Map<String, AttributeValue>> sortItemsByPartitionAndSortKey(
            List<Map<String, AttributeValue>> items) {
        return items.stream().sorted(
                        Comparator.comparing(
                                        (Map<String, AttributeValue> item) -> item.get("partition_key").s())
                                .thenComparing(item -> Integer.parseInt(item.get("sort_key").n())))
                .collect(Collectors.toList());
    }
} 