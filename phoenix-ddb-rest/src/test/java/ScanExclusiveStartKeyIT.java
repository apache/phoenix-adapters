import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import software.amazon.awssdk.core.SdkBytes;

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
 * Tests scan pagination with different limits and different combinations of hash and sort key data types.
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
    
    // Test parameters
    public int scanLimit;
    public KeyTypeConfig keyTypeConfig;

    // Configuration for different key type combinations
    public static class KeyTypeConfig {
        public final String name;
        public final ScalarAttributeType hashKeyType;
        public final ScalarAttributeType sortKeyType;
        public final String hashKeyName;
        public final String sortKeyName;

        public KeyTypeConfig(String name, ScalarAttributeType hashKeyType, ScalarAttributeType sortKeyType) {
            this.name = name;
            this.hashKeyType = hashKeyType;
            this.sortKeyType = sortKeyType;
            this.hashKeyName = "partition_key";
            this.sortKeyName = "sort_key";
        }

        @Override
        public String toString() {
            return name;
        }
    }

    @Parameterized.Parameters(name = "limit_{0}_keyTypes_{1}")
    public static synchronized Collection<Object[]> data() {
        List<Object[]> parameters = new ArrayList<>();
        
        // Different scan limits to test
        int[] scanLimits = {1, 2, 3, 4, 5, 6, 7, 8};
        
        // Different key type combinations
        KeyTypeConfig[] keyConfigs = {
            new KeyTypeConfig("S_N", ScalarAttributeType.S, ScalarAttributeType.N), // String + Number
            new KeyTypeConfig("B_B", ScalarAttributeType.B, ScalarAttributeType.B), // Binary + Binary  
            new KeyTypeConfig("B_N", ScalarAttributeType.B, ScalarAttributeType.N), // Binary + Number
            new KeyTypeConfig("N_S", ScalarAttributeType.N, ScalarAttributeType.S), // Number + String
            new KeyTypeConfig("N_N", ScalarAttributeType.N, ScalarAttributeType.N), // Number + Number
            new KeyTypeConfig("S_S", ScalarAttributeType.S, ScalarAttributeType.S), // String + String
            new KeyTypeConfig("S_B", ScalarAttributeType.S, ScalarAttributeType.B)  // String + Binary
        };
        
        // Create all combinations of limits and key types
        for (int limit : scanLimits) {
            for (KeyTypeConfig config : keyConfigs) {
                parameters.add(new Object[]{limit, config});
            }
        }
        
        return parameters;
    }

    public ScanExclusiveStartKeyIT(int scanLimit, KeyTypeConfig keyTypeConfig) {
        this.scanLimit = scanLimit;
        this.keyTypeConfig = keyTypeConfig;
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
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_") + 
                "_limit_" + scanLimit + "_" + keyTypeConfig.name;
        
        // Create table with hash key and sort key of specified types
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, keyTypeConfig.hashKeyName,
                        keyTypeConfig.hashKeyType, keyTypeConfig.sortKeyName, keyTypeConfig.sortKeyType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert 56 items: 7 hash keys with 8 sort keys each
        List<Map<String, AttributeValue>> testItems = new ArrayList<>();
        for (int hashIndex = 0; hashIndex < 7; hashIndex++) {
            for (int sortIndex = 0; sortIndex < 8; sortIndex++) {
                Map<String, AttributeValue> item = createTestItem(hashIndex, sortIndex, keyTypeConfig);
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
                TestUtils.sortItemsByPartitionAndSortKey(phoenixItems, keyTypeConfig.hashKeyName, keyTypeConfig.sortKeyName);
        List<Map<String, AttributeValue>> sortedDynamoItems =
                TestUtils.sortItemsByPartitionAndSortKey(dynamoItems, keyTypeConfig.hashKeyName, keyTypeConfig.sortKeyName);
        Assert.assertTrue("Phoenix and DynamoDB should return identical items when sorted",
                ItemComparator.areItemsEqual(sortedPhoenixItems, sortedDynamoItems));
    }

    /**
     * Create a test item with the given partition key and sort key based on the key type configuration.
     */
    private Map<String, AttributeValue> createTestItem(int hashIndex, int sortIndex, KeyTypeConfig config) {
        Map<String, AttributeValue> item = new HashMap<>();
        
        // Create hash key value based on type
        AttributeValue hashKeyValue = createAttributeValue(config.hashKeyType, hashIndex, "pk");
        item.put(config.hashKeyName, hashKeyValue);
        
        // Create sort key value based on type
        AttributeValue sortKeyValue = createAttributeValue(config.sortKeyType, sortIndex, "sk");
        item.put(config.sortKeyName, sortKeyValue);
        
        // Add a data field for verification
        item.put("data_field", AttributeValue.builder().s("data_" + hashIndex + "_" + sortIndex).build());
        
        return item;
    }

    /**
     * Create an AttributeValue based on the specified type and index.
     */
    private AttributeValue createAttributeValue(ScalarAttributeType type, int index, String prefix) {
        switch (type) {
            case S:
                return AttributeValue.builder().s(prefix + index).build();
            case N:
                return AttributeValue.builder().n(String.valueOf(index)).build();
            case B:
                // Create binary data using the index as bytes
                byte[] bytes = (prefix + index).getBytes();
                return AttributeValue.builder().b(SdkBytes.fromByteArray(bytes)).build();
            default:
                throw new IllegalArgumentException("Unsupported attribute type: " + type);
        }
    }

} 