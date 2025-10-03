import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.ScanResponse;

/**
 * Comprehensive parametrized test for segment scan COUNT functionality.
 * 
 * This test verifies that segment scans with SELECT=COUNT work correctly across 
 * different key configurations and segment counts. For each test run:
 * 1. Creates a table with specified key schema
 * 2. Inserts items while periodically splitting the underlying HBase table
 * 3. Performs both full scan count and segment scan count with pagination
 * 4. Verifies that both approaches return identical counts
 * 5. Verifies sum of segment counts equals total expected count
 */
@RunWith(Parameterized.class)
public class CountSegmentScanIT extends BaseSegmentScanIT {

    private final String hashKeyName;
    private final ScalarAttributeType hashKeyType;
    private final String sortKeyName;
    private final ScalarAttributeType sortKeyType;
    private final int totalSegments;
    protected static final int TOTAL_ITEMS = 3000;
    protected static final int SPLIT_FREQUENCY = 600;
    protected static final int SCAN_LIMIT = 37;

    public CountSegmentScanIT(String hashKeyName, ScalarAttributeType hashKeyType, 
                              String sortKeyName, ScalarAttributeType sortKeyType, 
                              int totalSegments) {
        this.hashKeyName = hashKeyName;
        this.hashKeyType = hashKeyType;
        this.sortKeyName = sortKeyName;
        this.sortKeyType = sortKeyType;
        this.totalSegments = totalSegments;
    }

    @Parameterized.Parameters(name = "Hash_{1}_Sort_{3}_Segments_{4}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            // Hash key only - different segment counts
            {"pk", ScalarAttributeType.S, null, null, 3},
            {"pk", ScalarAttributeType.N, null, null, 4},
            {"pk", ScalarAttributeType.B, null, null, 6},
            
            // Hash + Sort key combinations - different segment counts
            {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.S, 2},
            {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.N, 5},
            {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.B, 3},
            
            {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.S, 4},
            {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.N, 2},
            {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.B, 5},

            {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.S, 2},
            {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.N, 3},
            {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.B, 4},

            // Large segment counts to test edge cases
            {"pk", ScalarAttributeType.S, null, null, 10},
            {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.S, 12},
        });
    }

    @Test(timeout = 300000)
    public void testCountSegmentScan() throws Exception {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_");

        // Create table
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(
                tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert items with periodic table splitting
        insertItemsWithSplitting(tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType, 
                                TOTAL_ITEMS, SPLIT_FREQUENCY, 100);

        // Perform full scan count with pagination
        int fullScanCount = performFullScanCount(tableName);

        // Perform segment scan count across all segments with pagination
        int segmentScanCount = performSegmentScanCount(tableName);

        // Verify results
        Assert.assertEquals("Full scan count should equal total items", TOTAL_ITEMS, fullScanCount);
        Assert.assertEquals("Segment scan count should equal full scan count", 
                fullScanCount, segmentScanCount);
    }

    /**
     * Perform full scan count with pagination using SELECT=COUNT.
     */
    private int performFullScanCount(String tableName) {
        int totalCount = 0;
        Map<String, AttributeValue> lastEvaluatedKey = null;
        
        do {
            ScanRequest.Builder scanBuilder = ScanRequest.builder()
                    .tableName(tableName)
                    .select("COUNT")
                    .limit(SCAN_LIMIT);
            if (lastEvaluatedKey != null) {
                scanBuilder.exclusiveStartKey(lastEvaluatedKey);
            }
            
            ScanResponse response = phoenixDBClientV2.scan(scanBuilder.build());
            totalCount += response.count();
            lastEvaluatedKey = response.lastEvaluatedKey();
            
            // Verify no items are returned when using COUNT select
            Assert.assertTrue("COUNT scan should not return items", 
                    response.items() == null || response.items().isEmpty());
            
        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
        return totalCount;
    }

    /**
     * Perform segment scan count across all segments with pagination.
     */
    private int performSegmentScanCount(String tableName) {
        int totalCount = 0;
        // Scan each segment sequentially and sum counts
        for (int segment = 0; segment < totalSegments; segment++) {
            int segmentCount = scanSingleSegmentCount(tableName, segment, totalSegments);
            totalCount += segmentCount;
        }
        return totalCount;
    }

    /**
     * Scan a single segment and return count with pagination.
     */
    private int scanSingleSegmentCount(String tableName, int segment, int totalSegments) {
        int segmentCount = 0;
        Map<String, AttributeValue> lastEvaluatedKey = null;
        
        do {
            ScanRequest.Builder scanBuilder = ScanRequest.builder()
                    .tableName(tableName)
                    .segment(segment)
                    .totalSegments(totalSegments)
                    .select("COUNT")
                    .limit(SCAN_LIMIT);
            if (lastEvaluatedKey != null) {
                scanBuilder.exclusiveStartKey(lastEvaluatedKey);
            }
            
            ScanResponse response = phoenixDBClientV2.scan(scanBuilder.build());
            segmentCount += response.count();
            lastEvaluatedKey = response.lastEvaluatedKey();
            
            // Verify no items are returned when using COUNT select
            Assert.assertTrue("COUNT segment scan should not return items", 
                    response.items() == null || response.items().isEmpty());
            
        } while (lastEvaluatedKey != null && !lastEvaluatedKey.isEmpty());
        
        return segmentCount;
    }
}
