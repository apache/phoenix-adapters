import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Comprehensive parametrized test for segment scan functionality.
 * 
 * This test verifies that segment scans work correctly across different key configurations
 * ,segment counts, limits and filters. For each test run:
 * 1. Creates a table with specified key schema
 * 2. Inserts items while periodically splitting the underlying HBase table
 * 3. Performs both full scan and segment scan with pagination, limits and filters
 * 4. Verifies that both approaches return identical results to ddb
 */
@RunWith(Parameterized.class)
public class SegmentScanIT extends BaseSegmentScanIT {

    private final String hashKeyName;
    private final ScalarAttributeType hashKeyType;
    private final String sortKeyName;
    private final ScalarAttributeType sortKeyType;
    private final int totalSegments;
    private final int scanLimit;
    private final boolean useFilter;
    protected static final int TOTAL_ITEMS = 2000;
    protected static final int SPLIT_FREQUENCY = 500;

    public SegmentScanIT(String hashKeyName, ScalarAttributeType hashKeyType, 
                         String sortKeyName, ScalarAttributeType sortKeyType, 
                         int totalSegments, int scanLimit, boolean useFilter) {
        this.hashKeyName = hashKeyName;
        this.hashKeyType = hashKeyType;
        this.sortKeyName = sortKeyName;
        this.sortKeyType = sortKeyType;
        this.totalSegments = totalSegments;
        this.scanLimit = scanLimit;
        this.useFilter = useFilter;
    }

    @Parameterized.Parameters(name = "Hash_{1}_Sort_{3}_Segments_{4}_Limit_{5}_Filter_{6}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            // Hash key only
            {"pk", ScalarAttributeType.S, null, null, 3, 34, true},
            {"pk", ScalarAttributeType.N, null, null, 4, 52, false},
            {"pk", ScalarAttributeType.B, null, null, 2, 41, true},

            // String Hash Key combinations
            {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.S, 1, 28, false},
            {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.N, 2, 59, true},
            {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.B, 4, 45, false},

            // Number Hash Key combinations
            {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.S, 1, 23, true},
            {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.N, 2, 37, false},
            {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.B, 3, 51, true},

            // Binary Hash Key combinations
            {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.S, 2, 29, false},
            {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.N, 4, 46, true},
            {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.B, 3, 33, false},

            // Total Segments > #regions
            {"pk", ScalarAttributeType.S, null, null, 5, 58, true},
            {"pk", ScalarAttributeType.N, null, null, 6, 25, false},
            {"pk", ScalarAttributeType.B, null, null, 7, 42, true},
            {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.S, 5, 31, false},
            {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.N, 6, 55, true},
            {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.B, 7, 38, false},
        });
    }


    @Test(timeout = 300000)
    public void testSegmentScan() throws Exception {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "_");

        // Create table
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(
                tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // Insert items with periodic table splitting
        insertItemsWithSplitting(tableName, hashKeyName, hashKeyType, sortKeyName, sortKeyType, 
                                TOTAL_ITEMS, SPLIT_FREQUENCY, 100);

        // Perform full scan with pagination
        List<Map<String, AttributeValue>> fullScanItemsPhoenix = performFullScanWithPagination(phoenixDBClientV2, tableName, useFilter);
        List<Map<String, AttributeValue>> fullScanItemsDDB = performFullScanWithPagination(dynamoDbClient, tableName, useFilter);

        // Perform segment scan serially on all segments with pagination
        List<Map<String, AttributeValue>> segmentScanItems = performSegmentScanWithPagination(tableName, useFilter);

        // Verify results
        TestUtils.verifyItemsEqual(fullScanItemsDDB, fullScanItemsPhoenix, hashKeyName, sortKeyName);
        TestUtils.verifyItemsEqual(fullScanItemsPhoenix, segmentScanItems, hashKeyName, sortKeyName);
    }

    /**
     * Perform segment scan across all segments with pagination.
     */
    private List<Map<String, AttributeValue>> performSegmentScanWithPagination(String tableName, boolean useFilter)
            throws SQLException {
        List<Map<String, AttributeValue>> allItems = new ArrayList<>();
        
        // Scan each segment sequentially
        int numRegions = TestUtils.getNumberOfTableRegions(testConnection, tableName);
        for (int segment = 0; segment < totalSegments; segment++) {
            List<Map<String, AttributeValue>> segmentItems = 
                scanSingleSegmentWithPagination(tableName, segment, totalSegments, scanLimit, false, useFilter);
            allItems.addAll(segmentItems);
            if (segment < numRegions && !useFilter) {
                Assert.assertFalse(segmentItems.isEmpty());
            }
        }
        return allItems;
    }
} 