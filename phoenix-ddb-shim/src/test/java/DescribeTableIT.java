import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClientV2;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * Tests for DescribeTable API. Brings up local DynamoDB server and HBase miniCluster, and tests
 * DescribeTable API for same table against both DDB and HBase/Phoenix servers and
 * compares the response.
 */
public class DescribeTableIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(DescribeTableIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

    private static String url;

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
    }

    @AfterClass
    public static void stopLocalDynamoDb() throws IOException, SQLException {
        LocalDynamoDbTestBase.localDynamoDb().stop();
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

    @Test(timeout = 120000)
    public void describeTableTest() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName,
                "PK1", ScalarAttributeType.B, "PK2", ScalarAttributeType.S);

        // add global index
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, "IDX1_" + tableName, "COL1",
                ScalarAttributeType.N, "COL2", ScalarAttributeType.B);

        // add local index
        createTableRequest = DDLTestUtils.addIndexToRequest(false, createTableRequest, "IDX2_" + tableName, "PK1",
                ScalarAttributeType.B, "LCOL2", ScalarAttributeType.S);

        // create table
        dynamoDbClient.createTable(createTableRequest);
        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        phoenixDBClientV2.createTable(createTableRequest);

        // describe table
        DescribeTableRequest dtr = DescribeTableRequest.builder().tableName(tableName).build();
        DescribeTableResponse describeTableResult1 = dynamoDbClient.describeTable(dtr);
        DescribeTableResponse describeTableResult2 = phoenixDBClientV2.describeTable(dtr);

        LOGGER.info("Describe Table response from DynamoDB: {}", describeTableResult1.toString());
        LOGGER.info("Describe Table response from Phoenix: {}", describeTableResult2.toString());

        DDLTestUtils.assertTableDescriptions(describeTableResult1.table(),
                describeTableResult2.table());
    }

    @Test(timeout = 120000)
    public void describeTableWithStreamTest() throws Exception {
        String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey",
                        ScalarAttributeType.B, "sortKey", ScalarAttributeType.N);

        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "OLD_IMAGE");

        PhoenixDBClientV2 phoenixDBClientV2 = new PhoenixDBClientV2(url);
        dynamoDbClient.createTable(createTableRequest);
        phoenixDBClientV2.createTable(createTableRequest);

        DescribeTableRequest dtr = DescribeTableRequest.builder().tableName(tableName).build();
        DescribeTableResponse describeTableResult1 = dynamoDbClient.describeTable(dtr);
        DescribeTableResponse describeTableResult2 = phoenixDBClientV2.describeTable(dtr);
        DDLTestUtils.assertTableDescriptions(describeTableResult1.table(),
                describeTableResult2.table());

        StreamSpecification streamSpec1 = describeTableResult1.table().streamSpecification();
        StreamSpecification streamSpec2 = describeTableResult2.table().streamSpecification();
        Assert.assertEquals(streamSpec1, streamSpec2);

        try (Connection connection = DriverManager.getConnection(url)) {
            DDLTestUtils.assertCDCMetadata(connection.unwrap(PhoenixConnection.class),
                    describeTableResult2.table(), "OLD_IMAGE");
        }
    }
}
