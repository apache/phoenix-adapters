import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.apache.phoenix.util.JacksonUtil;
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

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

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
        DDLTestUtils.addIndexToRequest(true, createTableRequest, "IDX1_" + tableName, "COL1",
                ScalarAttributeType.N, "COL2", ScalarAttributeType.B);

        // add local index
        DDLTestUtils.addIndexToRequest(false, createTableRequest, "IDX2_" + tableName, "PK1",
                ScalarAttributeType.B, "LCOL2", ScalarAttributeType.S);

        // create table
        amazonDynamoDB.createTable(createTableRequest);
        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        phoenixDBClient.createTable(createTableRequest);

        // describe table
        DescribeTableResult describeTableResult1 = amazonDynamoDB.describeTable(tableName);
        DescribeTableResult describeTableResult2 = phoenixDBClient.describeTable(tableName);
        LOGGER.info("Describe Table response from DynamoDB: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(describeTableResult1));
        LOGGER.info("Describe Table response from Phoenix: {}",
                JacksonUtil.getObjectWriterPretty().writeValueAsString(describeTableResult2));
        DDLTestUtils.assertTableDescriptions(describeTableResult1.getTable(),
                describeTableResult2.getTable());
    }

    @Test(timeout = 120000)
    public void describeTableWithStreamTest() throws Exception {
        String tableName = testName.getMethodName().toUpperCase();
        // create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey",
                        ScalarAttributeType.B, "sortKey", ScalarAttributeType.N);

        DDLTestUtils.addStreamSpecToRequest(createTableRequest, "OLD_IMAGE");

        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        amazonDynamoDB.createTable(createTableRequest);
        phoenixDBClient.createTable(createTableRequest);

        DescribeTableResult describeTableResult1 = amazonDynamoDB.describeTable(tableName);
        DescribeTableResult describeTableResult2 = phoenixDBClient.describeTable(tableName);
        DDLTestUtils.assertTableDescriptions(describeTableResult1.getTable(),
                describeTableResult2.getTable());

        StreamSpecification streamSpec1 = describeTableResult1.getTable().getStreamSpecification();
        StreamSpecification streamSpec2 = describeTableResult2.getTable().getStreamSpecification();
        Assert.assertEquals(streamSpec1, streamSpec2);

        try (Connection connection = DriverManager.getConnection(url)) {
            DDLTestUtils.assertCDCMetadata(connection.unwrap(PhoenixConnection.class),
                    describeTableResult2.getTable(), "OLD_IMAGE");
        }
    }
}
