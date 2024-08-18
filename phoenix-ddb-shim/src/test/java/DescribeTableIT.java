import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.util.JacksonUtil;
import org.apache.phoenix.util.PhoenixRuntime;
import org.junit.AfterClass;
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

    @Rule
    public final TestName testName = new TestName();

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;

    @BeforeClass
    public static void initialize() throws Exception {
        LocalDynamoDbTestBase.localDynamoDb().start();
        Configuration conf = HBaseConfiguration.create();
        HBaseTestingUtility utility = new HBaseTestingUtility(conf);
        setUpConfigForMiniCluster(conf);

        utility.startMiniCluster();
        String zkQuorum = "localhost:" + utility.getZkCluster().getClientPort();
        url = PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR + zkQuorum;
    }

    @AfterClass
    public static void stopLocalDynamoDb() {
        LocalDynamoDbTestBase.localDynamoDb().stop();
    }

    @Test
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
}
