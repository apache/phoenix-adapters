import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBStreams;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsRequest;
import com.amazonaws.services.dynamodbv2.model.ListStreamsResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.Stream;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
import org.apache.phoenix.ddb.PhoenixDBStreamsClient;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
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

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ListStreamsIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ListStreamsIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    @Rule
    public final TestName testName = new TestName();

    private final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private final AmazonDynamoDBStreams amazonDynamoDBStreams =
            LocalDynamoDbTestBase.localDynamoDb().createV1StreamsClient();

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
    public void testListStreamsWithOnlyOneActiveStream() throws ParseException {
        String tableName = testName.getMethodName().toUpperCase();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "hashKey",
                        ScalarAttributeType.B, "sortKey", ScalarAttributeType.N);

        DDLTestUtils.addStreamSpecToRequest(createTableRequest, "NEW_IMAGE");

        PhoenixDBClient phoenixDBClient = new PhoenixDBClient(url);
        PhoenixDBStreamsClient phoenixDBStreamsClient = new PhoenixDBStreamsClient(url);
        amazonDynamoDB.createTable(createTableRequest);
        phoenixDBClient.createTable(createTableRequest);

        ListStreamsRequest lsr = new ListStreamsRequest().withTableName(tableName);
        ListStreamsResult phoenixResult = phoenixDBStreamsClient.listStreams(lsr);
        ListStreamsResult dynamoResult = amazonDynamoDBStreams.listStreams(lsr);
        LOGGER.info("ListStreamsResult in Phoenix: " + phoenixResult);
        LOGGER.info("ListStreamsResult in DDB: " + dynamoResult);

        Assert.assertEquals(dynamoResult.getStreams().size(), phoenixResult.getStreams().size());
        Stream phoenixStream = phoenixResult.getStreams().get(0);
        Assert.assertEquals(tableName, phoenixStream.getTableName());
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz");
        Date date = df.parse(phoenixStream.getStreamLabel());
        Assert.assertTrue(phoenixStream.getStreamArn().contains(String.valueOf(date.getTime())));
    }
}