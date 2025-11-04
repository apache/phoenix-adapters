import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
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
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesRequest;
import software.amazon.awssdk.services.dynamodb.model.ListTablesResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

import java.sql.DriverManager;

import static org.apache.phoenix.query.BaseTest.generateUniqueName;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ListTablesIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(ListTablesIT.class);

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

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

    @Test(timeout = 120000)
    public void listTablesTest() throws Exception {
        CreateTableRequest createTableRequest;
        for (int i=0; i<10; i++) {
            createTableRequest = DDLTestUtils.getCreateTableRequest(
                    "tbl-Parallel._Runtime-Void--EndPoint_Shred.DaemonCloud_17X-XX.9."
                            + generateUniqueName(), "PK1", ScalarAttributeType.B, "PK2",
                    ScalarAttributeType.S);
            phoenixDBClientV2.createTable(createTableRequest);
            dynamoDbClient.createTable(createTableRequest);
        }

        ListTablesRequest.Builder listTablesRequest = ListTablesRequest.builder().limit(2);
        ListTablesResponse phoenixResponse, dynamoDbResponse;
        do {
            phoenixResponse = phoenixDBClientV2.listTables(listTablesRequest.build());
            dynamoDbResponse = dynamoDbClient.listTables(listTablesRequest.build());
            Assert.assertEquals(dynamoDbResponse.tableNames(), phoenixResponse.tableNames());
            listTablesRequest.exclusiveStartTableName(phoenixResponse.lastEvaluatedTableName());
        } while (phoenixResponse.lastEvaluatedTableName() != null);

        listTablesRequest = ListTablesRequest.builder();
        phoenixResponse = phoenixDBClientV2.listTables(listTablesRequest.build());
        dynamoDbResponse = dynamoDbClient.listTables(listTablesRequest.build());
        Assert.assertEquals(dynamoDbResponse.tableNames(), phoenixResponse.tableNames());
    }
}
