import java.sql.DriverManager;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableResponse;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class DeleteTableIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTableIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    private final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    private static DynamoDbClient phoenixDBClientV2;

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
    public void deleteTableTestWithDeleteTableRequest() throws Exception {
        final String tableName = testName.getMethodName().toUpperCase();
        //create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1", ScalarAttributeType.B, "PK2",
                        ScalarAttributeType.S);
        //creating table for aws
        dynamoDbClient.createTable(createTableRequest);
        Thread.sleep(2000);
        //creating table for phoenix
        phoenixDBClientV2.createTable(createTableRequest);

        //delete table request
        DeleteTableRequest deleteTableRequest =
                DeleteTableRequest.builder().tableName(tableName).build();

        //delete table for aws
        DeleteTableResponse DeleteTableResponse1 = dynamoDbClient.deleteTable(deleteTableRequest);
        //delete table for phoenix
        DeleteTableResponse DeleteTableResponse2 =
                phoenixDBClientV2.deleteTable(deleteTableRequest);

        LOGGER.info("Delete Table response from DynamoDB: {}", DeleteTableResponse1.toString());
        LOGGER.info("Delete Table response from Phoenix: {}", DeleteTableResponse2.toString());

        TableDescription tableDescription1 = DeleteTableResponse1.tableDescription();
        TableDescription tableDescription2 = DeleteTableResponse2.tableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);

    }

    @Test(timeout = 120000)
    public void deleteTableTestWithMixedCaseTableName() throws Exception {
        final String tableName = testName.getMethodName();
        //create table request
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                        ScalarAttributeType.B, "PK2", ScalarAttributeType.S);
        //creating table for aws
        dynamoDbClient.createTable(createTableRequest);
        Thread.sleep(2000);
        //creating table for phoenix
        phoenixDBClientV2.createTable(createTableRequest);

        //delete table request
        DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder().tableName(tableName).build();

        //delete table for aws
        DeleteTableResponse DeleteTableResponse1 = dynamoDbClient.deleteTable(deleteTableRequest);
        //delete table for phoenix
        DeleteTableResponse DeleteTableResponse2 = phoenixDBClientV2.deleteTable(deleteTableRequest);

        LOGGER.info("Delete Table response from DynamoDB: {}", DeleteTableResponse1.toString());
        LOGGER.info("Delete Table response from Phoenix: {}", DeleteTableResponse2.toString());

        TableDescription tableDescription1 = DeleteTableResponse1.tableDescription();
        TableDescription tableDescription2 = DeleteTableResponse2.tableDescription();
        DDLTestUtils.assertTableDescriptions(tableDescription1, tableDescription2);

    }
}

