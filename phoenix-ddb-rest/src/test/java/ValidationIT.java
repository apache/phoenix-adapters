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
import software.amazon.awssdk.core.exception.SdkServiceException;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.QueryRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.ScanRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;

import java.sql.DriverManager;
import java.util.HashMap;
import java.util.Map;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class ValidationIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValidationIT.class);

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

    @Test
    public void testUnsupportedReturnValues() throws Exception {
        String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK", AttributeValue.builder().s("key1").build());
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK", AttributeValue.builder().s("key1").build());
        UpdateItemRequest uir = UpdateItemRequest.builder().tableName(tableName).key(key).updateExpression("REMOVE COL").returnValues("UPDATED_OLD").build();
        try {
            phoenixDBClientV2.updateItem(uir);
            Assert.fail("UpdateItem with unsupported ReturnValue should have given 400 Bad Request.");
        } catch (SdkServiceException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage().contains("not supported for ReturnValue"));
        }
    }

    @Test
    public void testStreamViewType() throws Exception {
        String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK",
                        ScalarAttributeType.S, null, null);
        createTableRequest = DDLTestUtils.addStreamSpecToRequest(createTableRequest, "");
        try {
            phoenixDBClientV2.createTable(createTableRequest);
            Assert.fail("CreateTable with StreamEnabled but no StreamViewType should have given 400 Bad Request.");
        } catch (SdkServiceException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage().contains("STREAM_VIEW_TYPE attribute is required"));
        }
    }

    @Test
    public void testLegacyParamsValidation() {
        String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "PK",
                        ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK", AttributeValue.builder().s("key1").build());
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK", AttributeValue.builder().s("key1").build());
        PutItemRequest pir = PutItemRequest.builder().tableName(tableName).item(item).conditionalOperator("AND").build();
        UpdateItemRequest uir = UpdateItemRequest.builder().tableName(tableName).key(key).updateExpression("REMOVE COL").conditionalOperator("OR").build();
       DeleteItemRequest dir = DeleteItemRequest.builder().tableName(tableName).key(key).conditionalOperator("AND").build();
        GetItemRequest get = GetItemRequest.builder().tableName(tableName).key(key).attributesToGet("COL").build();
        QueryRequest query = QueryRequest.builder().tableName(tableName).attributesToGet("COL").build();
        ScanRequest scan = ScanRequest.builder().tableName(tableName).attributesToGet("COL").build();

        try {
            phoenixDBClientV2.putItem(pir);
            Assert.fail("PutItem with legacy parameter should have given 400 Bad Request.");
        } catch (SdkServiceException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage().contains("Legacy parameter"));
        }
        try {
            phoenixDBClientV2.updateItem(uir);
            Assert.fail("UpdateItem with legacy parameter should have given 400 Bad Request.");
        } catch (SdkServiceException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage().contains("Legacy parameter"));
        }
        try {
            phoenixDBClientV2.deleteItem(dir);
            Assert.fail("DeleteItem with legacy parameter should have given 400 Bad Request.");
        } catch (SdkServiceException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage().contains("Legacy parameter"));
        }
        try {
            phoenixDBClientV2.getItem(get);
            Assert.fail("GetItem with legacy parameter should have given 400 Bad Request.");
        } catch (SdkServiceException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage().contains("Legacy parameter"));
        }
        try {
            phoenixDBClientV2.query(query);
            Assert.fail("Query with legacy parameter should have given 400 Bad Request.");
        } catch (SdkServiceException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage().contains("Legacy parameter"));
        }
        try {
            phoenixDBClientV2.scan(scan);
            Assert.fail("Scan with legacy parameter should have given 400 Bad Request.");
        } catch (SdkServiceException e) {
            Assert.assertEquals(400, e.statusCode());
            Assert.assertTrue(e.getMessage().contains("Legacy parameter"));
        }

    }
}
