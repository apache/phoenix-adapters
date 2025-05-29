import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.ddb.utils.IndexBuildingActivator;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;
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
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteGlobalSecondaryIndexAction;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexUpdate;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateTableResponse;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

public class UpdateTableIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateTableIT.class);

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
    public void updateTableDeleteIndexTest() throws Exception {
        final String tableName = testName.getMethodName();
        final String indexName = "g_IDX" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "SubjectNumber", ScalarAttributeType.N);
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "title",
                ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // describe table shows index
        DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
        DescribeTableResponse describeTableResponse = phoenixDBClientV2.describeTable(describeTableRequest);
        Assert.assertTrue(describeTableResponse.table().hasGlobalSecondaryIndexes());

        // update table, delete index
        UpdateTableRequest.Builder utr = UpdateTableRequest.builder().tableName(tableName);
        List<GlobalSecondaryIndexUpdate> indexUpdates = new ArrayList<>();
        GlobalSecondaryIndexUpdate deleteIndexAction
                = GlobalSecondaryIndexUpdate.builder().delete(
                        DeleteGlobalSecondaryIndexAction.builder().indexName(indexName).build())
                .build();
        indexUpdates.add(deleteIndexAction);
        utr.globalSecondaryIndexUpdates(indexUpdates);
        UpdateTableResponse utre = phoenixDBClientV2.updateTable(utr.build());
        Assert.assertEquals("DELETING", utre.tableDescription().globalSecondaryIndexes().get(0).indexStatus().toString());

        //describe table shows index in DELETING state
        describeTableResponse = phoenixDBClientV2.describeTable(describeTableRequest);
        Assert.assertEquals("DELETING", describeTableResponse.table().globalSecondaryIndexes().get(0).indexStatus().toString());

        //ddb
        utre = dynamoDbClient.updateTable(utr.build());
        Assert.assertEquals("DELETING", utre.tableDescription().globalSecondaryIndexes().get(0).indexStatus().toString());
    }

    @Test(timeout = 120000)
    public void updateTableCreateIndexTest() throws Exception {
        final String tableName = testName.getMethodName();
        final String indexName = "g_IDX" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "SubjectNumber", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // describe table shows no index
        DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
        DescribeTableResponse describeTableResponse = phoenixDBClientV2.describeTable(describeTableRequest);
        Assert.assertFalse(describeTableResponse.table().hasGlobalSecondaryIndexes());

        // update table, create index
        UpdateTableRequest.Builder utr = UpdateTableRequest.builder().tableName(tableName);
        List<GlobalSecondaryIndexUpdate> indexUpdates = new ArrayList<>();
        CreateGlobalSecondaryIndexAction.Builder createIndexAction = CreateGlobalSecondaryIndexAction.builder().indexName(indexName);
        List<KeySchemaElement> idxKeySchemaElements = new ArrayList<>();
        idxKeySchemaElements.add(KeySchemaElement.builder().attributeName("title").keyType(KeyType.HASH).build());
        createIndexAction.keySchema(idxKeySchemaElements);
        createIndexAction.projection(Projection.builder().projectionType(ProjectionType.ALL).build());
        createIndexAction.provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(50L).writeCapacityUnits(50L).build());
        GlobalSecondaryIndexUpdate createIndexUpdate
                = GlobalSecondaryIndexUpdate.builder().create(createIndexAction.build()).build();
        indexUpdates.add(createIndexUpdate);
        utr.globalSecondaryIndexUpdates(indexUpdates);
        List<AttributeDefinition> attrDefs = new ArrayList<>(createTableRequest.attributeDefinitions());
        attrDefs.add(AttributeDefinition.builder().attributeName("title").attributeType(ScalarAttributeType.S).build());
        utr.attributeDefinitions(attrDefs);
        UpdateTableResponse utre = phoenixDBClientV2.updateTable(utr.build());
        Assert.assertEquals("CREATING", utre.tableDescription().globalSecondaryIndexes().get(0).indexStatus().toString());

        //describe table shows index in CREATING state
        describeTableResponse = phoenixDBClientV2.describeTable(describeTableRequest);
        Assert.assertEquals("CREATING", describeTableResponse.table().globalSecondaryIndexes().get(0).indexStatus().toString());

        //ddb
        utre = dynamoDbClient.updateTable(utr.build());
        Assert.assertEquals("CREATING", utre.tableDescription().globalSecondaryIndexes().get(0).indexStatus().toString());

        //change index state
        try (Connection connection = DriverManager.getConnection(url)) {
            IndexBuildingActivator.activateIndexesForBuilding(connection, 0);
            PTable pTable = connection.unwrap(PhoenixConnection.class).getTableNoCache(tableName);
            Assert.assertEquals(PIndexState.BUILDING, pTable.getIndexes().get(0).getIndexState());
        }
        describeTableResponse = phoenixDBClientV2.describeTable(describeTableRequest);
        Assert.assertEquals("CREATING", describeTableResponse.table().globalSecondaryIndexes().get(0).indexStatus().toString());
    }

    @Test(timeout = 120000)
    public void updateTableCreateCDC() throws Exception {
        final String tableName = testName.getMethodName();
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "SubjectNumber", ScalarAttributeType.N);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // update table, enable cdc
        UpdateTableRequest.Builder utr = UpdateTableRequest.builder().tableName(tableName);
        utr.streamSpecification(StreamSpecification.builder().streamEnabled(true).streamViewType(StreamViewType.OLD_IMAGE).build());

        // response shows stream enabled
        UpdateTableResponse utre = phoenixDBClientV2.updateTable(utr.build());
        Assert.assertTrue(utre.tableDescription().streamSpecification().streamEnabled());
        Assert.assertEquals(StreamViewType.OLD_IMAGE, utre.tableDescription().streamSpecification().streamViewType());

        // describe table shows stream enabled
        DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
        DescribeTableResponse describeTableResponse = phoenixDBClientV2.describeTable(describeTableRequest);
        Assert.assertTrue(describeTableResponse.table().streamSpecification().streamEnabled());
        Assert.assertEquals(StreamViewType.OLD_IMAGE, describeTableResponse.table().streamSpecification().streamViewType());
    }

    @Test(timeout = 120000)
    public void updateTableCreateCDCAndDropIndex() throws Exception {
        final String tableName = testName.getMethodName();
        final String indexName = "g_IDX" + tableName;
        CreateTableRequest createTableRequest =
                DDLTestUtils.getCreateTableRequest(tableName, "ForumName", ScalarAttributeType.S,
                        "SubjectNumber", ScalarAttributeType.N);
        createTableRequest = DDLTestUtils.addIndexToRequest(true, createTableRequest, indexName, "title",
                ScalarAttributeType.S, null, null);
        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        // update table, enable cdc
        UpdateTableRequest.Builder utr = UpdateTableRequest.builder().tableName(tableName);
        utr.streamSpecification(StreamSpecification.builder().streamEnabled(true).streamViewType(StreamViewType.OLD_IMAGE).build());
        List<GlobalSecondaryIndexUpdate> indexUpdates = new ArrayList<>();
        GlobalSecondaryIndexUpdate deleteIndexAction
                = GlobalSecondaryIndexUpdate.builder().delete(
                        DeleteGlobalSecondaryIndexAction.builder().indexName(indexName).build())
                .build();
        indexUpdates.add(deleteIndexAction);
        utr.globalSecondaryIndexUpdates(indexUpdates);

        // response shows stream enabled and index dropped
        UpdateTableResponse utre = phoenixDBClientV2.updateTable(utr.build());
        Assert.assertTrue(utre.tableDescription().streamSpecification().streamEnabled());
        Assert.assertEquals(StreamViewType.OLD_IMAGE, utre.tableDescription().streamSpecification().streamViewType());
        Assert.assertEquals("DELETING", utre.tableDescription().globalSecondaryIndexes().get(0).indexStatus().toString());

        // describe table shows stream enabled and index dropped
        DescribeTableRequest describeTableRequest = DescribeTableRequest.builder().tableName(tableName).build();
        DescribeTableResponse describeTableResponse = phoenixDBClientV2.describeTable(describeTableRequest);
        Assert.assertTrue(describeTableResponse.table().streamSpecification().streamEnabled());
        Assert.assertEquals(StreamViewType.OLD_IMAGE, describeTableResponse.table().streamSpecification().streamViewType());
        Assert.assertEquals("DELETING", describeTableResponse.table().globalSecondaryIndexes().get(0).indexStatus().toString());
    }
}
