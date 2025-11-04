import java.sql.DriverManager;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.rest.RESTServer;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_NEW;

/**
 * Tests for UpdateItem API without conditional updates.
 * Every test does 3 things:
 * 1. Puts a row into the phoenix,ddb tables
 * 2. Updates this row
 * 3. Gets row from both phoenix and ddb to validate the update
 *
 * {@link UpdateItemIT} has more tests for UpdateItem API.
 */
@RunWith(Parameterized.class)
public class UpdateItemBaseTests {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemBaseTests.class);

    protected final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();
    protected static DynamoDbClient phoenixDBClientV2;

    private static String url;
    private static HBaseTestingUtility utility = null;
    private static String tmpDir;
    private static RESTServer restServer = null;

    @Rule
    public final TestName testName = new TestName();

    protected boolean isSortKeyPresent;

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

    @Parameters(name="SortKey_{0}")
    public static synchronized Collection<Boolean> data() {
        return Arrays.asList( false, true);
    }

    public UpdateItemBaseTests(boolean isSortKeyPresent) {
        this.isSortKeyPresent = isSortKeyPresent;
    }

    /**
     * SET: Adds one or more attributes and values to an item. If any of these attributes already
     * exist, they are replaced by the new values. You can also use SET to add or subtract from an
     * attribute that is of type Number.
     */
    @Test(timeout = 120000)
    public void testSet() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = :v1, #2 = #2 + :v2, #3 = #3 - :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("3.2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("89.34").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    /**
     * REMOVE - Removes one or more attributes from an item.
     */
    @Test(timeout = 120000)
    public void testRemove() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("REMOVE #1.#2[0], #3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "COL1");
        uir.expressionAttributeNames(exprAttrNames);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    /**
     * The ADD action only supports Number and set data types.
     * In addition, ADD can only be used on top-level attributes, not nested attributes.
     * Both sets must have the same primitive data type.
     *
     * TODO: test with add fails if item had Double to begin with,
     * e.g. 34.15 + 89.21, ddb --> 123.36, phoenix--> 123.35999999999999
     * but 34 + 89.21, ddb=phoenix-->123.21
     */
    @Test(timeout = 120000)
    public void testAdd() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("ADD #2 :v2, #3 :v3, #4 :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "TopLevelSet");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", AttributeValue.builder().n("-3.2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("89.21").build());
        exprAttrVal.put(":v4", AttributeValue.builder().ss("setMember2").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    /**
     * The DELETE action only supports set data types.
     * In addition, DELETE can only be used on top-level attributes, not nested attributes.
     */
    @Test(timeout = 120000)
    public void testDelete() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("DELETE #4 :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#4", "TopLevelSet");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v4", AttributeValue.builder().ss("setMember1", "setMember2").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testSetDelete() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = :v1, #2 = #2 + :v2 DELETE #4 :v4 ");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#4", "TopLevelSet");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().n("3.2").build());
        exprAttrVal.put(":v4", AttributeValue.builder().ss("setMember1").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testRemoveAdd() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("REMOVE #3, #1.#2[0] ADD #4 :v4, #5 :v5");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "COL1");
        exprAttrNames.put("#4", "TopLevelSet");
        exprAttrNames.put("#5", "COL4");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v5", AttributeValue.builder().n("-3.2").build());
        exprAttrVal.put(":v4", AttributeValue.builder().ss("setMember2").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testAddSetRemove() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("ADD #1 :v1, #2 :v2 SET #3 = :v3, #4 = #4 - :v4 REMOVE #5");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        exprAttrNames.put("#2", "TopLevelSet");
        exprAttrNames.put("#3", "COL2");
        exprAttrNames.put("#4", "COL4");
        exprAttrNames.put("#5", "COL3");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().n("-3.2").build());
        exprAttrVal.put(":v2", AttributeValue.builder().ss("setMember2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().s("TiTlE2").build());
        exprAttrVal.put(":v4", AttributeValue.builder().n("-3").build());
        uir.expressionAttributeValues(exprAttrVal);
        dynamoDbClient.updateItem(uir.build());
        phoenixDBClientV2.updateItem(uir.build());

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testDeleteRemoveSetAdd() {
        final String tableName = testName.getMethodName().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("DELETE #1 :v1 REMOVE #2 ADD #3 :v3 SET #4 = :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "TopLevelSet");
        exprAttrNames.put("#2", "Reviews");
        exprAttrNames.put("#3", "COL1");
        exprAttrNames.put("#4", "COL3");
        uir.expressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().ss("setMember2").build());
        exprAttrVal.put(":v3", AttributeValue.builder().n("1000000").build());
        exprAttrVal.put(":v4", AttributeValue.builder().s("dEsCrIpTiOn1").build());
        uir.expressionAttributeValues(exprAttrVal);
        uir.returnValues(ALL_NEW);
        UpdateItemResponse dynamoResult = dynamoDbClient.updateItem(uir.build());
        UpdateItemResponse phoenixResult = phoenixDBClientV2.updateItem(uir.build());
        Assert.assertEquals(dynamoResult.attributes(), phoenixResult.attributes());

        validateItem(tableName, key);
    }


    protected void createTableAndPutItem(String tableName) {
        //create table
        CreateTableRequest createTableRequest;
        if (isSortKeyPresent) {
            createTableRequest =
                    DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                            ScalarAttributeType.S, "PK2", ScalarAttributeType.N);
        } else {
            createTableRequest =
                    DDLTestUtils.getCreateTableRequest(tableName, "PK1",
                            ScalarAttributeType.S, null, null);
        }

        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = getItem1();
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);
    }

    protected void validateItem(String tableName, Map<String, AttributeValue> key) {
        GetItemRequest gir = GetItemRequest.builder().tableName(tableName).key(key).build();
        GetItemResponse phoenixResult = phoenixDBClientV2.getItem(gir);
        GetItemResponse dynamoResult = dynamoDbClient.getItem(gir);
        Assert.assertEquals(dynamoResult.item(), phoenixResult.item());
    }

    protected Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", AttributeValue.builder().s("A").build());
        item.put("PK2", AttributeValue.builder().n("1").build());
        item.put("COL1", AttributeValue.builder().n("1").build());
        item.put("COL2", AttributeValue.builder().s("Title1").build());
        item.put("COL3", AttributeValue.builder().s("Description1").build());
        item.put("COL4", AttributeValue.builder().n("34").build());
        item.put("TopLevelSet",  AttributeValue.builder().ss("setMember1").build());
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", AttributeValue.builder().s("Alice").build());
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", AttributeValue.builder().l(AttributeValue.builder().m(reviewMap1).build()).build());
        item.put("Reviews", AttributeValue.builder().m(fiveStarMap).build());
        return item;
    }

    protected Map<String, AttributeValue> getKey() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK1", AttributeValue.builder().s("A").build());
        if (isSortKeyPresent) key.put("PK2", AttributeValue.builder().n("1").build());
        return key;
    }
}
