import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.UpdateItemRequest;
import com.amazonaws.services.dynamodbv2.model.UpdateItemResult;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClient;
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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static com.amazonaws.services.dynamodbv2.model.ReturnValue.ALL_NEW;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

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

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    protected static PhoenixDBClient phoenixDBClient = null;

    protected final AmazonDynamoDB amazonDynamoDB =
            LocalDynamoDbTestBase.localDynamoDb().createV1Client();

    private static String url;

    @Rule
    public final TestName testName = new TestName();

    private boolean isSortKeyPresent;

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
        phoenixDBClient = new PhoenixDBClient(url);
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
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("SET #1 = :v1, #2 = #2 + :v2, #3 = #3 - :v3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", new AttributeValue().withS("TiTlE2"));
        exprAttrVal.put(":v2", new AttributeValue().withN("3.2"));
        exprAttrVal.put(":v3", new AttributeValue().withN("89.34"));
        uir.setExpressionAttributeValues(exprAttrVal);
        amazonDynamoDB.updateItem(uir);
        phoenixDBClient.updateItem(uir);

        validateItem(tableName, key);
    }

    /**
     * REMOVE - Removes one or more attributes from an item.
     */
    @Test(timeout = 120000)
    public void testRemove() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("REMOVE #1.#2[0], #3");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "COL1");
        uir.setExpressionAttributeNames(exprAttrNames);
        amazonDynamoDB.updateItem(uir);
        phoenixDBClient.updateItem(uir);

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
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("ADD #2 :v2, #3 :v3, #4 :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#3", "COL4");
        exprAttrNames.put("#4", "TopLevelSet");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v2", new AttributeValue().withN("-3.2"));
        exprAttrVal.put(":v3", new AttributeValue().withN("89.21"));
        exprAttrVal.put(":v4", new AttributeValue().withSS("setMember2"));
        uir.setExpressionAttributeValues(exprAttrVal);
        amazonDynamoDB.updateItem(uir);
        phoenixDBClient.updateItem(uir);

        validateItem(tableName, key);
    }

    /**
     * The DELETE action only supports set data types.
     * In addition, DELETE can only be used on top-level attributes, not nested attributes.
     */
    @Test(timeout = 120000)
    public void testDelete() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("DELETE #4 :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#4", "TopLevelSet");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v4", new AttributeValue().withSS("setMember1", "setMember2"));
        uir.setExpressionAttributeValues(exprAttrVal);
        amazonDynamoDB.updateItem(uir);
        phoenixDBClient.updateItem(uir);

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testSetDelete() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("SET #1 = :v1, #2 = #2 + :v2 DELETE #4 :v4 ");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL2");
        exprAttrNames.put("#2", "COL1");
        exprAttrNames.put("#4", "TopLevelSet");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", new AttributeValue().withS("TiTlE2"));
        exprAttrVal.put(":v2", new AttributeValue().withN("3.2"));
        exprAttrVal.put(":v4", new AttributeValue().withSS("setMember1"));
        uir.setExpressionAttributeValues(exprAttrVal);
        amazonDynamoDB.updateItem(uir);
        phoenixDBClient.updateItem(uir);

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testRemoveAdd() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("REMOVE #3, #1.#2[0] ADD #4 :v4, #5 :v5");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "Reviews");
        exprAttrNames.put("#2", "FiveStar");
        exprAttrNames.put("#3", "COL1");
        exprAttrNames.put("#4", "TopLevelSet");
        exprAttrNames.put("#5", "COL4");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v5", new AttributeValue().withN("-3.2"));
        exprAttrVal.put(":v4", new AttributeValue().withSS("setMember2"));
        uir.setExpressionAttributeValues(exprAttrVal);
        amazonDynamoDB.updateItem(uir);
        phoenixDBClient.updateItem(uir);

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testAddSetRemove() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("ADD #1 :v1, #2 :v2 SET #3 = :v3, #4 = #4 - :v4 REMOVE #5");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "COL1");
        exprAttrNames.put("#2", "TopLevelSet");
        exprAttrNames.put("#3", "COL2");
        exprAttrNames.put("#4", "COL4");
        exprAttrNames.put("#5", "COL3");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", new AttributeValue().withN("-3.2"));
        exprAttrVal.put(":v2", new AttributeValue().withSS("setMember2"));
        exprAttrVal.put(":v3", new AttributeValue().withS("TiTlE2"));
        exprAttrVal.put(":v4", new AttributeValue().withN("-3"));
        uir.setExpressionAttributeValues(exprAttrVal);
        amazonDynamoDB.updateItem(uir);
        phoenixDBClient.updateItem(uir);

        validateItem(tableName, key);
    }

    @Test(timeout = 120000)
    public void testDeleteRemoveSetAdd() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);

        // update item
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest uir = new UpdateItemRequest().withTableName(tableName).withKey(key);
        uir.setUpdateExpression("DELETE #1 :v1 REMOVE #2 ADD #3 :v3 SET #4 = :v4");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "TopLevelSet");
        exprAttrNames.put("#2", "Reviews");
        exprAttrNames.put("#3", "COL1");
        exprAttrNames.put("#4", "COL3");
        uir.setExpressionAttributeNames(exprAttrNames);
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", new AttributeValue().withSS("setMember2"));
        exprAttrVal.put(":v3", new AttributeValue().withN("1000000"));
        exprAttrVal.put(":v4", new AttributeValue().withS("dEsCrIpTiOn1"));
        uir.setExpressionAttributeValues(exprAttrVal);
        uir.setReturnValues(ALL_NEW);
        UpdateItemResult dynamoResult = amazonDynamoDB.updateItem(uir);
        UpdateItemResult phoenixResult = phoenixDBClient.updateItem(uir);
        Assert.assertEquals(dynamoResult.getAttributes(), phoenixResult.getAttributes());

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

        phoenixDBClient.createTable(createTableRequest);
        amazonDynamoDB.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = getItem1();
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        phoenixDBClient.putItem(putItemRequest);
        amazonDynamoDB.putItem(putItemRequest);
    }

    protected void validateItem(String tableName, Map<String, AttributeValue> key) {
        GetItemRequest gir = new GetItemRequest(tableName, key);
        GetItemResult phoenixResult = phoenixDBClient.getItem(gir);
        GetItemResult dynamoResult = amazonDynamoDB.getItem(gir);
        Assert.assertEquals(dynamoResult.getItem(), phoenixResult.getItem());
    }

    protected Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("PK1", new AttributeValue().withS("A"));
        item.put("PK2", new AttributeValue().withN("1"));
        item.put("COL1", new AttributeValue().withN("1"));
        item.put("COL2", new AttributeValue().withS("Title1"));
        item.put("COL3", new AttributeValue().withS("Description1"));
        item.put("COL4", new AttributeValue().withN("34"));
        item.put("TopLevelSet",  new AttributeValue().withSS("setMember1"));
        Map<String, AttributeValue> reviewMap1 = new HashMap<>();
        reviewMap1.put("reviewer", new AttributeValue().withS("Alice"));
        Map<String, AttributeValue> fiveStarMap = new HashMap<>();
        fiveStarMap.put("FiveStar", new AttributeValue().withL(new AttributeValue().withM(reviewMap1)));
        item.put("Reviews", new AttributeValue().withM(fiveStarMap));
        return item;
    }

    protected Map<String, AttributeValue> getKey() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("PK1", new AttributeValue().withS("A"));
        if (isSortKeyPresent) key.put("PK2", new AttributeValue().withN("1"));
        return key;
    }
}
