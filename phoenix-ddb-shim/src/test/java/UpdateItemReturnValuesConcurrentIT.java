import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemResponse;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClientV2;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static software.amazon.awssdk.services.dynamodb.model.ReturnValue.ALL_NEW;
import static software.amazon.awssdk.services.dynamodb.model.ReturnValuesOnConditionCheckFailure.ALL_OLD;
import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;

/**
 * For both dynamodb and phoenix:
 * Update the same item with the same UpdateItemRequest using 5 threads concurrently.
 * Use different combinations of ConditionExpression, ReturnValue and ReturnValuesOnConditionCheckFailure
 * Verify how many threads saw a successful update vs how many saw an Exception.
 */
@RunWith(Parameterized.class)
public class UpdateItemReturnValuesConcurrentIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemReturnValuesConcurrentIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    protected static PhoenixDBClientV2 phoenixDBClientV2 = null;

    protected final DynamoDbClient dynamoDbClient =
            LocalDynamoDbTestBase.localDynamoDb().createV2Client();

    private static String url;

    @Rule
    public final TestName testName = new TestName();

    private boolean hasCondition;
    private boolean isConditionTrue;
    private boolean hasReturnVal;
    private boolean hasReturnValOnConditionFailure;

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
        phoenixDBClientV2 = new PhoenixDBClientV2(url);
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

    @Parameterized.Parameters(name="UpdateItemReturnValuesIT_isConditionTrue_{1}_hasReturnValue_{2}_hasReturnValueOnConditionCheck_{3}")
    public static synchronized Collection<Boolean[]> data() {
        return Arrays.asList(new Boolean[][] {
                { false, false, false, false },
                { true, false, true, false },
                { true, false, true, true },
                { true, true, true, false },
                { true, true, true, true  }
        });
    }

    public UpdateItemReturnValuesConcurrentIT(boolean hasCondition, boolean isConditionTrue,
                                              boolean hasReturnVal, boolean hasReturnValOnConditionFailure) {
        this.hasCondition = hasCondition;
        this.isConditionTrue = isConditionTrue;
        this.hasReturnVal = hasReturnVal;
        this.hasReturnValOnConditionFailure = hasReturnValOnConditionFailure;
    }

    @Test(timeout = 120000)
    public void test() {
        final String tableName = testName.getMethodName().toUpperCase().replaceAll("[\\[\\]]", "");
        createTableAndPutItem(tableName);
        UpdateItemRequest uir = getUpdateItemRequest(tableName);
        int[] dynamoResult = getUpdateAndErrorCounts(uir, dynamoDbClient);
        int[] phoenixResult = getUpdateAndErrorCounts(uir, phoenixDBClientV2);
        Assert.assertArrayEquals(dynamoResult, phoenixResult);
    }

    private int[] getUpdateAndErrorCounts(UpdateItemRequest uir, DynamoDbClient dbClient) {
        ExecutorService executorService1 = Executors.newFixedThreadPool(5);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger errorCount = new AtomicInteger(0);

        for (int i = 0; i < 5; i++) {
            executorService1.submit(() -> {
                try {
                    UpdateItemResponse result = dbClient.updateItem(uir);
                    if (hasReturnVal) {
                        Assert.assertNotNull(result.attributes());
                    } else {
                        Assert.assertNull(result.attributes());
                    }
                    updateCount.incrementAndGet();
                } catch (ConditionalCheckFailedException e) {
                    if (hasReturnValOnConditionFailure) {
                        Assert.assertNotNull(e.item());
                    } else {
                        Assert.assertNull(e.item());
                    }
                    errorCount.incrementAndGet();
                }
            });
        }
        executorService1.shutdown();
        try {
            executorService1.awaitTermination(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Assert.fail("testConcurrentConditionalUpdateWithReturnValues was interrupted.");
        }
        return new int[] {updateCount.get(), errorCount.get()};
    }

    private UpdateItemRequest getUpdateItemRequest(String tableName) {
        Map<String, AttributeValue> key = getKey();
        UpdateItemRequest.Builder uir = UpdateItemRequest.builder().tableName(tableName).key(key);
        uir.updateExpression("SET #1 = #1 + :v1");
        Map<String, String> exprAttrNames = new HashMap<>();
        exprAttrNames.put("#1", "col1");
        Map<String, AttributeValue> exprAttrVal = new HashMap<>();
        exprAttrVal.put(":v1", AttributeValue.builder().n("20").build());
        if (hasCondition) {
            uir.conditionExpression("#1 < :v2");
            if (isConditionTrue) {
                exprAttrVal.put(":v2", AttributeValue.builder().n("2").build());
            } else {
                exprAttrVal.put(":v2", AttributeValue.builder().n("0").build());
            }
        }
        uir.expressionAttributeNames(exprAttrNames);
        uir.expressionAttributeValues(exprAttrVal);
        if (hasReturnVal) {
            uir.returnValues(ALL_NEW);
        }
        if (hasReturnValOnConditionFailure) {
            uir.returnValuesOnConditionCheckFailure(ALL_OLD);
        }
        return uir.build();
    }

    private void createTableAndPutItem(String tableName) {
        //create table
        CreateTableRequest createTableRequest = DDLTestUtils.getCreateTableRequest(tableName, "pk1",
                            ScalarAttributeType.S, "pk2", ScalarAttributeType.N);

        phoenixDBClientV2.createTable(createTableRequest);
        dynamoDbClient.createTable(createTableRequest);

        //put item
        Map<String, AttributeValue> item = getItem1();
        PutItemRequest putItemRequest = PutItemRequest.builder().tableName(tableName).item(item).build();
        phoenixDBClientV2.putItem(putItemRequest);
        dynamoDbClient.putItem(putItemRequest);
    }

    private Map<String, AttributeValue> getItem1() {
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("pk1", AttributeValue.builder().s("A").build());
        item.put("pk2", AttributeValue.builder().n("1").build());
        item.put("col1", AttributeValue.builder().n("1").build());
        return item;
    }

    private Map<String, AttributeValue> getKey() {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("pk1", AttributeValue.builder().s("A").build());
        key.put("pk2", AttributeValue.builder().n("1").build());
        return key;
    }
}
