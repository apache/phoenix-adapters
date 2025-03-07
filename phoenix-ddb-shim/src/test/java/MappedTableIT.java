import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.phoenix.ddb.PhoenixDBClientV2;
import org.apache.phoenix.end2end.ServerMetadataCacheTestImpl;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.util.PhoenixRuntime;
import org.apache.phoenix.util.ServerUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedClient;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbTable;
import software.amazon.awssdk.enhanced.dynamodb.Expression;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.mapper.StaticTableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.DeleteItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.EnhancedGlobalSecondaryIndex;
import software.amazon.awssdk.enhanced.dynamodb.model.PutItemEnhancedRequest;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.ConditionalCheckFailedException;
import software.amazon.awssdk.services.dynamodb.model.DeleteTableRequest;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Objects;

import static org.apache.phoenix.query.BaseTest.setUpConfigForMiniCluster;
import static software.amazon.awssdk.enhanced.dynamodb.internal.AttributeValues.stringValue;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primaryPartitionKey;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.primarySortKey;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.secondaryPartitionKey;
import static software.amazon.awssdk.enhanced.dynamodb.mapper.StaticAttributeTags.secondarySortKey;

public class MappedTableIT {
    private static final Logger LOGGER = LoggerFactory.getLogger(MappedTableIT.class);

    private static HBaseTestingUtility utility = null;
    private static String tmpDir;

    private static DynamoDbEnhancedClient ddbPhoenixEnhancedClient = null;
    private static DynamoDbClient phoenixDBClientV2 = null;
    private DynamoDbTable<Record> mappedTable = null;
    private DynamoDbTable<ShortRecord> mappedShortTable = null;

    private static String url;

    private static final String TABLE_NAME = "TEST_MAPPED_TABLE_NAME";

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
        ddbPhoenixEnhancedClient = DynamoDbEnhancedClient.builder()
                .dynamoDbClient(phoenixDBClientV2)
                .build();
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

    private static final String ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS = "attribute3";

    private static class Record {
        private String id;
        private String sort;
        private String attribute;
        private String attribute2;
        private String attribute3;

        private String getId() {
            return id;
        }

        private Record setId(String id) {
            this.id = id;
            return this;
        }

        private String getSort() {
            return sort;
        }

        private Record setSort(String sort) {
            this.sort = sort;
            return this;
        }

        private String getAttribute() {
            return attribute;
        }

        private Record setAttribute(String attribute) {
            this.attribute = attribute;
            return this;
        }

        private String getAttribute2() {
            return attribute2;
        }

        private Record setAttribute2(String attribute2) {
            this.attribute2 = attribute2;
            return this;
        }

        private String getAttribute3() {
            return attribute3;
        }

        private Record setAttribute3(String attribute3) {
            this.attribute3 = attribute3;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Record record = (Record) o;
            return Objects.equals(id, record.id) &&
                    Objects.equals(sort, record.sort) &&
                    Objects.equals(attribute, record.attribute) &&
                    Objects.equals(attribute2, record.attribute2) &&
                    Objects.equals(attribute3, record.attribute3);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, sort, attribute, attribute2, attribute3);
        }
    }

    private static class ShortRecord {
        private String id;
        private String sort;
        private String attribute;

        private String getId() {
            return id;
        }

        private ShortRecord setId(String id) {
            this.id = id;
            return this;
        }

        private String getSort() {
            return sort;
        }

        private ShortRecord setSort(String sort) {
            this.sort = sort;
            return this;
        }

        private String getAttribute() {
            return attribute;
        }

        private ShortRecord setAttribute(String attribute) {
            this.attribute = attribute;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ShortRecord that = (ShortRecord) o;
            return Objects.equals(id, that.id) &&
                    Objects.equals(sort, that.sort) &&
                    Objects.equals(attribute, that.attribute);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id, sort, attribute);
        }
    }

    private static final TableSchema<Record> TABLE_SCHEMA =
            StaticTableSchema.builder(Record.class)
                    .newItemSupplier(Record::new)
                    .addAttribute(String.class, a -> a.name("id")
                            .getter(Record::getId)
                            .setter(Record::setId)
                            .tags(primaryPartitionKey()))
                    .addAttribute(String.class, a -> a.name("sort")
                            .getter(Record::getSort)
                            .setter(Record::setSort)
                            .tags(primarySortKey()))
                    .addAttribute(String.class, a -> a.name("attribute")
                            .getter(Record::getAttribute)
                            .setter(Record::setAttribute))
                    .addAttribute(String.class, a -> a.name("attribute2*")
                            .getter(Record::getAttribute2)
                            .setter(Record::setAttribute2)
                            .tags(secondaryPartitionKey("gsi_1")))
                    .addAttribute(String.class, a -> a.name(ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS)
                            .getter(Record::getAttribute3)
                            .setter(Record::setAttribute3)
                            .tags(secondarySortKey("gsi_1")))
                    .build();

    private static final TableSchema<ShortRecord> SHORT_TABLE_SCHEMA =
            StaticTableSchema.builder(ShortRecord.class)
                    .newItemSupplier(ShortRecord::new)
                    .addAttribute(String.class, a -> a.name("id")
                            .getter(ShortRecord::getId)
                            .setter(ShortRecord::setId)
                            .tags(primaryPartitionKey()))
                    .addAttribute(String.class, a -> a.name("sort")
                            .getter(ShortRecord::getSort)
                            .setter(ShortRecord::setSort)
                            .tags(primarySortKey()))
                    .addAttribute(String.class, a -> a.name("attribute")
                            .getter(ShortRecord::getAttribute)
                            .setter(ShortRecord::setAttribute))
                    .build();


    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Before
    public void createTable() {
        mappedTable = ddbPhoenixEnhancedClient.table(TABLE_NAME, TABLE_SCHEMA);
        mappedShortTable = ddbPhoenixEnhancedClient.table(TABLE_NAME, SHORT_TABLE_SCHEMA);
        mappedTable.createTable(r -> r.globalSecondaryIndices(
                        EnhancedGlobalSecondaryIndex.builder()
                                .indexName("gsi_1")
                                .projection(p -> p.projectionType(ProjectionType.ALL))
                                .build()));
    }

    @After
    public void deleteTable() {
        phoenixDBClientV2.deleteTable(DeleteTableRequest.builder()
                .tableName(TABLE_NAME)
                .build());
    }

    @Test
    public void putThenGetItemUsingKey() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));

        Record result = mappedTable.getItem(r -> r.key(k -> k.partitionValue("id-value").sortValue("sort-value")));

        Assert.assertEquals(record, result);
    }

    @Test
    public void putThenGetItemUsingKeyItem() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));

        Record keyItem = new Record();
        keyItem.setId("id-value");
        keyItem.setSort("sort-value");

        Record result = mappedTable.getItem(keyItem);

        Assert.assertEquals(record, result);
    }

    @Test
    public void getNonExistentItem() {
        Record result = mappedTable.getItem(r -> r.key(k -> k.partitionValue("id-value").sortValue("sort-value")));
        Assert.assertNull(result);
    }

    @Test
    public void putTwiceThenGetItem() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        Record record2 = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("four")
                .setAttribute2("five")
                .setAttribute3("six");

        mappedTable.putItem(r -> r.item(record2));
        Record result = mappedTable.getItem(r -> r.key(k -> k.partitionValue("id-value").sortValue("sort-value")));

        Assert.assertEquals(record2, result);
    }

    @Test
    public void putThenDeleteItem_usingShortcutForm() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(record);
        Record beforeDeleteResult =
                mappedTable.deleteItem(Key.builder().partitionValue("id-value").sortValue("sort-value").build());
        Record afterDeleteResult =
                mappedTable.getItem(Key.builder().partitionValue("id-value").sortValue("sort-value").build());

        Assert.assertEquals(record, beforeDeleteResult);
        Assert.assertNull(afterDeleteResult);
    }

    @Test
    public void putThenDeleteItem_usingKeyItemForm() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(record);
        Record beforeDeleteResult =
                mappedTable.deleteItem(record);
        Record afterDeleteResult =
                mappedTable.getItem(Key.builder().partitionValue("id-value").sortValue("sort-value").build());

        Assert.assertEquals(record, beforeDeleteResult);
        Assert.assertNull(afterDeleteResult);
    }

    @Test
    public void putWithConditionThatSucceeds() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        record.setAttribute("four");

        Expression conditionExpression = Expression.builder()
                .expression("#key = :value OR #key1 = :value1")
                .putExpressionName("#key", "attribute")
                .putExpressionName("#key1", ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS)
                .putExpressionValue(":value", stringValue("wrong"))
                .putExpressionValue(":value1", stringValue("three"))
                .build();

        mappedTable.putItem(PutItemEnhancedRequest.builder(Record.class)
                .item(record)
                .conditionExpression(conditionExpression).build());

        Record result = mappedTable.getItem(r -> r.key(k -> k.partitionValue("id-value").sortValue("sort-value")));
        Assert.assertEquals(record, result);
    }

    @Test
    public void putWithConditionThatFails() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        record.setAttribute("four");

        Expression conditionExpression = Expression.builder()
                .expression("#key = :value OR #key1 = :value1")
                .putExpressionName("#key", "attribute")
                .putExpressionName("#key1", ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS)
                .putExpressionValue(":value", stringValue("wrong"))
                .putExpressionValue(":value1", stringValue("wrong"))
                .build();

        exception.expect(ConditionalCheckFailedException.class);
        mappedTable.putItem(PutItemEnhancedRequest.builder(Record.class)
                .item(record)
                .conditionExpression(conditionExpression).build());
    }

    @Test
    public void deleteNonExistentItem() {
        Record result = mappedTable.deleteItem(r -> r.key(k -> k.partitionValue("id-value").sortValue("sort-value")));
        Assert.assertNull(result);
    }

    @Test
    public void deleteWithConditionThatSucceeds() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));

        Expression conditionExpression = Expression.builder()
                .expression("#key = :value OR #key1 = :value1")
                .putExpressionName("#key", "attribute")
                .putExpressionName("#key1", ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS)
                .putExpressionValue(":value", stringValue("wrong"))
                .putExpressionValue(":value1", stringValue("three"))
                .build();

        Key key = mappedTable.keyFrom(record);
        mappedTable.deleteItem(DeleteItemEnhancedRequest.builder().key(key).conditionExpression(conditionExpression).build());

        Record result = mappedTable.getItem(r -> r.key(key));
        Assert.assertNull(result);
    }

    @Test
    public void deleteWithConditionThatFails() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));

        Expression conditionExpression = Expression.builder()
                .expression("#key = :value OR #key1 = :value1")
                .putExpressionName("#key", "attribute")
                .putExpressionName("#key1", ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS)
                .putExpressionValue(":value", stringValue("wrong"))
                .putExpressionValue(":value1", stringValue("wrong"))
                .build();

        exception.expect(ConditionalCheckFailedException.class);
        mappedTable.deleteItem(DeleteItemEnhancedRequest.builder().key(mappedTable.keyFrom(record))
                .conditionExpression(conditionExpression)
                .build());
    }

    @Test
    public void updateOverwriteCompleteRecord_usingShortcutForm() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(record);
        Record record2 = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("four")
                .setAttribute2("five")
                .setAttribute3("six");
        Record result = mappedTable.updateItem(record2);

        Assert.assertEquals(record2, result);
    }

    @Ignore("TODO: Phoenix UPDATE_EXPRESSION does not support insertion currently.")
    public void updateCreatePartialRecord() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one");

        Record result = mappedTable.updateItem(r -> r.item(record));

        Assert.assertEquals(record, result);
    }

    @Ignore("TODO: Phoenix UPDATE_EXPRESSION does not support insertion currently.")
    public void updateCreateKeyOnlyRecord() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value");

        Record result = mappedTable.updateItem(r -> r.item(record));
        Assert.assertEquals(record, result);
    }

    @Test
    public void updateOverwriteModelledNulls() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        Record record2 = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("four");
        Record result = mappedTable.updateItem(r -> r.item(record2));

        Assert.assertEquals(record2, result);
    }

    @Test
    public void updateCanIgnoreNullsAndDoPartialUpdate() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        Record record2 = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("four");
        Record result = mappedTable.updateItem(UpdateItemEnhancedRequest.builder(Record.class)
                .item(record2)
                .ignoreNulls(true)
                .build());

        Record expectedResult = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("four")
                .setAttribute2("two")
                .setAttribute3("three");
        Assert.assertEquals(expectedResult, result);
    }

    @Test
    public void updateShortRecordDoesPartialUpdate() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        ShortRecord record2 = new ShortRecord()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("four");
        ShortRecord shortResult = mappedShortTable.updateItem(r -> r.item(record2));
        Record result = mappedTable.getItem(r -> r.key(k -> k.partitionValue(record.getId()).sortValue(record.getSort())));

        Record expectedResult = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("four")
                .setAttribute2("two")
                .setAttribute3("three");
        Assert.assertEquals(expectedResult, result);
        Assert.assertEquals(record2, shortResult);
    }

    @Test
    public void updateKeyOnlyExistingRecordDoesNothing() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        Record updateRecord = new Record().setId("id-value").setSort("sort-value");

        Record result = mappedTable.updateItem(UpdateItemEnhancedRequest.builder(Record.class)
                .item(updateRecord)
                .ignoreNulls(true)
                .build());

        Assert.assertEquals(record, result);
    }

    @Test
    public void updateWithConditionThatSucceeds() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        record.setAttribute("four");

        Expression conditionExpression = Expression.builder()
                .expression("#key = :value OR #key1 = :value1")
                .putExpressionName("#key", "attribute")
                .putExpressionName("#key1", ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS)
                .putExpressionValue(":value", stringValue("wrong"))
                .putExpressionValue(":value1", stringValue("three"))
                .build();

        mappedTable.updateItem(UpdateItemEnhancedRequest.builder(Record.class)
                .item(record)
                .conditionExpression(conditionExpression)
                .build());

        Record result = mappedTable.getItem(r -> r.key(k -> k.partitionValue("id-value").sortValue("sort-value")));
        Assert.assertEquals(record, result);
    }

    @Test
    public void updateWithConditionThatFails() {
        Record record = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one")
                .setAttribute2("two")
                .setAttribute3("three");

        mappedTable.putItem(r -> r.item(record));
        record.setAttribute("four");

        Expression conditionExpression = Expression.builder()
                .expression("#key = :value OR #key1 = :value1")
                .putExpressionName("#key", "attribute")
                .putExpressionName("#key1", ATTRIBUTE_NAME_WITH_SPECIAL_CHARACTERS)
                .putExpressionValue(":value", stringValue("wrong"))
                .putExpressionValue(":value1", stringValue("wrong"))
                .build();

        exception.expect(ConditionalCheckFailedException.class);
        mappedTable.updateItem(UpdateItemEnhancedRequest.builder(Record.class)
                .item(record)
                .conditionExpression(conditionExpression)
                .build());
    }

    @Test
    public void getAShortRecordWithNewModelledFields() {
        ShortRecord shortRecord = new ShortRecord()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one");
        mappedShortTable.putItem(r -> r.item(shortRecord));
        Record expectedRecord = new Record()
                .setId("id-value")
                .setSort("sort-value")
                .setAttribute("one");

        Record result = mappedTable.getItem(r -> r.key(k -> k.partitionValue("id-value").sortValue("sort-value")));
        Assert.assertEquals(expectedRecord, result);
    }
}
