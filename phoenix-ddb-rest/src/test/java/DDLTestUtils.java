import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.junit.Assert;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.Projection;
import software.amazon.awssdk.services.dynamodb.model.ProjectionType;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughput;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Utility functions for DDL API tests.
 */
public class DDLTestUtils {

    /**
     * Return a CreateTableRequest with the given table name and key schema.
     * @return
     */
    public static CreateTableRequest getCreateTableRequest(String tableName, String hashKey,
               ScalarAttributeType hashKeyType, String sortKey, ScalarAttributeType sortKeyType) {
        CreateTableRequest.Builder createTableRequest = CreateTableRequest.builder();
        createTableRequest.tableName(tableName);
        List<AttributeDefinition> attrDefs = new ArrayList<>();
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build());
        attrDefs.add(AttributeDefinition.builder().attributeName(hashKey).attributeType(hashKeyType).build());
        if (sortKey != null && sortKeyType != null) {
            keySchemaElements.add(KeySchemaElement.builder().attributeName(sortKey).keyType(KeyType.RANGE).build());
            attrDefs.add(AttributeDefinition.builder().attributeName(sortKey).attributeType(sortKeyType).build());
        }
        createTableRequest.keySchema(keySchemaElements);
        createTableRequest.attributeDefinitions(attrDefs);
        createTableRequest.provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(50L).writeCapacityUnits(50L).build());
        return createTableRequest.build();
    }

    /**
     * Add index to the given CreateTableRequest with the given key schema.
     */
    public static CreateTableRequest addIndexToRequest(boolean isGlobalIndex, CreateTableRequest request, String indexName, String hashKey,
               ScalarAttributeType hashKeyType, String sortKey, ScalarAttributeType sortKeyType) {
        CreateTableRequest.Builder requestBuilder = request.toBuilder();
        List<KeySchemaElement> idxKeySchemaElements = new ArrayList<>();
        List<AttributeDefinition> attrDefs = new ArrayList<>(request.attributeDefinitions());
        idxKeySchemaElements.add(KeySchemaElement.builder().attributeName(hashKey).keyType(KeyType.HASH).build());
        // for local index, hash key will be the same as the table
        // and would already be present in the request attributes
        if (isGlobalIndex) {
            attrDefs.add(AttributeDefinition.builder().attributeName(hashKey).attributeType(hashKeyType).build());
        }
        if (sortKey != null && sortKeyType != null) {
            idxKeySchemaElements.add(KeySchemaElement.builder().attributeName(sortKey).keyType(KeyType.RANGE).build());
            attrDefs.add(AttributeDefinition.builder().attributeName(sortKey).attributeType(sortKeyType).build());
        }
        if (isGlobalIndex) {
            requestBuilder = addGlobalIndexToRequest(requestBuilder, indexName, idxKeySchemaElements, request.globalSecondaryIndexes());
        } else {
            requestBuilder = addLocalIndexToRequest(requestBuilder, indexName, idxKeySchemaElements, request.localSecondaryIndexes());
        }
        return requestBuilder.attributeDefinitions(attrDefs).build();
    }

    /**
     * Add StreamSpecification to the given CreateTableRequest with the given stream type.
     */
    public static CreateTableRequest addStreamSpecToRequest(CreateTableRequest request, String streamType) {
        StreamSpecification streamSpec
                = StreamSpecification.builder().streamEnabled(true).streamViewType(streamType).build();
        return request.toBuilder().streamSpecification(streamSpec).build();
    }

    /**
     * Verify table name, key schema, attributes and indexes are same in the provided TableDescriptions
     * @throws Exception
     */
    public static void assertTableDescriptions(TableDescription tableDescription1,
                                         TableDescription tableDescription2) throws Exception {
        // table name
        Assert.assertEquals(tableDescription1.tableName(), tableDescription2.tableName());
        Assert.assertEquals(tableDescription1.tableStatus(), tableDescription2.tableStatus());


        // key schema
        List<KeySchemaElement> keySchemaElements1 = tableDescription1.keySchema();
        List<KeySchemaElement> keySchemaElements2 = tableDescription2.keySchema();

        Assert.assertEquals(keySchemaElements1.size(), keySchemaElements2.size());
        for (int i = 0; i < keySchemaElements1.size(); i++) {
            Assert.assertEquals(keySchemaElements1.get(i).attributeName(),
                    keySchemaElements2.get(i).attributeName());
            Assert.assertEquals(keySchemaElements1.get(i).keyType(),
                    keySchemaElements2.get(i).keyType());
        }

        // attribute definitions
        List<AttributeDefinition> attributeDefinitions1 =
                tableDescription1.attributeDefinitions();
        List<AttributeDefinition> attributeDefinitions2 =
                tableDescription2.attributeDefinitions();

        Assert.assertEquals(attributeDefinitions1.size(), attributeDefinitions2.size());
        for (int i = 0; i < attributeDefinitions1.size(); i++) {
            Assert.assertEquals(attributeDefinitions1.get(i).attributeName(),
                    attributeDefinitions2.get(i).attributeName());
            Assert.assertEquals(attributeDefinitions1.get(i).attributeType(),
                    attributeDefinitions2.get(i).attributeType());
        }

        // global indexes
        List<GlobalSecondaryIndexDescription> indexDescriptions1 =
                tableDescription1.globalSecondaryIndexes();
        List<GlobalSecondaryIndexDescription> indexDescriptions2 =
                tableDescription2.globalSecondaryIndexes();
        Assert.assertTrue((indexDescriptions1==null && indexDescriptions2==null)
                || (indexDescriptions1!=null && indexDescriptions2!=null));

        if (indexDescriptions1 != null) {
            Assert.assertEquals(indexDescriptions1.size(), indexDescriptions2.size());

            for (int i = 0; i < indexDescriptions1.size(); i++) {
                Assert.assertEquals(indexDescriptions1.get(i).indexName(),
                        indexDescriptions2.get(i).indexName());
                Assert.assertEquals(indexDescriptions1.get(i).indexStatus(),
                        indexDescriptions2.get(i).indexStatus());
                Assert.assertEquals(indexDescriptions1.get(i).keySchema().size(),
                        indexDescriptions2.get(i).keySchema().size());
                for (int j = 0; j < indexDescriptions1.get(i).keySchema().size(); j++) {
                    Assert.assertEquals(
                            indexDescriptions1.get(i).keySchema().get(j).attributeName(),
                            indexDescriptions2.get(i).keySchema().get(j).attributeName());
                    Assert.assertEquals(
                            indexDescriptions1.get(i).keySchema().get(j).keyType(),
                            indexDescriptions2.get(i).keySchema().get(j).keyType());
                }
            }
        }

        // local indexes
        List<LocalSecondaryIndexDescription> localIndexDescriptions1 =
                tableDescription1.localSecondaryIndexes();
        List<LocalSecondaryIndexDescription> localIndexDescriptions2 =
                tableDescription2.localSecondaryIndexes();
        Assert.assertTrue((localIndexDescriptions1==null && localIndexDescriptions2==null)
                || (localIndexDescriptions1!=null && localIndexDescriptions2!=null));


        if (localIndexDescriptions1 != null) {
            Assert.assertEquals(localIndexDescriptions1.size(), localIndexDescriptions2.size());

            for (int i = 0; i < localIndexDescriptions1.size(); i++) {
                Assert.assertEquals(localIndexDescriptions1.get(i).indexName(),
                        localIndexDescriptions2.get(i).indexName());
                Assert.assertEquals(localIndexDescriptions1.get(i).keySchema().size(),
                        localIndexDescriptions2.get(i).keySchema().size());
                for (int j = 0; j < localIndexDescriptions1.get(i).keySchema().size(); j++) {
                    Assert.assertEquals(
                            localIndexDescriptions1.get(i).keySchema().get(j).attributeName(),
                            localIndexDescriptions2.get(i).keySchema().get(j).attributeName());
                    Assert.assertEquals(
                            localIndexDescriptions1.get(i).keySchema().get(j).keyType(),
                            localIndexDescriptions2.get(i).keySchema().get(j).keyType());
                }
            }
        }

        Thread.sleep(1000);

        // creation date
        Assert.assertTrue("DDB table should have been created before Phoenix table",
                tableDescription1.creationDateTime()
                        .isBefore(tableDescription2.creationDateTime()));
        Assert.assertTrue(
                tableDescription2.creationDateTime()
                        .isBefore(new Date(System.currentTimeMillis()).toInstant()));

        Assert.assertNull(tableDescription2.tableArn());

        // stream spec
        StreamSpecification streamSpec1 = tableDescription1.streamSpecification();
        StreamSpecification streamSpec2 = tableDescription2.streamSpecification();
        Assert.assertEquals(streamSpec1, streamSpec2);
    }

    /**
     * Verify CDC metadata in TableDescription returned from Phoenix client.
     * CDC index, SCHEMA_VERSION column in PTable and timestamp.
     */
    public static void assertCDCMetadata(PhoenixConnection pconn, TableDescription td,
                                         String streamType)
            throws SQLException, ParseException {
        String tableName = td.tableName();
        Assert.assertTrue(td.latestStreamArn().startsWith("phoenix-cdc-stream-"));
        Assert.assertTrue(td.latestStreamArn().contains(tableName));

        PTable dataTable = pconn.getTable("DDB." + tableName);
        Assert.assertEquals(streamType, dataTable.getSchemaVersion());
        boolean cdcIndexPresent = false;
        PTable cdcIndex = null;
        for (PTable index : dataTable.getIndexes()) {
            if (CDCUtil.isCDCIndex(index.getName().getString().split("DDB.")[1])) {
                cdcIndexPresent = true;
                cdcIndex = index;
            }
        }
        Assert.assertTrue(cdcIndexPresent);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz");
        Date date = df.parse(td.latestStreamLabel());
        Assert.assertEquals(String.valueOf(cdcIndex.getTimeStamp()), String.valueOf(date.getTime()));
        Assert.assertTrue(td.latestStreamArn().contains(String.valueOf(cdcIndex.getTimeStamp())));
    }

    private static CreateTableRequest.Builder addGlobalIndexToRequest(CreateTableRequest.Builder request, String indexName,
                                                List<KeySchemaElement> keySchemaElements, List<GlobalSecondaryIndex> indexList) {
        List<GlobalSecondaryIndex> indexes = new ArrayList<>(indexList);
        indexes.add(GlobalSecondaryIndex.builder().indexName(indexName)
                        .keySchema(keySchemaElements)
                        .provisionedThroughput(ProvisionedThroughput.builder().readCapacityUnits(50L).writeCapacityUnits(50L).build())
                        .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).build());
        return request.globalSecondaryIndexes(indexes);
    }

    private static CreateTableRequest.Builder addLocalIndexToRequest(CreateTableRequest.Builder request, String indexName,
                                               List<KeySchemaElement> keySchemaElements, List<LocalSecondaryIndex> indexList) {
        List<LocalSecondaryIndex> indexes = new ArrayList<>(indexList);
        indexes.add(LocalSecondaryIndex.builder().indexName(indexName)
                .keySchema(keySchemaElements)
                .projection(Projection.builder().projectionType(ProjectionType.ALL).build()).build());
        return request.localSecondaryIndexes(indexes);
    }
}
