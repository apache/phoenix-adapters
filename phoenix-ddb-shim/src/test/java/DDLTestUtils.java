import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
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
        CreateTableRequest createTableRequest = new CreateTableRequest();
        createTableRequest.setTableName(tableName);
        List<KeySchemaElement> keySchemaElements = new ArrayList<>();
        keySchemaElements.add(new KeySchemaElement(hashKey, KeyType.HASH));
        createTableRequest.withAttributeDefinitions(
                new AttributeDefinition(hashKey, hashKeyType));
        if (sortKey != null && sortKeyType != null) {
            keySchemaElements.add(new KeySchemaElement(sortKey, KeyType.RANGE));
            createTableRequest.withAttributeDefinitions(
                    new AttributeDefinition(sortKey, sortKeyType));
        }
        createTableRequest.setKeySchema(keySchemaElements);
        createTableRequest.setProvisionedThroughput(new ProvisionedThroughput(50L, 50L));
        return createTableRequest;
    }

    /**
     * Add index to the given CreateTableRequest with the given key schema.
     */
    public static void addIndexToRequest(boolean isGlobalIndex, CreateTableRequest request, String indexName, String hashKey,
               ScalarAttributeType hashKeyType, String sortKey, ScalarAttributeType sortKeyType) {
        List<KeySchemaElement> idxKeySchemaElements = new ArrayList<>();
        idxKeySchemaElements.add(new KeySchemaElement(hashKey, KeyType.HASH));
        // for local index, hash key will be the same as the table
        // and would already be present in the request attributes
        if (isGlobalIndex) {
            request.withAttributeDefinitions(new AttributeDefinition(hashKey, hashKeyType));
        }
        if (sortKey != null && sortKeyType != null) {
            idxKeySchemaElements.add(new KeySchemaElement(sortKey, KeyType.RANGE));
            request.withAttributeDefinitions(new AttributeDefinition(sortKey, sortKeyType));
        }
        if (isGlobalIndex) {
            addGlobalIndexToRequest(request, indexName, idxKeySchemaElements);
        } else {
            addLocalIndexToRequest(request, indexName, idxKeySchemaElements);
        }
    }

    /**
     * Add StreamSpecification to the given CreateTableRequest with the given stream type.
     */
    public static void addStreamSpecToRequest(CreateTableRequest request, String streamType) {
        StreamSpecification streamSpec = new StreamSpecification();
        streamSpec.setStreamEnabled(true);
        streamSpec.setStreamViewType(streamType);
        request.setStreamSpecification(streamSpec);
    }

    /**
     * Verify table name, key schema, attributes and indexes are same in the provided TableDescriptions
     * @throws Exception
     */
    public static void assertTableDescriptions(TableDescription tableDescription1,
                                         TableDescription tableDescription2) throws Exception {
        // table name
        Assert.assertEquals(tableDescription1.getTableName(), tableDescription2.getTableName());
        Assert.assertEquals(tableDescription1.getTableStatus(), tableDescription2.getTableStatus());


        // key schema
        List<KeySchemaElement> keySchemaElements1 = tableDescription1.getKeySchema();
        List<KeySchemaElement> keySchemaElements2 = tableDescription2.getKeySchema();

        Assert.assertEquals(keySchemaElements1.size(), keySchemaElements2.size());
        for (int i = 0; i < keySchemaElements1.size(); i++) {
            Assert.assertEquals(keySchemaElements1.get(i).getAttributeName(),
                    keySchemaElements2.get(i).getAttributeName());
            Assert.assertEquals(keySchemaElements1.get(i).getKeyType(),
                    keySchemaElements2.get(i).getKeyType());
        }

        // attribute definitions
        List<AttributeDefinition> attributeDefinitions1 =
                tableDescription1.getAttributeDefinitions();
        List<AttributeDefinition> attributeDefinitions2 =
                tableDescription2.getAttributeDefinitions();

        Assert.assertEquals(attributeDefinitions1.size(), attributeDefinitions2.size());
        for (int i = 0; i < attributeDefinitions1.size(); i++) {
            Assert.assertEquals(attributeDefinitions1.get(i).getAttributeName(),
                    attributeDefinitions2.get(i).getAttributeName());
            Assert.assertEquals(attributeDefinitions1.get(i).getAttributeType(),
                    attributeDefinitions2.get(i).getAttributeType());
        }

        // global indexes
        List<GlobalSecondaryIndexDescription> indexDescriptions1 =
                tableDescription1.getGlobalSecondaryIndexes();
        List<GlobalSecondaryIndexDescription> indexDescriptions2 =
                tableDescription2.getGlobalSecondaryIndexes();
        Assert.assertTrue((indexDescriptions1==null && indexDescriptions2==null)
                || (indexDescriptions1!=null && indexDescriptions2!=null));

        if (indexDescriptions1 != null) {
            Assert.assertEquals(indexDescriptions1.size(), indexDescriptions2.size());

            for (int i = 0; i < indexDescriptions1.size(); i++) {
                Assert.assertEquals(indexDescriptions1.get(i).getIndexName(),
                        indexDescriptions2.get(i).getIndexName());
                Assert.assertEquals(indexDescriptions1.get(i).getIndexStatus(),
                        indexDescriptions2.get(i).getIndexStatus());
                Assert.assertEquals(indexDescriptions1.get(i).getKeySchema().size(),
                        indexDescriptions2.get(i).getKeySchema().size());
                for (int j = 0; j < indexDescriptions1.get(i).getKeySchema().size(); j++) {
                    Assert.assertEquals(
                            indexDescriptions1.get(i).getKeySchema().get(j).getAttributeName(),
                            indexDescriptions2.get(i).getKeySchema().get(j).getAttributeName());
                    Assert.assertEquals(
                            indexDescriptions1.get(i).getKeySchema().get(j).getKeyType(),
                            indexDescriptions2.get(i).getKeySchema().get(j).getKeyType());
                }
            }
        }

        // local indexes
        List<LocalSecondaryIndexDescription> localIndexDescriptions1 =
                tableDescription1.getLocalSecondaryIndexes();
        List<LocalSecondaryIndexDescription> localIndexDescriptions2 =
                tableDescription2.getLocalSecondaryIndexes();
        Assert.assertTrue((localIndexDescriptions1==null && localIndexDescriptions2==null)
                || (localIndexDescriptions1!=null && localIndexDescriptions2!=null));


        if (localIndexDescriptions1 != null) {
            Assert.assertEquals(localIndexDescriptions1.size(), localIndexDescriptions2.size());

            for (int i = 0; i < localIndexDescriptions1.size(); i++) {
                Assert.assertEquals(localIndexDescriptions1.get(i).getIndexName(),
                        localIndexDescriptions2.get(i).getIndexName());
                Assert.assertEquals(localIndexDescriptions1.get(i).getKeySchema().size(),
                        localIndexDescriptions2.get(i).getKeySchema().size());
                for (int j = 0; j < localIndexDescriptions1.get(i).getKeySchema().size(); j++) {
                    Assert.assertEquals(
                            localIndexDescriptions1.get(i).getKeySchema().get(j).getAttributeName(),
                            localIndexDescriptions2.get(i).getKeySchema().get(j).getAttributeName());
                    Assert.assertEquals(
                            localIndexDescriptions1.get(i).getKeySchema().get(j).getKeyType(),
                            localIndexDescriptions2.get(i).getKeySchema().get(j).getKeyType());
                }
            }
        }

        Thread.sleep(1000);

        // creation date
        Assert.assertTrue("DDB table should have been created before Phoenix table",
                tableDescription1.getCreationDateTime()
                        .before(tableDescription2.getCreationDateTime()));
        Assert.assertTrue(
                tableDescription2.getCreationDateTime()
                        .before(new Date(System.currentTimeMillis())));

        Assert.assertNull(tableDescription2.getTableArn());

        // stream spec
        StreamSpecification streamSpec1 = tableDescription1.getStreamSpecification();
        StreamSpecification streamSpec2 = tableDescription2.getStreamSpecification();
        Assert.assertEquals(streamSpec1, streamSpec2);
    }

    /**
     * Verify CDC metadata in TableDescription returned from Phoenix client.
     * CDC index, SCHEMA_VERSION column in PTable and timestamp.
     */
    public static void assertCDCMetadata(PhoenixConnection pconn, TableDescription td,
                                         String streamType)
            throws SQLException, ParseException {
        String tableName = td.getTableName();
        Assert.assertTrue(td.getLatestStreamArn().startsWith("phoenix-cdc-stream-"));
        Assert.assertTrue(td.getLatestStreamArn().contains(tableName));

        PTable dataTable = pconn.getTable(tableName);
        Assert.assertEquals(streamType, dataTable.getSchemaVersion());
        boolean cdcIndexPresent = false;
        PTable cdcIndex = null;
        for (PTable index : dataTable.getIndexes()) {
            if (CDCUtil.isCDCIndex(index.getName().getString())) {
                cdcIndexPresent = true;
                cdcIndex = index;
            }
        }
        Assert.assertTrue(cdcIndexPresent);
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS zzz");
        Date date = df.parse(td.getLatestStreamLabel());
        Assert.assertEquals(String.valueOf(cdcIndex.getTimeStamp()), String.valueOf(date.getTime()));
        Assert.assertTrue(td.getLatestStreamArn().contains(String.valueOf(cdcIndex.getTimeStamp())));
    }

    private static void addGlobalIndexToRequest(CreateTableRequest request, String indexName,
                                                List<KeySchemaElement> keySchemaElements) {
        List<GlobalSecondaryIndex> indexes = request.getGlobalSecondaryIndexes();
        if (indexes == null) {
            indexes = new ArrayList();
        }
        indexes.add(
                new GlobalSecondaryIndex().withIndexName(indexName).withKeySchema()
                        .withKeySchema(keySchemaElements)
                        .withProvisionedThroughput(new ProvisionedThroughput(50L, 50L))
                        .withProjection(new Projection().withProjectionType(
                                ProjectionType.ALL)));
        request.setGlobalSecondaryIndexes(indexes);
    }

    private static void addLocalIndexToRequest(CreateTableRequest request, String indexName,
                                               List<KeySchemaElement> keySchemaElements) {
        List<LocalSecondaryIndex> indexes = request.getLocalSecondaryIndexes();
        if (indexes == null) {
            indexes = new ArrayList();
        }
        indexes.add(
                new LocalSecondaryIndex().withIndexName(indexName).withKeySchema()
                        .withKeySchema(keySchemaElements)
                        .withProjection(new Projection().withProjectionType(
                                ProjectionType.ALL)));
        request.setLocalSecondaryIndexes(indexes);
    }
}
