/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.service.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.schema.PIndexState;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.util.CDCUtil;

/**
 * Utilities to build TableDescriptor object for the given Phoenix table object.
 */
public class TableDescriptorUtils {

    private static final Map<PIndexState, String> indexStateMap = new HashMap<PIndexState, String>()
    {{
        put(PIndexState.CREATE_DISABLE, "CREATING");
        put(PIndexState.DISABLE, "DELETING");
        put(PIndexState.ACTIVE, "ACTIVE");
        put(PIndexState.BUILDING, "CREATING");
    }};

    public static void updateTableDescriptorForIndexes(PTable table,
                                                       Map<String, Object> tableDescription,
                                                       Set<AttributeDefinition> attributeDefinitionSet) {
        if (table.getIndexes() != null && !table.getIndexes().isEmpty()) {
            for (PTable index : table.getIndexes()) {
                String indexName = index.getName().getString();
                // skip the CDC index when building table descriptor
                if (indexName.startsWith("DDB.") && CDCUtil.isCDCIndex(
                        indexName.split("DDB.")[1])) {
                    continue;
                }

                List<PColumn> respPkColumns = index.getPKColumns();
                List<Map<String, Object>> keySchemaList = new ArrayList<>();

                String hashKeyName = respPkColumns.get(0).getName().getString();
                hashKeyName = CommonServiceUtils.getKeyNameFromBsonValueFunc(hashKeyName);
                Map<String, Object> hashKeyElement = new HashMap<>();
                hashKeyElement.put("AttributeName", hashKeyName);
                hashKeyElement.put("KeyType", KeyType.HASH.toString());
                keySchemaList.add(hashKeyElement);

                AttributeDefinition hashAttr = AttributeDefinition.builder().attributeName(hashKeyName)
                        .attributeType(CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(0).getDataType()))
                        .build();
                if (!attributeDefinitionSet.contains(hashAttr)) {
                    attributeDefinitionSet.add(hashAttr);

                    List<Map<String, Object>> attributeDefinitionsList =
                            (List<Map<String, Object>>) tableDescription.get("AttributeDefinitions");

                    Map<String, Object> attributeDefinitionElement = new HashMap<>();
                    attributeDefinitionElement.put("AttributeName", hashAttr.attributeName());
                    attributeDefinitionElement.put("AttributeType",
                            hashAttr.attributeType().toString());

                    attributeDefinitionsList.add(attributeDefinitionElement);
                }
                if (respPkColumns.size() - table.getPKColumns().size() > 1 || (
                        respPkColumns.size() > 1 && respPkColumns.get(1).getName().getString()
                                .contains("BSON_VALUE"))) {
                    String sortKeyName = respPkColumns.get(1).getName().getString();
                    sortKeyName = CommonServiceUtils.getKeyNameFromBsonValueFunc(sortKeyName);

                    Map<String, Object> sortKeyElement = new HashMap<>();
                    sortKeyElement.put("AttributeName", sortKeyName);
                    sortKeyElement.put("KeyType", KeyType.RANGE.toString());
                    keySchemaList.add(sortKeyElement);

                    AttributeDefinition sortAttr = AttributeDefinition.builder().attributeName(sortKeyName)
                            .attributeType(CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(1).getDataType()))
                            .build();

                    if (!attributeDefinitionSet.contains(sortAttr)) {
                        attributeDefinitionSet.add(sortAttr);

                        List<Map<String, Object>> attributeDefinitionsList =
                                (List<Map<String, Object>>) tableDescription.get("AttributeDefinitions");

                        Map<String, Object> attributeDefinitionElement = new HashMap<>();
                        attributeDefinitionElement.put("AttributeName", sortAttr.attributeName());
                        attributeDefinitionElement.put("AttributeType",
                                sortAttr.attributeType().toString());

                        attributeDefinitionsList.add(attributeDefinitionElement);
                    }
                }
                if (hashKeyName.equals(table.getPKColumns().get(0).getName().getString())) {
                    tableDescription.putIfAbsent("LocalSecondaryIndexes",
                            new ArrayList<Map<String, Object>>());
                    List<Map<String, Object>> localSecondaryIndexes =
                            (List<Map<String, Object>>) tableDescription.get("LocalSecondaryIndexes");

                    Map<String, Object> localSecondaryIndexElement = new HashMap<>();
                    localSecondaryIndexElement.put("IndexName", index.getTableName().getString());
                    localSecondaryIndexElement.put("KeySchema", keySchemaList);
                    localSecondaryIndexElement.put("IndexStatus", indexStateMap.get(index.getIndexState()));
                    localSecondaryIndexes.add(localSecondaryIndexElement);
                } else {
                    tableDescription.putIfAbsent("GlobalSecondaryIndexes",
                            new ArrayList<Map<String, Object>>());
                    List<Map<String, Object>> globalSecondaryIndexes =
                            (List<Map<String, Object>>) tableDescription.get("GlobalSecondaryIndexes");

                    Map<String, Object> globalSecondaryIndexElement = new HashMap<>();
                    globalSecondaryIndexElement.put("IndexName", index.getTableName().getString());
                    globalSecondaryIndexElement.put("KeySchema", keySchemaList);
                    globalSecondaryIndexElement.put("IndexStatus", indexStateMap.get(index.getIndexState()));
                    globalSecondaryIndexes.add(globalSecondaryIndexElement);
                }
            }
        }
    }

    public static Map<String, Object> getTableDescription(String tableName, String connectionUrl,
                                                          String topResponseAttribute) {
        Set<AttributeDefinition> attributeDefinitionSet = new LinkedHashSet<>();
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTableNoCache(phoenixConnection.getTenantId(),
                    "DDB." + tableName);
            List<PColumn> respPkColumns = table.getPKColumns();

            Map<String, Object> tableDescriptionResponse = new HashMap<>();
            tableDescriptionResponse.put(topResponseAttribute, new HashMap<String, Object>());
            Map<String, Object> tableDescription =
                    (Map<String, Object>) tableDescriptionResponse.get(topResponseAttribute);
            tableDescription.put("TableName", table.getTableName().getString());
            tableDescription.put("TableStatus", TableStatus.ACTIVE.toString());
            tableDescription.put("KeySchema", getKeySchemaList(table.getPKColumns()));
            tableDescription.put("AttributeDefinitions", getAttributeDefs(table, attributeDefinitionSet));
            tableDescription.put("CreationDateTime", table.getTimeStamp()/1000);

            updateTableDescriptorForIndexes(table, tableDescription, attributeDefinitionSet);
            updateStreamSpecification(table, tableDescription, phoenixConnection);

            return tableDescriptionResponse;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Map<String, Object>> getAttributeDefs(PTable table,
                                                              Set<AttributeDefinition> attributeDefinitionSet) {
        List<Map<String, Object>> response = new ArrayList<>();
        List<PColumn> respPkColumns = table.getPKColumns();

        AttributeDefinition hashAttribute =
                AttributeDefinition.builder()
                        .attributeName(respPkColumns.get(0).getName().getString())
                        .attributeType(CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(0).getDataType()))
                        .build();
        Map<String, Object> hashAttributeMap = new HashMap<>();
        hashAttributeMap.put("AttributeName", respPkColumns.get(0).getName().getString());
        hashAttributeMap.put("AttributeType", CommonServiceUtils.getScalarAttributeFromPDataType(
                respPkColumns.get(0).getDataType()).toString());
        response.add(hashAttributeMap);
        attributeDefinitionSet.add(hashAttribute);

        if (respPkColumns.size() == 2) {
            AttributeDefinition sortAttribute =
                    AttributeDefinition.builder()
                            .attributeName(respPkColumns.get(1).getName().getString())
                            .attributeType(CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(1).getDataType()))
                            .build();
            Map<String, Object> sortAttributeMap = new HashMap<>();
            sortAttributeMap.put("AttributeName", respPkColumns.get(1).getName().getString());
            sortAttributeMap.put("AttributeType", CommonServiceUtils
                    .getScalarAttributeFromPDataType(respPkColumns.get(1).getDataType())
                    .toString());
            response.add(sortAttributeMap);
            attributeDefinitionSet.add(sortAttribute);
        }
        return response;
    }

    private static List<Map<String, Object>> getKeySchemaList(List<PColumn> pkColumns) {
        List<Map<String, Object>> response = new ArrayList<>();
        Map<String, Object> hashKeySchema = new HashMap<>();
        hashKeySchema.put("AttributeName", pkColumns.get(0).getName().getString());
        hashKeySchema.put("KeyType", KeyType.HASH.toString());
        response.add(hashKeySchema);
        if (pkColumns.size() == 2) {
            Map<String, Object> sortKeySchema = new HashMap<>();
            sortKeySchema.put("AttributeName", pkColumns.get(1).getName().getString());
            sortKeySchema.put("KeyType", KeyType.RANGE.toString());
            response.add(sortKeySchema);
        }
        return response;
    }

    /**
     * If stream is enabled on this table i.e. SCHEMA_VERSION is set to some Stream Type,
     * populate the StreamSpecification, LatestStreamArn and LatestStreamLabel in the
     * TableDescription
     */
    private static void updateStreamSpecification(PTable table,
                                                  Map<String, Object> tableDescription,
                                                  PhoenixConnection pconn) throws SQLException {
        String streamName = DDBShimCDCUtils.getEnabledStreamName(pconn,
                table.getName().getString());
        if (streamName != null && table.getSchemaVersion() != null) {
            long creationTS = DDBShimCDCUtils.getCDCIndexTimestampFromStreamName(streamName);

            tableDescription.put("LatestStreamArn", streamName);
            tableDescription.put("LatestStreamLabel", DDBShimCDCUtils.getStreamLabel(creationTS));

            Map<String, Object> streamSpecification = new HashMap<>();
            streamSpecification.put("StreamEnabled", true);
            streamSpecification.put("StreamViewType", table.getSchemaVersion());

            tableDescription.put("StreamSpecification", streamSpecification);
        }
    }
}
