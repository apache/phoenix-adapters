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
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.DDBShimCDCUtils;
import org.apache.phoenix.schema.PIndexState;

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
                                                       Set<Map<String, Object>> attributeDefinitionSet) {
        if (table.getIndexes() != null && !table.getIndexes().isEmpty()) {
            String tableName = table.getTableName().getString();
            for (PTable index : table.getIndexes()) {
                String indexName = index.getName().getString();
                // skip the CDC index when building table descriptor
                if (CDCUtil.isCDCIndex(PhoenixUtils.getTableNameFromFullName(indexName, false))) {
                    continue;
                }

                List<PColumn> respPkColumns = index.getPKColumns();
                List<Map<String, Object>> keySchemaList = new ArrayList<>();

                String hashKeyName = respPkColumns.get(0).getName().getString();
                hashKeyName = CommonServiceUtils.getKeyNameFromBsonValueFunc(hashKeyName);
                Map<String, Object> hashKeyElement = new HashMap<>();
                hashKeyElement.put(ApiMetadata.ATTRIBUTE_NAME, hashKeyName);
                hashKeyElement.put(ApiMetadata.KEY_TYPE, "HASH");
                keySchemaList.add(hashKeyElement);

                Map<String, Object> hashAttr = getAttributeDefinitionMap(hashKeyName,
                        CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(0).getDataType()).toString());
                if (!attributeDefinitionSet.contains(hashAttr)) {
                    attributeDefinitionSet.add(hashAttr);
                    List<Map<String, Object>> attributeDefinitionsList =
                            (List<Map<String, Object>>) tableDescription.get(ApiMetadata.ATTRIBUTE_DEFINITIONS);
                    attributeDefinitionsList.add(hashAttr);
                }
                if (respPkColumns.size() - table.getPKColumns().size() > 1 || (
                        respPkColumns.size() > 1 && respPkColumns.get(1).getName().getString()
                                .contains("BSON_VALUE"))) {
                    String sortKeyName = respPkColumns.get(1).getName().getString();
                    sortKeyName = CommonServiceUtils.getKeyNameFromBsonValueFunc(sortKeyName);

                    Map<String, Object> sortKeyElement = new HashMap<>();
                    sortKeyElement.put(ApiMetadata.ATTRIBUTE_NAME, sortKeyName);
                    sortKeyElement.put(ApiMetadata.KEY_TYPE, "RANGE");
                    keySchemaList.add(sortKeyElement);

                    Map<String, Object> sortAttr =  getAttributeDefinitionMap(sortKeyName,
                            CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(1).getDataType()).toString());

                    if (!attributeDefinitionSet.contains(sortAttr)) {
                        attributeDefinitionSet.add(sortAttr);
                        List<Map<String, Object>> attributeDefinitionsList =
                                (List<Map<String, Object>>) tableDescription.get(ApiMetadata.ATTRIBUTE_DEFINITIONS);
                        attributeDefinitionsList.add(sortAttr);
                    }
                }
                if (hashKeyName.equals(table.getPKColumns().get(0).getName().getString())) {
                    tableDescription.putIfAbsent(ApiMetadata.LOCAL_SECONDARY_INDEXES,
                            new ArrayList<Map<String, Object>>());
                    List<Map<String, Object>> localSecondaryIndexes =
                            (List<Map<String, Object>>) tableDescription.get(ApiMetadata.LOCAL_SECONDARY_INDEXES);

                    Map<String, Object> localSecondaryIndexElement = new HashMap<>();
                    localSecondaryIndexElement.put(ApiMetadata.INDEX_NAME,
                            index.getTableName().getString().split(tableName + "_")[1]);
                    localSecondaryIndexElement.put(ApiMetadata.KEY_SCHEMA, keySchemaList);
                    localSecondaryIndexElement.put(ApiMetadata.INDEX_STATUS, indexStateMap.get(index.getIndexState()));
                    localSecondaryIndexes.add(localSecondaryIndexElement);
                } else {
                    tableDescription.putIfAbsent(ApiMetadata.GLOBAL_SECONDARY_INDEXES,
                            new ArrayList<Map<String, Object>>());
                    List<Map<String, Object>> globalSecondaryIndexes =
                            (List<Map<String, Object>>) tableDescription.get(ApiMetadata.GLOBAL_SECONDARY_INDEXES);

                    Map<String, Object> globalSecondaryIndexElement = new HashMap<>();
                    globalSecondaryIndexElement.put(ApiMetadata.INDEX_NAME,
                            index.getTableName().getString().split(tableName + "_")[1]);
                    globalSecondaryIndexElement.put(ApiMetadata.KEY_SCHEMA, keySchemaList);
                    globalSecondaryIndexElement.put(ApiMetadata.INDEX_STATUS, indexStateMap.get(index.getIndexState()));
                    globalSecondaryIndexes.add(globalSecondaryIndexElement);
                }
            }
        }
    }

    public static Map<String, Object> getTableDescription(String tableName, String connectionUrl,
                                                          String topResponseAttribute) {
        Set<Map<String, Object>> attributeDefinitionSet = new LinkedHashSet<>();
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTableNoCache(phoenixConnection.getTenantId(),
                    PhoenixUtils.getFullTableName(tableName, false));

            Map<String, Object> tableDescriptionResponse = new HashMap<>();
            tableDescriptionResponse.put(topResponseAttribute, new HashMap<String, Object>());
            Map<String, Object> tableDescription =
                    (Map<String, Object>) tableDescriptionResponse.get(topResponseAttribute);
            tableDescription.put(ApiMetadata.TABLE_NAME, table.getTableName().getString());
            tableDescription.put(ApiMetadata.TABLE_STATUS, "ACTIVE");
            tableDescription.put(ApiMetadata.KEY_SCHEMA, getKeySchemaList(table.getPKColumns()));
            tableDescription.put(ApiMetadata.ATTRIBUTE_DEFINITIONS, getAttributeDefs(table, attributeDefinitionSet));
            tableDescription.put(ApiMetadata.CREATION_DATE_TIME, table.getTimeStamp()/1000);

            updateTableDescriptorForIndexes(table, tableDescription, attributeDefinitionSet);
            updateStreamSpecification(table, tableDescription, phoenixConnection);

            return tableDescriptionResponse;
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
    }

    private static List<Map<String, Object>> getAttributeDefs(PTable table,
                                                              Set<Map<String, Object>> attributeDefinitionSet) {
        List<Map<String, Object>> response = new ArrayList<>();
        List<PColumn> respPkColumns = table.getPKColumns();

        Map<String, Object> hashAttributeMap = getAttributeDefinitionMap(
                respPkColumns.get(0).getName().getString(),
                CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(0).getDataType()).toString());
        response.add(hashAttributeMap);
        attributeDefinitionSet.add(hashAttributeMap);

        if (respPkColumns.size() == 2) {
            Map<String, Object> sortAttributeMap = getAttributeDefinitionMap(
                    respPkColumns.get(1).getName().getString(),
                    CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(1).getDataType()).toString());
            response.add(sortAttributeMap);
            attributeDefinitionSet.add(sortAttributeMap);
        }
        return response;
    }

    private static List<Map<String, Object>> getKeySchemaList(List<PColumn> pkColumns) {
        List<Map<String, Object>> response = new ArrayList<>();
        Map<String, Object> hashKeySchema = new HashMap<>();
        hashKeySchema.put("AttributeName", pkColumns.get(0).getName().getString());
        hashKeySchema.put("KeyType", "HASH");
        response.add(hashKeySchema);
        if (pkColumns.size() == 2) {
            Map<String, Object> sortKeySchema = new HashMap<>();
            sortKeySchema.put("AttributeName", pkColumns.get(1).getName().getString());
            sortKeySchema.put("KeyType", "RANGE");
            response.add(sortKeySchema);
        }
        return response;
    }

    private static Map<String, Object> getAttributeDefinitionMap(String name, String type) {
        Map<String, Object> attrDef = new HashMap<>();
        attrDef.put(ApiMetadata.ATTRIBUTE_NAME, name);
        attrDef.put(ApiMetadata.ATTRIBUTE_TYPE, type);
        return attrDef;
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
            tableDescription.put(ApiMetadata.LATEST_STREAM_ARN, streamName);
            tableDescription.put(ApiMetadata.LATEST_STREAM_LABEL, DDBShimCDCUtils.getStreamLabel(streamName));

            Map<String, Object> streamSpecification = new HashMap<>();
            streamSpecification.put(ApiMetadata.STREAM_ENABLED, true);
            streamSpecification.put(ApiMetadata.STREAM_VIEW_TYPE, table.getSchemaVersion());

            tableDescription.put(ApiMetadata.STREAM_SPECIFICATION, streamSpecification);
        }
    }
}
