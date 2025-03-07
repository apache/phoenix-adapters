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

package org.apache.phoenix.ddb.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.IndexStatus;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndexDescription;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import software.amazon.awssdk.services.dynamodb.model.TableStatus;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.CDCUtil;

/**
 * Utilities to build TableDescriptor object for the given Phoenix table object.
 */
public class TableDescriptorUtils {

  public static TableDescription.Builder updateTableDescriptorForIndexes(PTable table,
      TableDescription.Builder tableDescription, Set<AttributeDefinition> attributeDefinitionSet) {
      if (table.getIndexes() != null && !table.getIndexes().isEmpty()) {
          List<GlobalSecondaryIndexDescription> globalSecondaryIndexDescriptions =
              new ArrayList<>();
          List<LocalSecondaryIndexDescription> localSecondaryIndexDescriptions =
              new ArrayList<>();
          for (PTable index : table.getIndexes()) {
              // skip the CDC index when building table descriptor
              if (CDCUtil.isCDCIndex(index.getName().getString())) continue;

              List<PColumn> respPkColumns = index.getPKColumns();
              List<KeySchemaElement> respKeySchemaElements = new ArrayList<>();
              String hashKeyName = respPkColumns.get(0).getName().getString();
              hashKeyName = CommonServiceUtils.getKeyNameFromBsonValueFunc(hashKeyName);
              KeySchemaElement hashKeyElement
                      = KeySchemaElement.builder().attributeName(hashKeyName).keyType(KeyType.HASH).build();
              respKeySchemaElements.add(hashKeyElement);
              AttributeDefinition hashAttr = AttributeDefinition.builder().attributeName(hashKeyName)
                      .attributeType(CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(0).getDataType()))
                      .build();
              if (!attributeDefinitionSet.contains(hashAttr)) {
                  attributeDefinitionSet.add(hashAttr);
                  tableDescription.attributeDefinitions(attributeDefinitionSet);
              }
              if (respPkColumns.size() - table.getPKColumns().size() > 1 || (
                  respPkColumns.size() > 1 && respPkColumns.get(1).getName().getString()
                      .contains("BSON_VALUE"))) {
                  String sortKeyName = respPkColumns.get(1).getName().getString();
                  sortKeyName = CommonServiceUtils.getKeyNameFromBsonValueFunc(sortKeyName);
                  KeySchemaElement sortKeyElement
                          = KeySchemaElement.builder().attributeName(sortKeyName).keyType(KeyType.RANGE).build();
                  respKeySchemaElements.add(sortKeyElement);
                  AttributeDefinition sortAttr = AttributeDefinition.builder().attributeName(sortKeyName)
                          .attributeType(CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(1).getDataType()))
                          .build();
                  if (!attributeDefinitionSet.contains(sortAttr)) {
                      attributeDefinitionSet.add(sortAttr);
                      tableDescription.attributeDefinitions(attributeDefinitionSet);
                  }
              }
              if (hashKeyName.equals(table.getPKColumns().get(0).getName().getString())) {
                  LocalSecondaryIndexDescription localSecondaryIndexDescription
                          = LocalSecondaryIndexDescription.builder()
                          .indexName(index.getTableName().getString())
                          .keySchema(respKeySchemaElements).build();
                  localSecondaryIndexDescriptions.add(localSecondaryIndexDescription);
              } else {
                  GlobalSecondaryIndexDescription globalSecondaryIndexDescription
                          = GlobalSecondaryIndexDescription.builder()
                          .indexName(index.getTableName().getString()).keySchema(respKeySchemaElements)
                          .indexStatus(IndexStatus.ACTIVE).build();
                  globalSecondaryIndexDescriptions.add(globalSecondaryIndexDescription);
              }
          }
          if (!globalSecondaryIndexDescriptions.isEmpty()) {
              tableDescription = tableDescription.globalSecondaryIndexes(globalSecondaryIndexDescriptions);
          }
          if (!localSecondaryIndexDescriptions.isEmpty()) {
              tableDescription = tableDescription.localSecondaryIndexes(localSecondaryIndexDescriptions);
          }
      }
      return tableDescription;
  }

  public static TableDescription getTableDescription(String tableName, String connectionUrl) {
      Set<AttributeDefinition> attributeDefinitionSet = new LinkedHashSet<>();
      try (Connection connection = DriverManager.getConnection(connectionUrl)) {
          PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
          PTable table = phoenixConnection.getTable(
              new PTableKey(phoenixConnection.getTenantId(), tableName));
          TableDescription.Builder tableDescription = TableDescription.builder();
          tableDescription = tableDescription.tableName(table.getTableName().getString());
          tableDescription = tableDescription.tableStatus(TableStatus.ACTIVE);

          List<PColumn> respPkColumns = table.getPKColumns();
          List<KeySchemaElement> respKeySchemaElements = new ArrayList<>();
          KeySchemaElement hashKeyElement = KeySchemaElement.builder()
                      .attributeName(respPkColumns.get(0).getName().getString())
                      .keyType(KeyType.HASH)
                      .build();
          respKeySchemaElements.add(hashKeyElement);
          if (respPkColumns.size() == 2) {
              KeySchemaElement sortKeyElement =
                      KeySchemaElement.builder()
                              .attributeName(respPkColumns.get(1).getName().getString())
                              .keyType(KeyType.RANGE)
                              .build();
              respKeySchemaElements.add(sortKeyElement);
          }
          tableDescription = tableDescription.keySchema(respKeySchemaElements);

          List<AttributeDefinition> respAttributeDefs = new ArrayList<>();
          AttributeDefinition hashAttribute =
                  AttributeDefinition.builder()
                          .attributeName(respPkColumns.get(0).getName().getString())
                          .attributeType(CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(0).getDataType()))
                          .build();
          respAttributeDefs.add(hashAttribute);
          attributeDefinitionSet.add(hashAttribute);
          if (respPkColumns.size() == 2) {
              AttributeDefinition sortAttribute =
                      AttributeDefinition.builder()
                              .attributeName(respPkColumns.get(1).getName().getString())
                              .attributeType(CommonServiceUtils.getScalarAttributeFromPDataType(respPkColumns.get(1).getDataType()))
                              .build();
              respAttributeDefs.add(sortAttribute);
              attributeDefinitionSet.add(sortAttribute);
          }
          tableDescription = tableDescription.attributeDefinitions(respAttributeDefs);

          tableDescription = updateTableDescriptorForIndexes(table, tableDescription, attributeDefinitionSet);
          tableDescription = updateStreamSpecification(table, tableDescription, phoenixConnection);
          tableDescription = tableDescription.creationDateTime(new Date(table.getTimeStamp()).toInstant());
          return tableDescription.build();
      } catch (SQLException e) {
          throw new RuntimeException(e);
      }
  }

    /**
     * If stream is enabled on this table i.e. SCHEMA_VERSION is set to some Stream Type,
     * populate the StreamSpecification, LatestStreamArn and LatestStreamLabel in the
     * TableDescription
     */
  private static TableDescription.Builder updateStreamSpecification(PTable table, TableDescription.Builder tableDescription,
                                                PhoenixConnection pconn) throws SQLException {
      String streamName =  DDBShimCDCUtils.getEnabledStreamName(pconn, table.getName().getString());
      if (streamName != null && table.getSchemaVersion() != null) {
          long creationTS = DDBShimCDCUtils.getCDCIndexTimestampFromStreamName(streamName);
          StreamSpecification streamSpec = StreamSpecification.builder()
                  .streamEnabled(true)
                  .streamViewType(table.getSchemaVersion())
                  .build();
          tableDescription = tableDescription.latestStreamArn(streamName)
                  .latestStreamLabel(DDBShimCDCUtils.getStreamLabel(creationTS))
                  .streamSpecification(streamSpec);
      }
      return tableDescription;
  }
}
