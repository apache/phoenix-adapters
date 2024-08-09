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

package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.IndexStatus;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.TableStatus;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;

public class TableDescriptorUtils {

  public static void updateTableDescriptorForIndexes(PTable table,
      TableDescription tableDescription, Set<AttributeDefinition> attributeDefinitionSet) {
      if (table.getIndexes() != null && !table.getIndexes().isEmpty()) {
          List<GlobalSecondaryIndexDescription> globalSecondaryIndexDescriptions =
              new ArrayList<>();
          List<LocalSecondaryIndexDescription> localSecondaryIndexDescriptions =
              new ArrayList<>();
          for (PTable index : table.getIndexes()) {
              List<PColumn> respPkColumns = index.getPKColumns();
              List<KeySchemaElement> respKeySchemaElements = new ArrayList<>();
              String hashKeyName = respPkColumns.get(0).getName().getString();
              hashKeyName = CommonServiceUtils.getKeyNameFromBsonValueFunc(hashKeyName);
              KeySchemaElement hashKeyElement = new KeySchemaElement(hashKeyName, KeyType.HASH);
              respKeySchemaElements.add(hashKeyElement);
              AttributeDefinition hashAttr = new AttributeDefinition(hashKeyName,
                  CommonServiceUtils.getScalarAttributeFromPDataType(
                      respPkColumns.get(0).getDataType()));
              if (!attributeDefinitionSet.contains(hashAttr)) {
                  tableDescription.getAttributeDefinitions().add(hashAttr);
                  attributeDefinitionSet.add(hashAttr);
              }
              if (respPkColumns.size() - table.getPKColumns().size() > 1 || (
                  respPkColumns.size() > 1 && respPkColumns.get(1).getName().getString()
                      .contains("BSON_VALUE"))) {
                  String sortKeyName = respPkColumns.get(1).getName().getString();
                  sortKeyName = CommonServiceUtils.getKeyNameFromBsonValueFunc(sortKeyName);
                  KeySchemaElement sortKeyElement =
                      new KeySchemaElement(sortKeyName, KeyType.RANGE);
                  respKeySchemaElements.add(sortKeyElement);
                  AttributeDefinition sortAttr = new AttributeDefinition(sortKeyName,
                      CommonServiceUtils.getScalarAttributeFromPDataType(
                          respPkColumns.get(1).getDataType()));
                  if (!attributeDefinitionSet.contains(sortAttr)) {
                      tableDescription.getAttributeDefinitions().add(sortAttr);
                      attributeDefinitionSet.add(sortAttr);
                  }
              }
              if (hashKeyName.equals(table.getPKColumns().get(0).getName().getString())) {
                  LocalSecondaryIndexDescription localSecondaryIndexDescription =
                      new LocalSecondaryIndexDescription();
                  localSecondaryIndexDescription.setIndexName(index.getTableName().getString());
                  localSecondaryIndexDescription.setKeySchema(respKeySchemaElements);
                  localSecondaryIndexDescriptions.add(localSecondaryIndexDescription);
              } else {
                  GlobalSecondaryIndexDescription globalSecondaryIndexDescription =
                      new GlobalSecondaryIndexDescription();
                  globalSecondaryIndexDescription.setIndexName(index.getTableName().getString());
                  globalSecondaryIndexDescription.setKeySchema(respKeySchemaElements);
                  globalSecondaryIndexDescription.setIndexStatus(IndexStatus.ACTIVE);
                  globalSecondaryIndexDescriptions.add(globalSecondaryIndexDescription);
              }
          }
          if (!globalSecondaryIndexDescriptions.isEmpty()) {
              tableDescription.setGlobalSecondaryIndexes(globalSecondaryIndexDescriptions);
          }
          if (!localSecondaryIndexDescriptions.isEmpty()) {
              tableDescription.setLocalSecondaryIndexes(localSecondaryIndexDescriptions);
          }
      }
  }

  public static TableDescription getTableDescription(String tableName, String connectionUrl) {
      Set<AttributeDefinition> attributeDefinitionSet = new HashSet<>();
      try (Connection connection = DriverManager.getConnection(connectionUrl)) {
          PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
          PTable table = phoenixConnection.getTable(
              new PTableKey(phoenixConnection.getTenantId(), tableName));
          TableDescription tableDescription = new TableDescription();
          tableDescription.setTableName(table.getTableName().getString());
          tableDescription.setTableStatus(TableStatus.ACTIVE);

          List<PColumn> respPkColumns = table.getPKColumns();
          List<KeySchemaElement> respKeySchemaElements = new ArrayList<>();
          KeySchemaElement hashKeyElement =
              new KeySchemaElement(respPkColumns.get(0).getName().getString(), KeyType.HASH);
          respKeySchemaElements.add(hashKeyElement);
          if (respPkColumns.size() == 2) {
              KeySchemaElement sortKeyElement =
                  new KeySchemaElement(respPkColumns.get(1).getName().getString(), KeyType.RANGE);
              respKeySchemaElements.add(sortKeyElement);
          }
          tableDescription.setKeySchema(respKeySchemaElements);

          List<AttributeDefinition> respAttributeDefs = new ArrayList<>();
          AttributeDefinition hashAttribute =
              new AttributeDefinition(respPkColumns.get(0).getName().getString(),
                  CommonServiceUtils.getScalarAttributeFromPDataType(
                      respPkColumns.get(0).getDataType()));
          respAttributeDefs.add(hashAttribute);
          attributeDefinitionSet.add(hashAttribute);
          if (respPkColumns.size() == 2) {
              AttributeDefinition sortAttribute =
                  new AttributeDefinition(respPkColumns.get(1).getName().getString(),
                      CommonServiceUtils.getScalarAttributeFromPDataType(
                          respPkColumns.get(1).getDataType()));
              respAttributeDefs.add(sortAttribute);
              attributeDefinitionSet.add(sortAttribute);
          }
          tableDescription.setAttributeDefinitions(respAttributeDefs);

          updateTableDescriptorForIndexes(table, tableDescription, attributeDefinitionSet);
          tableDescription.setCreationDateTime(new Date(table.getTimeStamp()));
          return tableDescription;
      } catch (SQLException e) {
          throw new RuntimeException(e);
      }
  }
}
