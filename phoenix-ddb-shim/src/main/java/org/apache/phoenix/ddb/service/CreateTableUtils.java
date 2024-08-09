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
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class CreateTableUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableUtils.class);

  public static CreateTableResult getCreateTableResponse(final String tableName,
      final String connectionUrl) {
      CreateTableResult result = new CreateTableResult();
      TableDescription
          tableDescription = TableDescriptorUtils.getTableDescription(tableName, connectionUrl);
      result.setTableDescription(tableDescription);
      return result;
  }

  public static void addIndexDDL(CreateTableRequest request, Set<String> pkCols,
      List<KeySchemaElement> keySchemaElements, List<AttributeDefinition> attributeDefinitions,
      List<String> indexDDLs, String indexName) {
      final StringBuilder indexOn = new StringBuilder();

      String indexHashKey = null;
      String indexSortKey = null;

      KeySchemaElement hashKey = keySchemaElements.get(0);
      if (pkCols.contains(hashKey.getAttributeName())) {
          indexOn.append(hashKey.getAttributeName());
          indexHashKey = hashKey.getAttributeName();
      } else {
          Optional<AttributeDefinition> hashKeyAttr =
                  attributeDefinitions.stream()
                          .filter(attributeDefinition -> hashKey.getAttributeName()
                                  .equals(attributeDefinition.getAttributeName()))
                          .findFirst();
          Preconditions.checkArgument(hashKeyAttr.isPresent(),
                  "Hash key attribute should be " +
                          "defined");
          AttributeDefinition hashKeyAttribute = hashKeyAttr.get();
          String hashKeyType = hashKeyAttribute.getAttributeType();
          switch (hashKeyType) {
              case "S":
                  indexOn.append("BSON_VALUE(COL,'")
                          .append(hashKey.getAttributeName())
                          .append("','VARCHAR')");
                  break;
              case "N":
                  indexOn.append("BSON_VALUE(COL,'")
                          .append(hashKey.getAttributeName())
                          .append("','DOUBLE')");
                  break;
              case "B":
                  indexOn.append("BSON_VALUE(COL,'")
                          .append(hashKey.getAttributeName())
                          .append("','VARBINARY_ENCODED')");
                  break;
              default:
                  throw new IllegalArgumentException(
                          "Attribute Type " + hashKeyType + " is not " +
                                  "correct type");
          }
          indexHashKey = "BSON_VALUE(COL,'" + hashKey.getAttributeName() + "','VARCHAR')";
      }

      if (keySchemaElements.size() == 2) {
          KeySchemaElement rangeKey = keySchemaElements.get(1);
          indexOn.append(",");
          if (pkCols.contains(rangeKey.getAttributeName())) {
              indexOn.append(rangeKey.getAttributeName());
              indexSortKey = rangeKey.getAttributeName();
          } else {
              Optional<AttributeDefinition> rangeKeyAttr =
                      attributeDefinitions.stream()
                              .filter(attributeDefinition -> rangeKey.getAttributeName()
                                      .equals(attributeDefinition.getAttributeName()))
                              .findFirst();
              Preconditions.checkArgument(rangeKeyAttr.isPresent(),
                      "Range key attribute should be " +
                              "defined");
              AttributeDefinition rangeKeyAttribute = rangeKeyAttr.get();
              String rangeKeyType = rangeKeyAttribute.getAttributeType();
              switch (rangeKeyType) {
                  case "S":
                      indexOn.append("BSON_VALUE(COL,'")
                          .append(rangeKey.getAttributeName())
                          .append("','VARCHAR')");
                      break;
                  case "N":
                      indexOn.append("BSON_VALUE(COL,'")
                          .append(rangeKey.getAttributeName())
                          .append("','DOUBLE')");
                      break;
                  case "B":
                      indexOn.append("BSON_VALUE(COL,'")
                          .append(rangeKey.getAttributeName())
                          .append("','VARBINARY_ENCODED')");
                      break;
                  default:
                      throw new IllegalArgumentException(
                              "Attribute Type " + rangeKeyType + " is not " +
                                      "correct type");
              }
              indexSortKey = "BSON_VALUE(COL,'" + rangeKey.getAttributeName() + "','VARCHAR')";
          }
      }

      indexDDLs.add(
              "CREATE INDEX " + indexName + " ON " + request.getTableName()
                      + " (" + indexOn + ") INCLUDE (COL) WHERE " + indexHashKey + " IS NOT " +
                      "NULL" + ((indexSortKey != null) ? " AND " + indexSortKey + " IS NOT " +
                      "NULL" : ""));
  }

  public static List<String> getIndexDDLs(CreateTableRequest request, Set<String> pkCols) {
      final List<String> indexDDLs = new ArrayList<>();
      List<AttributeDefinition> attributeDefinitions = request.getAttributeDefinitions();

      if (request.getGlobalSecondaryIndexes() != null) {
          for (GlobalSecondaryIndex globalSecondaryIndex : request.getGlobalSecondaryIndexes()) {
              final String indexName = globalSecondaryIndex.getIndexName();
              final List<KeySchemaElement> keySchemaElements =
                  globalSecondaryIndex.getKeySchema();
              addIndexDDL(request, pkCols, keySchemaElements,
                  attributeDefinitions, indexDDLs, indexName);
          }
      }

      if (request.getLocalSecondaryIndexes() != null) {
          for (LocalSecondaryIndex localSecondaryIndex : request.getLocalSecondaryIndexes()) {
              final String indexName = localSecondaryIndex.getIndexName();
              final List<KeySchemaElement> keySchemaElements = localSecondaryIndex.getKeySchema();
              addIndexDDL(request, pkCols, keySchemaElements,
                  attributeDefinitions, indexDDLs, indexName);
          }
      }
      return indexDDLs;
  }

  public static CreateTableResult createTable(final CreateTableRequest request,
      final String connectionUrl) {
      final String tableName = request.getTableName();
      List<KeySchemaElement> keySchemaElements = request.getKeySchema();
      List<AttributeDefinition> attributeDefinitions = request.getAttributeDefinitions();

      StringBuilder cols = new StringBuilder();
      StringBuilder pkCols = new StringBuilder();
      Set<String> pkColsSet = new HashSet<>();

      KeySchemaElement hashKey = keySchemaElements.get(0);
      cols.append(hashKey.getAttributeName()).append(" ");
      pkCols.append(hashKey.getAttributeName());

      Optional<AttributeDefinition> hashKeyAttr =
              attributeDefinitions.stream()
                      .filter(attributeDefinition -> hashKey.getAttributeName()
                              .equals(attributeDefinition.getAttributeName())).findFirst();
      Preconditions.checkArgument(hashKeyAttr.isPresent(), "Hash key attribute should be " +
              "defined");
      AttributeDefinition hashKeyAttribute = hashKeyAttr.get();
      String hashKeyType = hashKeyAttribute.getAttributeType();
      pkColsSet.add(hashKey.getAttributeName());
      switch (hashKeyType) {
          case "S":
              cols.append("VARCHAR");
              break;
          case "N":
              cols.append("DOUBLE");
              break;
          case "B":
              cols.append("VARBINARY_ENCODED");
              break;
          default:
              throw new IllegalArgumentException("Attribute Type " + hashKeyType + " is not " +
                      "correct type");
      }

      if (keySchemaElements.size() == 2) {
          cols.append(", ");
          KeySchemaElement rangeKey = keySchemaElements.get(1);
          cols.append(rangeKey.getAttributeName()).append(" ");
          pkCols.append(",").append(rangeKey.getAttributeName());

          Optional<AttributeDefinition> rangeKeyAttr =
                  attributeDefinitions.stream()
                          .filter(attributeDefinition -> rangeKey.getAttributeName()
                                  .equals(attributeDefinition.getAttributeName())).findFirst();
          Preconditions.checkArgument(rangeKeyAttr.isPresent(), "Range key attribute should be "
                  + "defined");
          AttributeDefinition rangeKeyAttribute = rangeKeyAttr.get();
          String rangeKeyType = rangeKeyAttribute.getAttributeType();
          pkColsSet.add(rangeKey.getAttributeName());
          switch (rangeKeyType) {
              case "S":
                  cols.append("VARCHAR");
                  break;
              case "N":
                  cols.append("DOUBLE");
                  break;
              case "B":
                  cols.append("VARBINARY_ENCODED");
                  break;
              default:
                  throw new IllegalArgumentException("Attribute Type " + rangeKeyType + " is " +
                          "not correct type");
          }
      }
      cols.append(", COL BSON CONSTRAINT pk PRIMARY KEY (")
              .append(pkCols)
              .append(")");

      String createTableDDL = "CREATE TABLE " + tableName + " (" + cols + ")";
      LOGGER.info("Create Table Query: " + createTableDDL);

      List<String> createIndexDDLs = getIndexDDLs(request, pkColsSet);
      for (String createIndexDDL : createIndexDDLs) {
          LOGGER.info("Create Index Query: " + createIndexDDL);
      }

      try (Connection connection = DriverManager.getConnection(connectionUrl)) {
          connection.createStatement().execute(createTableDDL);
          for (String createIndexDDL : createIndexDDLs) {
              connection.createStatement().execute(createIndexDDL);
          }
      } catch (SQLException e) {
          throw new RuntimeException(e);
      }

      return getCreateTableResponse(request.getTableName(), connectionUrl);
  }
}
