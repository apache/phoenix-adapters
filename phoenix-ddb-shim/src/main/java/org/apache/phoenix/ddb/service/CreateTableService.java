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

import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.CreateTableRequest;
import software.amazon.awssdk.services.dynamodb.model.CreateTableResponse;
import software.amazon.awssdk.services.dynamodb.model.GlobalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.LocalSecondaryIndex;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;
import software.amazon.awssdk.services.dynamodb.model.StreamSpecification;
import software.amazon.awssdk.services.dynamodb.model.StreamViewType;
import software.amazon.awssdk.services.dynamodb.model.TableDescription;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.TableDescriptorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;

public class CreateTableService {

  private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableService.class);

  private static final String CREATE_CDC_DDL = "CREATE CDC CDC_%s on %s";
  private static final String ALTER_TABLE_STREAM_TYPE_DDL = "ALTER TABLE %s set SCHEMA_VERSION = '%s'";

  public static CreateTableResponse getCreateTableResponse(final String tableName,
      final String connectionUrl) {
      TableDescription
          tableDescription = TableDescriptorUtils.getTableDescription(tableName, connectionUrl);
      return CreateTableResponse.builder().tableDescription(tableDescription).build();
  }

  public static void addIndexDDL(CreateTableRequest request, Set<String> pkCols,
      List<KeySchemaElement> keySchemaElements, List<AttributeDefinition> attributeDefinitions,
      List<String> indexDDLs, String indexName) {
      final StringBuilder indexOn = new StringBuilder();

      String indexHashKey = null;
      String indexSortKey = null;

      KeySchemaElement hashKey = keySchemaElements.get(0);
      Optional<AttributeDefinition> hashKeyAttr = attributeDefinitions.stream().filter(
          attributeDefinition -> hashKey.attributeName()
              .equals(attributeDefinition.attributeName())).findFirst();
      Preconditions.checkArgument(hashKeyAttr.isPresent(),
          "Hash key attribute should be " + "defined");
      AttributeDefinition hashKeyAttribute = hashKeyAttr.get();
      ScalarAttributeType hashKeyType = hashKeyAttribute.attributeType();
      switch (hashKeyType) {
      case S:
          indexOn.append("BSON_VALUE(COL,'").append(hashKey.attributeName())
              .append("','VARCHAR')");
          break;
      case N:
          indexOn.append("BSON_VALUE(COL,'").append(hashKey.attributeName())
              .append("','DOUBLE')");
          break;
      case B:
          indexOn.append("BSON_VALUE(COL,'").append(hashKey.attributeName())
              .append("','VARBINARY_ENCODED')");
          break;
      default:
          throw new IllegalArgumentException(
              "Attribute Type " + hashKeyType + " is not " + "correct type");
      }
      indexHashKey = "BSON_VALUE(COL,'" + hashKey.attributeName() + "','VARCHAR')";

      if (keySchemaElements.size() == 2) {
          KeySchemaElement rangeKey = keySchemaElements.get(1);
          indexOn.append(",");
          Optional<AttributeDefinition> rangeKeyAttr = attributeDefinitions.stream().filter(
              attributeDefinition -> rangeKey.attributeName()
                  .equals(attributeDefinition.attributeName())).findFirst();
          Preconditions.checkArgument(rangeKeyAttr.isPresent(),
              "Range key attribute should be " + "defined");
          AttributeDefinition rangeKeyAttribute = rangeKeyAttr.get();
          ScalarAttributeType rangeKeyType = rangeKeyAttribute.attributeType();
          switch (rangeKeyType) {
          case S:
              indexOn.append("BSON_VALUE(COL,'").append(rangeKey.attributeName())
                  .append("','VARCHAR')");
              break;
          case N:
              indexOn.append("BSON_VALUE(COL,'").append(rangeKey.attributeName())
                  .append("','DOUBLE')");
              break;
          case B:
              indexOn.append("BSON_VALUE(COL,'").append(rangeKey.attributeName())
                  .append("','VARBINARY_ENCODED')");
              break;
          default:
              throw new IllegalArgumentException(
                  "Attribute Type " + rangeKeyType + " is not " + "correct type");
          }
          indexSortKey = "BSON_VALUE(COL,'" + rangeKey.attributeName() + "','VARCHAR')";
      }

      indexDDLs.add(
              "CREATE INDEX " + indexName + " ON " + request.tableName()
                      + " (" + indexOn + ") INCLUDE (COL) WHERE " + indexHashKey + " IS NOT " +
                      "NULL" + ((indexSortKey != null) ? " AND " + indexSortKey + " IS NOT " +
                      "NULL" : ""));
  }

  public static List<String> getIndexDDLs(CreateTableRequest request, Set<String> pkCols) {
      final List<String> indexDDLs = new ArrayList<>();
      List<AttributeDefinition> attributeDefinitions = request.attributeDefinitions();

      if (request.globalSecondaryIndexes() != null) {
          for (GlobalSecondaryIndex globalSecondaryIndex : request.globalSecondaryIndexes()) {
              final String indexName = globalSecondaryIndex.indexName();
              final List<KeySchemaElement> keySchemaElements =
                  globalSecondaryIndex.keySchema();
              addIndexDDL(request, pkCols, keySchemaElements,
                  attributeDefinitions, indexDDLs, indexName);
          }
      }

      if (request.localSecondaryIndexes() != null) {
          for (LocalSecondaryIndex localSecondaryIndex : request.localSecondaryIndexes()) {
              final String indexName = localSecondaryIndex.indexName();
              final List<KeySchemaElement> keySchemaElements = localSecondaryIndex.keySchema();
              addIndexDDL(request, pkCols, keySchemaElements,
                  attributeDefinitions, indexDDLs, indexName);
          }
      }
      return indexDDLs;
  }

    /**
     * If StreamEnabled is set to true, return 2 DDLs for CDC.
     * 1. CREATE CDC ddl to create the virtual cdc table and index
     * 2. ALTER TABLE ddl to store Stream Type in the table metadata
     */
  public static List<String> getCdcDDL(CreateTableRequest request) {
      final List<String> cdcDDLs = new ArrayList<>();
      StreamSpecification streamSpec = request.streamSpecification();
      if (streamSpec != null && streamSpec.streamEnabled()) {
        String tableName = request.tableName();
        StreamViewType streamType = streamSpec.streamViewType();
        cdcDDLs.add(String.format(CREATE_CDC_DDL, tableName, tableName));
        cdcDDLs.add(String.format(ALTER_TABLE_STREAM_TYPE_DDL, tableName, streamType));
      }
      return cdcDDLs;
  }

  public static CreateTableResponse createTable(final CreateTableRequest request,
      final String connectionUrl) {
      final String tableName = request.tableName();
      List<KeySchemaElement> keySchemaElements = request.keySchema();
      List<AttributeDefinition> attributeDefinitions = request.attributeDefinitions();

      StringBuilder cols = new StringBuilder();
      StringBuilder pkCols = new StringBuilder();
      Set<String> pkColsSet = new HashSet<>();

      KeySchemaElement hashKey = keySchemaElements.get(0);
      String hashKeyQuoted = CommonServiceUtils.getEscapedArgument(hashKey.attributeName());
      cols.append(hashKeyQuoted).append(" ");
      pkCols.append(hashKeyQuoted);

      Optional<AttributeDefinition> hashKeyAttr =
              attributeDefinitions.stream()
                      .filter(attributeDefinition -> hashKey.attributeName()
                              .equals(attributeDefinition.attributeName())).findFirst();
      Preconditions.checkArgument(hashKeyAttr.isPresent(), "Hash key attribute should be " +
              "defined");
      AttributeDefinition hashKeyAttribute = hashKeyAttr.get();
      ScalarAttributeType hashKeyType = hashKeyAttribute.attributeType();
      pkColsSet.add(hashKey.attributeName());
      switch (hashKeyType) {
          case S:
              cols.append("VARCHAR NOT NULL");
              break;
          case N:
              cols.append("DOUBLE NOT NULL");
              break;
          case B:
              cols.append("VARBINARY_ENCODED NOT NULL");
              break;
          default:
              throw new IllegalArgumentException("Attribute Type " + hashKeyType + " is not " +
                      "correct type");
      }

      if (keySchemaElements.size() == 2) {
          cols.append(", ");
          KeySchemaElement rangeKey = keySchemaElements.get(1);
          String rangeKeyQuoted = CommonServiceUtils.getEscapedArgument(rangeKey.attributeName());
          cols.append(rangeKeyQuoted).append(" ");
          pkCols.append(",").append(rangeKeyQuoted);

          Optional<AttributeDefinition> rangeKeyAttr =
                  attributeDefinitions.stream()
                          .filter(attributeDefinition -> rangeKey.attributeName()
                                  .equals(attributeDefinition.attributeName())).findFirst();
          Preconditions.checkArgument(rangeKeyAttr.isPresent(), "Range key attribute should be "
                  + "defined");
          AttributeDefinition rangeKeyAttribute = rangeKeyAttr.get();
          ScalarAttributeType rangeKeyType = rangeKeyAttribute.attributeType();
          pkColsSet.add(rangeKey.attributeName());
          switch (rangeKeyType) {
              case S:
                  cols.append("VARCHAR NOT NULL");
                  break;
              case N:
                  cols.append("DOUBLE NOT NULL");
                  break;
              case B:
                  cols.append("VARBINARY_ENCODED NOT NULL");
                  break;
              default:
                  throw new IllegalArgumentException("Attribute Type " + rangeKeyType + " is " +
                          "not correct type");
          }
      }
      cols.append(", COL BSON CONSTRAINT pk PRIMARY KEY (")
              .append(pkCols)
              .append(")");

      String createTableDDL = "CREATE TABLE " + tableName + " (" + cols + ") MERGE_ENABLED = false";
      LOGGER.info("Create Table Query: " + createTableDDL);

      List<String> createIndexDDLs = getIndexDDLs(request, pkColsSet);
      for (String createIndexDDL : createIndexDDLs) {
          LOGGER.info("Create Index Query: " + createIndexDDL);
      }

      List<String> createCdcDDLs = getCdcDDL(request);
      for (String ddl : createCdcDDLs) {
          LOGGER.info("CDC DDL: " + ddl);
      }

      try (Connection connection = DriverManager.getConnection(connectionUrl)) {
          connection.createStatement().execute(createTableDDL);
          for (String createIndexDDL : createIndexDDLs) {
              connection.createStatement().execute(createIndexDDL);
          }
          for (String ddl : createCdcDDLs) {
              connection.createStatement().execute(ddl);
          }
      } catch (SQLException e) {
          throw new RuntimeException(e);
      }

      return getCreateTableResponse(request.tableName(), connectionUrl);
  }
}
