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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.schema.TableAlreadyExistsException;
import org.apache.phoenix.schema.TableNotFoundException;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.cache.Cache;
import org.apache.phoenix.thirdparty.com.google.common.cache.CacheBuilder;

public class CreateTableService {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableService.class);

    private static final String CREATE_CDC_DDL = "CREATE CDC CDC_%s on %s";
    private static final String ALTER_TABLE_STREAM_TYPE_DDL =
            "ALTER TABLE %s set SCHEMA_VERSION = '%s'";

    private static final Cache<String, ReentrantLock> CREATE_TABLE_LOCKS =
            CacheBuilder.newBuilder().expireAfterAccess(5, TimeUnit.MINUTES).build();

    public static Map<String, Object> getCreateTableResponse(final String tableName,
                                                             final String connectionUrl) {
        return TableDescriptorUtils.getTableDescription(tableName, connectionUrl,
                "TableDescription");
    }

    public static void addIndexDDL(Map<String, Object> request,
            Set<String> pkCols, List<Map<String, Object>> keySchemaElements,
            List<Map<String, Object>> attributeDefinitions, List<String> indexDDLs,
            String indexName) {
        final StringBuilder indexOn = new StringBuilder();

        String indexHashKey = null;
        String indexSortKey = null;

        Map<String, Object> hashKey = keySchemaElements.get(0);
        // re-arrange hash and sort keys if required
        if ("RANGE".equals(hashKey.get("KeyType"))) {
            if (keySchemaElements.size() != 2) {
                throw new IllegalArgumentException("Global Index Range key attribute present "
                        + "but key schema element size is not 2");
            }
            Map<String, Object> sortKey = keySchemaElements.get(0);
            hashKey = keySchemaElements.get(1);
            keySchemaElements = new ArrayList<>(2);
            keySchemaElements.add(hashKey);
            keySchemaElements.add(sortKey);
        }
        Preconditions.checkArgument("HASH".equals(hashKey.get("KeyType")), "Hash key not present");

        String hashKeyType = null;
        for (Map<String, Object> attributeDef : attributeDefinitions) {
            if (hashKey.get("AttributeName").equals(attributeDef.get("AttributeName"))) {
                hashKeyType = (String) attributeDef.get("AttributeType");
                break;
            }
        }
        Preconditions.checkArgument(hashKeyType != null, "Hash key attribute should be defined");

        String hashKeyAttributeName = (String) hashKey.get("AttributeName");
        switch (hashKeyType) {
            case "S":
                indexOn.append("BSON_VALUE(COL,'").append(hashKeyAttributeName)
                        .append("','VARCHAR')");
                break;
            case "N":
                indexOn.append("BSON_VALUE(COL,'").append(hashKeyAttributeName)
                        .append("','DOUBLE')");
                break;
            case "B":
                indexOn.append("BSON_VALUE(COL,'").append(hashKeyAttributeName)
                        .append("','VARBINARY_ENCODED')");
                break;
            default:
                throw new IllegalArgumentException(
                        "Attribute Type " + hashKeyType + " is not " + "correct type");
        }
        indexHashKey = "BSON_VALUE(COL,'" + hashKeyAttributeName + "','VARCHAR')";

        if (keySchemaElements.size() == 2) {
            Map<String, Object> rangeKey = keySchemaElements.get(1);
            indexOn.append(",");

            String rangeKeyType = null;
            for (Map<String, Object> attributeDef : attributeDefinitions) {
                if (rangeKey.get("AttributeName").equals(attributeDef.get("AttributeName"))) {
                    rangeKeyType = (String) attributeDef.get("AttributeType");
                    break;
                }
            }
            Preconditions.checkArgument(rangeKeyType != null,
                    "Global Index Range key attribute should be defined");

            String rangeKeyAttributeName = (String) rangeKey.get("AttributeName");
            switch (rangeKeyType) {
                case "S":
                    indexOn.append("BSON_VALUE(COL,'").append(rangeKeyAttributeName)
                            .append("','VARCHAR')");
                    break;
                case "N":
                    indexOn.append("BSON_VALUE(COL,'").append(rangeKeyAttributeName)
                            .append("','DOUBLE')");
                    break;
                case "B":
                    indexOn.append("BSON_VALUE(COL,'").append(rangeKeyAttributeName)
                            .append("','VARBINARY_ENCODED')");
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Attribute Type " + rangeKeyType + " is not " + "correct type");
            }
            indexSortKey = "BSON_VALUE(COL,'" + rangeKeyAttributeName + "','VARCHAR')";
        }

        indexDDLs.add(
                "CREATE INDEX " + indexName + " ON " + request.get("TableName")
                        + " (" + indexOn + ") INCLUDE (COL) WHERE " + indexHashKey + " IS NOT " +
                        "NULL" + ((indexSortKey != null) ? " AND " + indexSortKey + " IS NOT " +
                        "NULL" : ""));
    }

    public static List<String> getIndexDDLs(Map<String, Object> request, Set<String> pkCols) {
        final List<String> indexDDLs = new ArrayList<>();
        List<Map<String, Object>> attributeDefinitions =
                (List<Map<String, Object>>) request.get("AttributeDefinitions");

        if (request.get("GlobalSecondaryIndexes") != null) {
            for (Map<String, Object> globalSecondaryIndex : (List<Map<String, Object>>) request.get(
                    "GlobalSecondaryIndexes")) {
                final String indexName = (String) globalSecondaryIndex.get("IndexName");
                final List<Map<String, Object>> keySchemaElements =
                        (List<Map<String, Object>>) globalSecondaryIndex.get("KeySchema");
                addIndexDDL(request, pkCols, keySchemaElements,
                        attributeDefinitions, indexDDLs, indexName);
            }
        }

        if (request.get("LocalSecondaryIndexes") != null) {
            for (Map<String, Object> localSecondaryIndex : (List<Map<String, Object>>) request.get(
                    "LocalSecondaryIndexes")) {
                final String indexName = (String) localSecondaryIndex.get("IndexName");
                final List<Map<String, Object>> keySchemaElements =
                        (List<Map<String, Object>>) localSecondaryIndex.get("KeySchema");
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
    public static List<String> getCdcDDL(Map<String, Object> request) {
        final List<String> cdcDDLs = new ArrayList<>();
        Map<String, Object> streamSpec = (Map<String, Object>) request.get("StreamSpecification");
        if (streamSpec != null && (Boolean) streamSpec.get("StreamEnabled")) {
            String tableName = (String) request.get("TableName");
            String streamType = (String) streamSpec.get("StreamViewType");
            cdcDDLs.add(String.format(CREATE_CDC_DDL, tableName, tableName));
            cdcDDLs.add(String.format(ALTER_TABLE_STREAM_TYPE_DDL, tableName, streamType));
        }
        return cdcDDLs;
    }

    public static Map<String, Object> createTable(final Map<String, Object> request,
            final String connectionUrl) {
        final String tableName = (String) request.get("TableName");

        CREATE_TABLE_LOCKS.asMap().putIfAbsent(tableName, new ReentrantLock());
        CREATE_TABLE_LOCKS.asMap().get(tableName).lock();
        try {

            List<Map<String, Object>> keySchemaElements =
                    (List<Map<String, Object>>) request.get("KeySchema");
            List<Map<String, Object>> attributeDefinitions =
                    (List<Map<String, Object>>) request.get("AttributeDefinitions");

            StringBuilder cols = new StringBuilder();
            StringBuilder pkCols = new StringBuilder();
            Set<String> pkColsSet = new HashSet<>();

            Map<String, Object> hashKey = keySchemaElements.get(0);
            // re-arrange hash and sort keys if required
            if ("RANGE".equals(hashKey.get("KeyType"))) {
                if (keySchemaElements.size() != 2) {
                    throw new IllegalArgumentException(
                            "Range key attribute present but key schema element size is not 2");
                }
                Map<String, Object> sortKey = keySchemaElements.get(0);
                hashKey = keySchemaElements.get(1);
                keySchemaElements = new ArrayList<>(2);
                keySchemaElements.add(hashKey);
                keySchemaElements.add(sortKey);
            }
            Preconditions.checkArgument("HASH".equals(hashKey.get("KeyType")),
                    "Hash key not present");

            String hashKeyQuoted =
                    CommonServiceUtils.getEscapedArgument((String) hashKey.get("AttributeName"));
            cols.append(hashKeyQuoted).append(" ");
            pkCols.append(hashKeyQuoted);

            String hashKeyType = null;
            for (Map<String, Object> attributeDef : attributeDefinitions) {
                if (hashKey.get("AttributeName").equals(attributeDef.get("AttributeName"))) {
                    hashKeyType = (String) attributeDef.get("AttributeType");
                    break;
                }
            }
            Preconditions.checkArgument(hashKeyType != null,
                    "Hash key attribute should be defined");

            pkColsSet.add((String) hashKey.get("AttributeName"));
            switch (hashKeyType) {
                case "S":
                    cols.append("VARCHAR NOT NULL");
                    break;
                case "N":
                    cols.append("DOUBLE NOT NULL");
                    break;
                case "B":
                    cols.append("VARBINARY_ENCODED NOT NULL");
                    break;
                default:
                    throw new IllegalArgumentException(
                            "Attribute Type " + hashKeyType + " is not " + "correct type");
            }

            if (keySchemaElements.size() == 2) {
                cols.append(", ");
                Map<String, Object> rangeKey = keySchemaElements.get(1);
                String rangeKeyQuoted = CommonServiceUtils.getEscapedArgument(
                        (String) rangeKey.get("AttributeName"));
                cols.append(rangeKeyQuoted).append(" ");
                pkCols.append(",").append(rangeKeyQuoted);

                String rangeKeyType = null;
                for (Map<String, Object> attributeDef : attributeDefinitions) {
                    if (rangeKey.get("AttributeName").equals(attributeDef.get("AttributeName"))) {
                        rangeKeyType = (String) attributeDef.get("AttributeType");
                        break;
                    }
                }
                Preconditions.checkArgument(rangeKeyType != null,
                        "Range key attribute should be defined");

                pkColsSet.add((String) rangeKey.get("AttributeName"));
                switch (rangeKeyType) {
                    case "S":
                        cols.append("VARCHAR NOT NULL");
                        break;
                    case "N":
                        cols.append("DOUBLE NOT NULL");
                        break;
                    case "B":
                        cols.append("VARBINARY_ENCODED NOT NULL");
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Attribute Type " + rangeKeyType + " is " + "not correct type");
                }
            }
            cols.append(", COL BSON CONSTRAINT pk PRIMARY KEY (").append(pkCols).append(")");

            String createTableDDL =
                    "CREATE TABLE " + tableName + " (" + cols + ") MERGE_ENABLED = false";
            LOGGER.info("Create Table Query: {}", createTableDDL);

            List<String> createIndexDDLs = getIndexDDLs(request, pkColsSet);
            for (String createIndexDDL : createIndexDDLs) {
                LOGGER.info("Create Index Query: " + createIndexDDL);
            }

            List<String> createCdcDDLs = getCdcDDL(request);
            for (String ddl : createCdcDDLs) {
                LOGGER.info("CDC DDL: " + ddl);
            }

            try (Connection connection = DriverManager.getConnection(connectionUrl)) {
                PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
                try {
                    PTable table = phoenixConnection.getTable(
                            new PTableKey(phoenixConnection.getTenantId(), tableName));
                    if (table != null) {
                        return getCreateTableResponse(tableName, connectionUrl);
                    }
                } catch (TableNotFoundException e) {
                    // ignore
                }

                connection.createStatement().execute(createTableDDL);
                for (String createIndexDDL : createIndexDDLs) {
                    connection.createStatement().execute(createIndexDDL);
                }
                for (String ddl : createCdcDDLs) {
                    connection.createStatement().execute(ddl);
                }
            } catch (SQLException e) {
                if (!(e instanceof TableAlreadyExistsException)) {
                    throw new RuntimeException(e);
                }
            }

            return getCreateTableResponse(tableName, connectionUrl);
        } finally {
            CREATE_TABLE_LOCKS.asMap().get(tableName).unlock();
        }
    }
}
