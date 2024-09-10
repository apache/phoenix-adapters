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

package org.apache.phoenix.ddb;

import com.amazonaws.services.dynamodbv2.AbstractAmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;

import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import org.apache.phoenix.ddb.service.CreateTableUtils;
import org.apache.phoenix.ddb.service.PutItemUtils;
import org.apache.phoenix.ddb.service.QueryUtils;
import org.apache.phoenix.ddb.service.TableDescriptorUtils;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * PhoenixDB Client to convert DDB requests into Phoenix SQL queries and execute.
 */
public class PhoenixDBClient extends AbstractAmazonDynamoDB {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixDBClient.class);

    private final String connectionUrl;

    private static final String URL_PREFIX =
            PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    private static final String URL_ZK_PREFIX =
            PhoenixRuntime.JDBC_PROTOCOL_ZK + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    private static final String URL_MASTER_PREFIX =
            PhoenixRuntime.JDBC_PROTOCOL_MASTER + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    private static final String URL_RPC_PREFIX =
            PhoenixRuntime.JDBC_PROTOCOL_RPC + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;

    public PhoenixDBClient(String connectionUrl) {
        Preconditions.checkArgument(connectionUrl != null &&
                        (connectionUrl.startsWith(URL_PREFIX)
                                || connectionUrl.startsWith(URL_ZK_PREFIX)
                                || connectionUrl.startsWith(URL_MASTER_PREFIX)
                                || connectionUrl.startsWith(URL_RPC_PREFIX)),
                "JDBC url " + connectionUrl + " does not have the correct prefix");
        this.connectionUrl = connectionUrl;
        try {
            DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        } catch (SQLException e) {
            LOGGER.error("Phoenix Driver registration failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public CreateTableResult createTable(CreateTableRequest request) {
        return CreateTableUtils.createTable(request, connectionUrl);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DescribeTableResult describeTable(DescribeTableRequest request) {
        return this.describeTable(request.getTableName());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DescribeTableResult describeTable(String tableName) {
        TableDescription tableDescription
                = TableDescriptorUtils.getTableDescription(tableName, connectionUrl);
        return new DescribeTableResult().withTable(tableDescription);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public PutItemResult putItem(PutItemRequest request) {
        return PutItemUtils.putItem(request, connectionUrl);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public QueryResult query(QueryRequest request) {
        return QueryUtils.query(request, connectionUrl);
    }
}
