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

import com.amazonaws.services.dynamodbv2.model.DeleteTableResult;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import org.apache.phoenix.ddb.utils.TableDescriptorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeleteTableService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTableService.class);

    public static DeleteTableResult deleteTable(final String tableName,
                                                final String connectionUrl) {
        DeleteTableResult result = new DeleteTableResult();
        TableDescription
                tableDescription = TableDescriptorUtils.getTableDescription(tableName, connectionUrl);
        result.setTableDescription(tableDescription);
        String deleteTableDDL = "DROP TABLE " + tableName;
        LOGGER.info("Delete Table Query: " + deleteTableDDL);

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.createStatement().execute(deleteTableDDL);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return result;
    }
}

