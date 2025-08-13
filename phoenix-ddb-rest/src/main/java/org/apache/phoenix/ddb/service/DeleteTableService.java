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
import java.util.Map;

import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;

public class DeleteTableService {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteTableService.class);

    public static Map<String, Object> deleteTable(Map<String, Object> deleteTableRequest,
            final String connectionUrl) {
        String tableName = (String) deleteTableRequest.get(ApiMetadata.TABLE_NAME);
        Map<String, Object> tableDescription =
                TableDescriptorUtils.getTableDescription(tableName, connectionUrl,
                        "TableDescription");
        String deleteTableDDL = "DROP TABLE DDB.\"" + tableName + "\" CASCADE";
        LOGGER.debug("Delete Table Query: {}", deleteTableDDL);

        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.createStatement().execute(deleteTableDDL);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return tableDescription;
    }
}

