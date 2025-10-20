/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.bson.RawBsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.phoenix.ddb.bson.BsonDocumentToMap;
import org.apache.phoenix.ddb.service.utils.DQLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.schema.PColumn;

public class GetItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(GetItemService.class);

    private static final String SELECT_QUERY = "SELECT COL FROM %s WHERE %s = ? ";
    private static final String CLAUSE_FOR_SORT_COL = "AND %s = ?";

    public static Map<String, Object> getItem(Map<String, Object> request, String connectionUrl) {
        ValidationUtil.validateGetItemRequest(request);
        CommonServiceUtils.handleLegacyProjectionConversion(request);
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        List<PColumn> tablePKCols = null;
        try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
            // get PKs from phoenix
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);

            //build prepared statement and execute
            PreparedStatement stmt = getPreparedStatement(connection, request, tablePKCols);
            return executeQuery(stmt, request);
        } catch (SQLException e) {
            throw new PhoenixServiceException(e);
        }
    }

    /**
     * Build the SELECT query based on the query request parameters.
     * Return a PreparedStatement with values set.
     */
    public static PreparedStatement getPreparedStatement(Connection conn,
            Map<String, Object> request, List<PColumn> tablePKCols) throws SQLException {
        String tableName = (String) request.get("TableName");
        String partitionKeyPKCol = tablePKCols.get(0).toString();

        StringBuilder queryBuilder = new StringBuilder(String.format(SELECT_QUERY,
                PhoenixUtils.getFullTableName(tableName, true),
                CommonServiceUtils.getEscapedArgument(partitionKeyPKCol)));
        if (tablePKCols.size() > 1) {
            String sortKeyPKCol = tablePKCols.get(1).toString();
            queryBuilder.append(String.format(CLAUSE_FOR_SORT_COL,
                    CommonServiceUtils.getEscapedArgument(sortKeyPKCol)));
        }
        LOGGER.debug("SELECT Query: " + queryBuilder);
        PreparedStatement stmt = conn.prepareStatement(queryBuilder.toString());
        setPreparedStatementValues(stmt, request, tablePKCols);
        return stmt;
    }

    /**
     * Set all the values on the PreparedStatement:
     * - 1 value for partitionKey,
     * - 1 or 2 values for sortKey, if present
     */
    private static void setPreparedStatementValues(PreparedStatement stmt,
            Map<String, Object> request, List<PColumn> tablePKCols) throws SQLException {
        String partitionKeyPKCol = tablePKCols.get(0).toString();
        Map<String, Object> keyAttributes = (Map<String, Object>) request.get(ApiMetadata.KEY);
        DQLUtils.setKeyValueOnStatement(stmt, 1,
                (Map<String, Object>) keyAttributes.get(partitionKeyPKCol), false);
        if (tablePKCols.size() > 1) {
            String sortKeyPKCol = tablePKCols.get(1).toString();
            DQLUtils.setKeyValueOnStatement(stmt, 2,
                    (Map<String, Object>) keyAttributes.get(sortKeyPKCol), false);
        }
    }

    /**
     * Execute the given PreparedStatement, collect the returned item with projected attributes
     * and return GetItemResponse.
     */
    private static Map<String, Object> executeQuery(PreparedStatement stmt,
            Map<String, Object> request) throws SQLException {
        Map<String, Object> finalResult = new HashMap<>();
        ResultSet rs = stmt.executeQuery();
        if (rs.next()) {
            Map<String, Object> item =
                    BsonDocumentToMap.getProjectedItem((RawBsonDocument) rs.getObject(1),
                            getProjectionAttributes(request));
            finalResult.put("Item", item);
        }
        return finalResult;
    }

    /**
     * Return a list of attribute names to project.
     */
    private static List<String> getProjectionAttributes(Map<String, Object> request) {
        String projExpr = (String) request.get(ApiMetadata.PROJECTION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        return DQLUtils.getProjectionAttributes(projExpr, exprAttrNames);
    }
}
