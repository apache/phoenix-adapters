package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class UpdateTableService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateTableService.class);
    private static String DROP_INDEX_SQL = "ALTER INDEX \"%s\" ON \"%s\" DISABLE";

    public static Map<String, Object> updateTable(Map<String, Object> request, String connectionUrl) {
        String tableName = (String) request.get("TableName");
        List<String> ddls = new ArrayList<>();

        // index updates
        if (request.containsKey("GlobalSecondaryIndexUpdates")) {
            List<Map<String, Object>> indexUpdates = (List<Map<String, Object>>) request.get("GlobalSecondaryIndexUpdates");
            for (Map<String, Object> indexUpdate : indexUpdates) {
                if (indexUpdate.containsKey("Delete")) {
                    Map<String, Object> deleteIndexUpdate = (Map<String, Object>) indexUpdate.get("Delete");
                    String indexName = (String) deleteIndexUpdate.get("IndexName");
                    String ddl = String.format(DROP_INDEX_SQL, indexName, tableName);
                    LOGGER.info("DDL for Disable Index: {}", ddl);
                    ddls.add(ddl);
                } else if (indexUpdate.containsKey("Create")) {
                    Map<String, Object> createIndexUpdate = (Map<String, Object>) indexUpdate.get("Create");
                    String indexName = (String) createIndexUpdate.get("IndexName");
                    List<String> indexDDLs = new ArrayList<>();
                    List<Map<String, Object>> attrDefs = (List<Map<String, Object>>) request.get("AttributeDefinitions");
                    List<Map<String, Object>> keySchema = (List<Map<String, Object>>) createIndexUpdate.get("KeySchema");
                    CreateTableService.addIndexDDL(tableName, keySchema, attrDefs, indexDDLs, indexName);
                    String ddl = indexDDLs.get(0) + " ASYNC ";
                    LOGGER.info("DDL for Create Index: {}", ddl);
                    ddls.add(ddl);
                } else {
                    throw new IllegalArgumentException("Only Create and Delete index is supported in UpdateTable API.");
                }
            }
        }

        //cdc
        if (request.containsKey("StreamSpecification")) {
            List<String> cdcDDLs = CreateTableService.getCdcDDL(request);
            LOGGER.info("DDLs for Create CDC: {} ", cdcDDLs);
            ddls.addAll(cdcDDLs);
        }

        Properties props = new Properties();
        props.put(QueryServices.INDEX_CREATE_DEFAULT_STATE, PIndexState.CREATE_DISABLE.toString());
        try (Connection connection = DriverManager.getConnection(connectionUrl, props)) {
            for (String ddl : ddls) {
                connection.createStatement().execute(ddl);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
        return TableDescriptorUtils.getTableDescription(tableName, connectionUrl, "TableDescription");
    }
}
