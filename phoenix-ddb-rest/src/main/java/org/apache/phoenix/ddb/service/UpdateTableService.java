package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
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
    private static String DROP_INDEX_SQL = "ALTER INDEX \"%s\" ON %s.\"%s\" DISABLE";

    public static Map<String, Object> updateTable(Map<String, Object> request, String connectionUrl) {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        List<String> ddls = new ArrayList<>();

        // index updates
        if (request.containsKey(ApiMetadata.GLOBAL_SECONDARY_INDEX_UPDATES)) {
            List<Map<String, Object>> indexUpdates = (List<Map<String, Object>>) request.get(ApiMetadata.GLOBAL_SECONDARY_INDEX_UPDATES);
            for (Map<String, Object> indexUpdate : indexUpdates) {
                if (indexUpdate.containsKey(ApiMetadata.DELETE)) {
                    Map<String, Object> deleteIndexUpdate = (Map<String, Object>) indexUpdate.get(ApiMetadata.DELETE);
                    String indexName = (String) deleteIndexUpdate.get(ApiMetadata.INDEX_NAME);
                    String ddl = String.format(DROP_INDEX_SQL, indexName, "DDB", tableName);
                    LOGGER.debug("DDL for Disable Index: {}", ddl);
                    ddls.add(ddl);
                } else if (indexUpdate.containsKey(ApiMetadata.CREATE)) {
                    Map<String, Object> createIndexUpdate = (Map<String, Object>) indexUpdate.get(ApiMetadata.CREATE);
                    String indexName = (String) createIndexUpdate.get(ApiMetadata.INDEX_NAME);
                    List<String> indexDDLs = new ArrayList<>();
                    List<Map<String, Object>> attrDefs = (List<Map<String, Object>>) request.get(ApiMetadata.ATTRIBUTE_DEFINITIONS);
                    List<Map<String, Object>> keySchema = (List<Map<String, Object>>) createIndexUpdate.get(ApiMetadata.KEY_SCHEMA);
                    CreateTableService.addIndexDDL(tableName, keySchema, attrDefs, indexDDLs, indexName, true);
                    LOGGER.debug("DDL for Create Index: {}", indexDDLs);
                    ddls.addAll(indexDDLs);
                } else {
                    throw new ValidationException("Only Create and Delete index is supported in UpdateTable API.");
                }
            }
        }

        //cdc
        if (request.containsKey(ApiMetadata.STREAM_SPECIFICATION)) {
            List<String> cdcDDLs = CreateTableService.getCdcDDL(request);
            LOGGER.debug("DDLs for Create CDC: {} ", cdcDDLs);
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
        return TableDescriptorUtils.getTableDescription(tableName, connectionUrl, ApiMetadata.TABLE_DESCRIPTION);
    }
}
