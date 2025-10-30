package org.apache.phoenix.ddb.service;

import org.apache.phoenix.ddb.ConnectionUtil;
import org.apache.phoenix.ddb.service.exceptions.PhoenixServiceException;
import org.apache.phoenix.ddb.service.exceptions.ValidationException;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.ddb.service.utils.TableDescriptorUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PIndexState;
import org.apache.phoenix.schema.PTable;

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
    private static final String DROP_INDEX_SQL = "ALTER INDEX \"%s\" ON %s DISABLE";
    private static final String ALTER_MERGE_SQL = "ALTER TABLE %s SET MERGE_ENABLED=false";

    public static Map<String, Object> updateTable(Map<String, Object> request, String connectionUrl) {
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        handleIndexUpdates(request, tableName, connectionUrl);
        handleStreamUpdates(request, tableName, connectionUrl);
        return TableDescriptorUtils.getTableDescription(tableName, connectionUrl, ApiMetadata.TABLE_DESCRIPTION);
    }

    private static void handleStreamUpdates(Map<String, Object> request, String tableName,
            String connectionUrl) {
        if (request.containsKey(ApiMetadata.STREAM_SPECIFICATION)) {
            try (Connection connection = ConnectionUtil.getConnection(connectionUrl)) {
                PhoenixConnection pconn = connection.unwrap(PhoenixConnection.class);
                PTable table = pconn.getTableNoCache(pconn.getTenantId(), PhoenixUtils.getFullTableName(tableName, false));
                for (String ddl : getCdcDdlsToExecute(table, request)) {
                    connection.createStatement().execute(ddl);
                }
            } catch (SQLException e) {
                throw new PhoenixServiceException(e);
            }
        }
    }

    private static void handleIndexUpdates(Map<String, Object> request, String tableName,
            String connectionUrl) {
        List<String> indexDDLs = new ArrayList<>();
        if (request.containsKey(ApiMetadata.GLOBAL_SECONDARY_INDEX_UPDATES)) {
            List<Map<String, Object>> indexUpdates
                    = (List<Map<String, Object>>) request.get(ApiMetadata.GLOBAL_SECONDARY_INDEX_UPDATES);
            for (Map<String, Object> indexUpdate : indexUpdates) {
                if (indexUpdate.containsKey(ApiMetadata.DELETE)) {
                    Map<String, Object> deleteIndexUpdate
                            = (Map<String, Object>) indexUpdate.get(ApiMetadata.DELETE);
                    String indexName = (String) deleteIndexUpdate.get(ApiMetadata.INDEX_NAME);
                    final String finalIndexName = PhoenixUtils.getInternalIndexName(tableName, indexName);
                    String ddl = String.format(DROP_INDEX_SQL, finalIndexName,
                            PhoenixUtils.getFullTableName(tableName, true));
                    LOGGER.debug("DDL for Disable Index: {}", ddl);
                    indexDDLs.add(ddl);
                } else if (indexUpdate.containsKey(ApiMetadata.CREATE)) {
                    Map<String, Object> createIndexUpdate
                            = (Map<String, Object>) indexUpdate.get(ApiMetadata.CREATE);
                    String indexName = (String) createIndexUpdate.get(ApiMetadata.INDEX_NAME);
                    List<String> ddl = new ArrayList<>();
                    List<Map<String, Object>> attrDefs
                            = (List<Map<String, Object>>) request.get(ApiMetadata.ATTRIBUTE_DEFINITIONS);
                    List<Map<String, Object>> keySchema
                            = (List<Map<String, Object>>) createIndexUpdate.get(ApiMetadata.KEY_SCHEMA);
                    CreateTableService.addIndexDDL(tableName, keySchema, attrDefs, indexDDLs, indexName, true);
                    LOGGER.debug("DDL for Create Index: {}", indexDDLs);
                    indexDDLs.addAll(ddl);
                } else {
                    throw new ValidationException("Only Create and Delete index is supported in UpdateTable API.");
                }
            }
        }
        if (!indexDDLs.isEmpty()) {
            Properties props = ConnectionUtil.getMutableProps();
            props.put(QueryServices.INDEX_CREATE_DEFAULT_STATE, PIndexState.CREATE_DISABLE.toString());
            try (Connection connection = DriverManager.getConnection(connectionUrl, props)) {
                for (String ddl : indexDDLs) {
                    connection.createStatement().execute(ddl);
                }
            } catch (SQLException e) {
                throw new PhoenixServiceException(e);
            }
        }
    }

    public static List<String> getCdcDdlsToExecute(PTable table, Map<String, Object> request) {
        // create cdc, alter stream_view_type
        List<String> cdcDDLs = CreateTableService.getCdcDDL(request);
        Map<String, Object> streamSpec
                = (Map<String, Object>) request.get(ApiMetadata.STREAM_SPECIFICATION);

        boolean currentStreamEnabled = table.getSchemaVersion() != null;
        boolean newStreamEnabled = (boolean) streamSpec.get(ApiMetadata.STREAM_ENABLED);

        // cannot disable if already disabled
        if (!currentStreamEnabled && !newStreamEnabled) {
            throw new ValidationException("Stream is already disabled. Nothing to update.");
        }

        // TODO: we do not support disabling stream yet
        if (currentStreamEnabled && !newStreamEnabled) {
            throw new ValidationException("Disabling a stream is not yet supported.");
        }

        // cannot enable if already enabled
        if (currentStreamEnabled && newStreamEnabled) {
            throw new ValidationException("Table already has an enabled stream.");
        }

        cdcDDLs.add(String.format(ALTER_MERGE_SQL,
                PhoenixUtils.getFullTableName(table.getTableName().getString(), true)));
        LOGGER.debug("DDLs for Update CDC: {} ", cdcDDLs);
        return cdcDDLs;
    }
}
