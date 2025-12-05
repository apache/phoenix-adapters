package org.apache.phoenix.ddb.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.mapreduce.index.IndexTool;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.thirdparty.com.google.common.collect.Lists;
import org.apache.phoenix.util.PhoenixRuntime;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Helper methods for Phoenix based functionality.
 */
public class PhoenixUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(PhoenixUtils.class);

    public static final String URL_PREFIX =
            PhoenixRuntime.JDBC_PROTOCOL + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    public static final String URL_ZK_PREFIX =
            PhoenixRuntime.JDBC_PROTOCOL_ZK + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    public static final String URL_MASTER_PREFIX =
            PhoenixRuntime.JDBC_PROTOCOL_MASTER + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    public static final String URL_RPC_PREFIX =
            PhoenixRuntime.JDBC_PROTOCOL_RPC + PhoenixRuntime.JDBC_PROTOCOL_SEPARATOR;
    public static final String TTL_EXPRESSION = "BSON_VALUE(COL, ''%s'', ''BIGINT'') IS NOT NULL "
            + "AND TO_NUMBER(CURRENT_TIME()) > BSON_VALUE(COL, ''%s'', ''BIGINT'') * 1000";
    
    public static final String SCHEMA_NAME = "DDB";
    public static final String SCHEMA_DELIMITER = ".";
    public static final String SCHEMA_NAME_AND_DELIM = SCHEMA_NAME + SCHEMA_DELIMITER;
    public static final String ESCAPE_CHARACTER = "\"";

    /**
     * To support same index names for different tables under DDB schema,
     * we will prefix index names with the table names.
     */
    public static String getInternalIndexName(String tableName, String indexName) {
        return tableName + "_" + indexName;
    }

    public static String getIndexNameFromInternalName(String tableName, String internalIndexName) {
        return internalIndexName.split(tableName + "_")[1];
    }

    /**
     *  Append schema name to return the full table name.
     *  Escape tableName with quotes if case-sensitive.
     */
    public static String getFullTableName(String tableName, boolean withQuotes) {
        if (withQuotes) {
            return SCHEMA_NAME + SCHEMA_DELIMITER + "\"" + tableName + "\"";
        } else {
            return SCHEMA_NAME + SCHEMA_DELIMITER + tableName;
        }
    }

    /**
     * Extract table name from the given full name which can be case-sensitive.
     */
    public static String getTableNameFromFullName(String tableName, boolean hasQuotes) {
       tableName = tableName.startsWith(SCHEMA_NAME_AND_DELIM)
                ? tableName.split(SCHEMA_NAME_AND_DELIM)[1]
                : tableName;
       if (hasQuotes) {
           tableName = tableName.substring(1, tableName.length()-1);
       }
       return tableName;
    }

    public static String getEscapedArgument(String arg) {
        return ESCAPE_CHARACTER + arg + ESCAPE_CHARACTER;
    }

    /**
     * Check whether the connection url provided has the right format.
     *
     * @param connectionUrl
     */
    public static void checkConnectionURL(String connectionUrl) {
        Preconditions.checkArgument(connectionUrl != null && (connectionUrl.startsWith(URL_PREFIX)
                        || connectionUrl.startsWith(URL_ZK_PREFIX) || connectionUrl.startsWith(
                        URL_MASTER_PREFIX) || connectionUrl.startsWith(URL_RPC_PREFIX)),
                "JDBC url " + connectionUrl + " does not have the correct prefix");
    }

    /**
     * Register a Phoenix Driver.
     */
    public static void registerDriver() {
        try {
            DriverManager.registerDriver(PhoenixDriver.INSTANCE);
        } catch (SQLException e) {
            LOGGER.error("Phoenix Driver registration failed", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Return the list of PK Columns for the given table.
     */
    public static List<PColumn> getPKColumns(Connection conn, String tableName)
            throws SQLException {
        PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
        PTable table = phoenixConnection.getTable(
                new PTableKey(phoenixConnection.getTenantId(), getFullTableName(tableName, false)));
        return table.getPKColumns();
    }

    /**
     * Return the list of PK Columns ONLY for the given index table.
     */
    public static List<PColumn> getOnlyIndexPKColumns(Connection conn, String indexName,
            String tableName) throws SQLException {
        List<PColumn> indexPKCols = new ArrayList<>();
        List<PColumn> tablePKCols = getPKColumns(conn, tableName);
        List<PColumn> indexAndTablePKCols = getPKColumns(conn, getInternalIndexName(tableName, indexName));
        int numIndexPKs = indexAndTablePKCols.size() - tablePKCols.size();
        for (int i = 0; i < numIndexPKs; i++) {
            indexPKCols.add(indexAndTablePKCols.get(i));
        }
        return indexPKCols;
    }

    /**
     * Return the COUNT_ROWS_SCANNED metric for the given ResultSet.
     */
    public static long getRowsScanned(ResultSet rs) throws SQLException {
        Map<String, Map<MetricType, Long>> metrics = PhoenixRuntime.getRequestReadMetricInfo(rs);

        long sum = 0;
        boolean valid = false;
        for (Map.Entry<String, Map<MetricType, Long>> entry : metrics.entrySet()) {
            Long val = entry.getValue().get(MetricType.COUNT_ROWS_SCANNED);
            if (val != null) {
                sum += val.longValue();
                valid = true;
            }
        }
        if (valid) {
            return sum;
        } else {
            return -1;
        }
    }

    /**
     * Extract the attribute name from the given conditional TTL Expression
     * of the form {@code TTL_EXPRESSION}.
     */
    public static String extractAttributeFromTTLExpression(String ttlExpression) {
        // ttlExpression -> null check AND timestamp check
        return CommonServiceUtils.getKeyNameFromBsonValueFunc(
                        ttlExpression.split("IS NOT NULL")[0]) // pass bson_value part
                .replaceAll("'", "") // remove single quotes
                .trim();
    }

    /**
     * Run IndexTool to build indexes.
     */
    public static IndexTool runIndexTool(Configuration conf, boolean useSnapshot, String schemaName,
                                         String dataTableName, String indexTableName, String tenantId,
                                         IndexTool.IndexVerifyType verifyType) throws Exception {
        IndexTool indexingTool = new IndexTool();
        indexingTool.setConf(conf);
        final String[] cmdArgs = getArgValues(useSnapshot, schemaName, dataTableName, indexTableName,
                tenantId, verifyType);
        List<String> cmdArgList = new ArrayList<>(Arrays.asList(cmdArgs));
        LOGGER.info("Running IndexTool with {}", Arrays.toString(cmdArgList.toArray()));
        int status = indexingTool.run(cmdArgList.toArray(new String[cmdArgList.size()]));
        LOGGER.info("IndexTool status = {}", status);
        return indexingTool;
    }

    private static String[] getArgValues(boolean useSnapshot, String schemaName, String dataTable, String indexTable, String tenantId, IndexTool.IndexVerifyType verifyType) {
        List<String> args = getArgList(useSnapshot, schemaName, dataTable, indexTable, tenantId, verifyType, (Long)null, (Long)null, (Long)null, false);
        return (String[])args.toArray(new String[0]);
    }

    private static List<String> getArgList(boolean useSnapshot, String schemaName, String dataTable, String indxTable, String tenantId, IndexTool.IndexVerifyType verifyType, Long startTime, Long endTime, Long incrementalVerify, boolean useIndexTableAsSource) {
        List<String> args = Lists.newArrayList();
        if (schemaName != null) {
            args.add("--schema=" + schemaName);
        }

        args.add("--data-table=" + dataTable);
        args.add("--index-table=" + indxTable);
        args.add("-v");
        args.add(verifyType.getValue());
        args.add("-runfg");
        if (useSnapshot) {
            args.add("-snap");
        }

        if (tenantId != null) {
            args.add("-tenant");
            args.add(tenantId);
        }

        if (startTime != null) {
            args.add("-st");
            args.add(String.valueOf(startTime));
        }

        if (endTime != null) {
            args.add("-et");
            args.add(String.valueOf(endTime));
        }

        if (incrementalVerify != null) {
            args.add("-rv");
            args.add(String.valueOf(incrementalVerify));
        }

        if (useIndexTableAsSource) {
            args.add("-fi");
        }

        args.add("-op");
        args.add("/tmp/" + UUID.randomUUID().toString());
        return args;
    }
}
