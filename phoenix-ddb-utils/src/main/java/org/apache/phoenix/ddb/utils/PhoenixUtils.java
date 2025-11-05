package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixDriver;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.thirdparty.com.google.common.base.Preconditions;
import org.apache.phoenix.util.PhoenixRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
    public static final String TTL_EXPRESSION = "BSON_VALUE(COL, ''%s'', ''BIGINT'') IS NOT NULL " +
            "AND TO_NUMBER(CURRENT_TIME()) > BSON_VALUE(COL, ''%s'', ''BIGINT'') * 1000";

    /**
     * Check whether the connection url provided has the right format.
     * @param connectionUrl
     */
    public static void checkConnectionURL(String connectionUrl) {
        Preconditions.checkArgument(connectionUrl != null &&
                        (connectionUrl.startsWith(URL_PREFIX)
                                || connectionUrl.startsWith(URL_ZK_PREFIX)
                                || connectionUrl.startsWith(URL_MASTER_PREFIX)
                                || connectionUrl.startsWith(URL_RPC_PREFIX)),
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
                new PTableKey(phoenixConnection.getTenantId(), "DDB." + tableName));
        return table.getPKColumns();
    }

    /**
     * Return the list of PK Columns ONLY for the given index table.
     */
    public static List<PColumn> getOnlyIndexPKColumns(Connection conn, String indexName,
                                                      String tableName)
            throws SQLException {
        List<PColumn> indexPKCols = new ArrayList<>();
        List<PColumn> tablePKCols = getPKColumns(conn, tableName);
        List<PColumn> indexAndTablePKCols = getPKColumns(conn, indexName);
        int numIndexPKs = indexAndTablePKCols.size() - tablePKCols.size();
        for (int i=0; i<numIndexPKs; i++) {
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
     * Add common phoenix client configuration properties which need to be
     * set on the connections used in the shim for all operations.
     */
    public static Properties getConnectionProps() {
        Properties props = new Properties();
        props.put(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
        return props;
    }

    /**
     * Extract the attribute name from the given conditional TTL Expression
     * of the form {@code TTL_EXPRESSION}.
     */
    public static String extractAttributeFromTTLExpression(String ttlExpression) {
        // ttlExpression -> null check AND timestamp check
        return CommonServiceUtils
                .getKeyNameFromBsonValueFunc(ttlExpression.split("IS NOT NULL")[0]) // pass bson_value part
                .replaceAll("'", "") // remove single quotes
                .trim();
    }

    /**
     * Get the default table options when creating a new table.
     */
    public static String getTableOptions() {
        return " MERGE_ENABLED=false" + "," +
                "IS_STRICT_TTL=false" + "," +
                "UPDATE_CACHE_FREQUENCY=1800000" + "," +
                "\"phoenix.max.lookback.age.seconds\"=97200" + "," +
                "\"hbase.hregion.majorcompaction\"=172800000";
    }

    /**
     * Get the default index options when creating a new index.
     */
    public static String getIndexOptions() {
        return "\"hbase.hregion.majorcompaction\"=172800000";
    }
}
