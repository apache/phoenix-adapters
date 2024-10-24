package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixResultSet;
import org.apache.phoenix.monitoring.MetricType;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;
import org.apache.phoenix.util.PhoenixRuntime;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Helper methods for Phoenix based functionality.
 */
public class PhoenixUtils {

    /**
     * Return the list of PK Columns for the given table.
     */
    public static List<PColumn> getPKColumns(Connection conn, String tableName)
            throws SQLException {
        PhoenixConnection phoenixConnection = conn.unwrap(PhoenixConnection.class);
        PTable table = phoenixConnection.getTable(
                new PTableKey(phoenixConnection.getTenantId(), tableName));
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
}
