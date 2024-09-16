package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
}
