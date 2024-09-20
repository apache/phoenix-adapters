import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.ddb.service.QueryService;
import org.apache.phoenix.ddb.service.ScanService;
import org.apache.phoenix.ddb.utils.PhoenixUtils;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.schema.PColumn;
import org.junit.Assert;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

public class TestUtils {

    /**
     * Verify index is used for a SQL query formed using a QueryRequest.
     */
    public static void validateIndexUsed(QueryRequest qr, String url) throws SQLException {
        String tableName = qr.getTableName();
        String indexName = qr.getIndexName();
        List<PColumn> tablePKCols, indexPKCols;
        try (Connection connection = DriverManager.getConnection(url)) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            PreparedStatement ps = QueryService.getPreparedStatement(connection, qr, true, tablePKCols, indexPKCols);
            ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
            Assert.assertEquals(indexName, explainPlanAttributes.getTableName());
        }
    }

    /**
     * Verify index is used for a SQL query formed using a ScanRequest.
     */
    public static void validateIndexUsed(ScanRequest sr, String url) throws SQLException {
        String tableName = sr.getTableName();
        String indexName = sr.getIndexName();
        List<PColumn> tablePKCols, indexPKCols;
        try (Connection connection = DriverManager.getConnection(url)) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            PreparedStatement ps = ScanService.getPreparedStatement(connection, sr, true, tablePKCols, indexPKCols);
            ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
            Assert.assertEquals(indexName, explainPlanAttributes.getTableName());
        }
    }
}
