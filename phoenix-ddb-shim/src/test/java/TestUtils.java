import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import org.apache.phoenix.compile.ExplainPlan;
import org.apache.phoenix.compile.ExplainPlanAttributes;
import org.apache.phoenix.ddb.service.QueryUtils;
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
    public static void validateIndexUsed(QueryRequest qr, String url) throws SQLException {
        String tableName = qr.getTableName();
        String indexName = qr.getIndexName();
        List<PColumn> tablePKCols, indexPKCols = null;
        try (Connection connection = DriverManager.getConnection(url)) {
            tablePKCols = PhoenixUtils.getPKColumns(connection, tableName);
            indexPKCols = PhoenixUtils.getOnlyIndexPKColumns(connection, indexName, tableName);
            PreparedStatement ps = QueryUtils.getPreparedStatement(connection, qr, true, tablePKCols, indexPKCols);
            ExplainPlan plan = ps.unwrap(PhoenixPreparedStatement.class).optimizeQuery().getExplainPlan();
            ExplainPlanAttributes explainPlanAttributes = plan.getPlanStepsAsAttributes();
            Assert.assertEquals(indexName, explainPlanAttributes.getTableName());
        }
    }
}
