package org.apache.phoenix.ddb.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.phoenix.ddb.service.utils.ValidationUtil;
import org.apache.phoenix.ddb.utils.ApiMetadata;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.lang3.StringUtils;
import org.apache.phoenix.ddb.service.utils.DMLUtils;
import org.apache.phoenix.ddb.utils.CommonServiceUtils;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.PTable;
import org.apache.phoenix.schema.PTableKey;

public class UpdateItemService {

    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateItemService.class);

    private static final String UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,'%s')";

    private static final String UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = BSON_UPDATE_EXPRESSION(COL,'%s')";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,'%s') \n" + " ELSE COL END";

    private static final String CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY =
            "UPSERT INTO %s.\"%s\" VALUES (?,?) " + " ON DUPLICATE KEY UPDATE\n"
                    + " COL = CASE WHEN BSON_CONDITION_EXPRESSION(COL,'%s') "
                    + " THEN BSON_UPDATE_EXPRESSION(COL,'%s') \n" + " ELSE COL END";

    public static Map<String, Object> updateItem(Map<String, Object> request,
            String connectionUrl) {
        ValidationUtil.validateUpdateItemRequest(request);
        try (Connection connection = DriverManager.getConnection(connectionUrl)) {
            connection.setAutoCommit(true);
            // get PTable and PK PColumns
            PhoenixConnection phoenixConnection = connection.unwrap(PhoenixConnection.class);
            PTable table = phoenixConnection.getTable(new PTableKey(phoenixConnection.getTenantId(),
                    "DDB." + request.get(ApiMetadata.TABLE_NAME)));
            List<PColumn> pkCols = table.getPKColumns();

            //create statement based on PKs and conditional expression
            PreparedStatement stmt = getPreparedStatement(connection, request, pkCols.size());

            // extract PKs from item
            DMLUtils.setKeysOnStatement(stmt, pkCols, (Map<String, Object>) request.get(ApiMetadata.KEY));

            //execute, auto commit is on
            LOGGER.debug("Upsert Query for UpdateItem: {}", stmt);
            Map<String, Object> returnAttrs =
                    DMLUtils.executeUpdate(stmt, (String) request.get(ApiMetadata.RETURN_VALUES),
                            (String) request.get(ApiMetadata.RETURN_VALUES_ON_CONDITION_CHECK_FAILURE),
                            (String) request.get(ApiMetadata.CONDITION_EXPRESSION), pkCols, false);
            return returnAttrs;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static PreparedStatement getPreparedStatement(Connection conn,
            Map<String, Object> request, int numPKs) throws SQLException {
        PreparedStatement stmt;
        String tableName = (String) request.get(ApiMetadata.TABLE_NAME);
        String condExpr = (String) request.get(ApiMetadata.CONDITION_EXPRESSION);
        Map<String, String> exprAttrNames =
                (Map<String, String>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_NAMES);
        Map<String, Object> exprAttrVals =
                (Map<String, Object>) request.get(ApiMetadata.EXPRESSION_ATTRIBUTE_VALUES);
        BsonDocument updateExpr = CommonServiceUtils.getBsonUpdateExpressionFromMap(
                (String) request.get(ApiMetadata.UPDATE_EXPRESSION), exprAttrNames, exprAttrVals);
        if (!StringUtils.isEmpty(condExpr)) {
            String bsonCondExpr =
                    CommonServiceUtils.getBsonConditionExpressionFromMap(condExpr, exprAttrNames,
                            exprAttrVals);
            String QUERY_FORMAT = (numPKs == 1) ?
                    CONDITIONAL_UPDATE_WITH_HASH_KEY :
                    CONDITIONAL_UPDATE_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(
                    String.format(QUERY_FORMAT, "DDB", tableName, bsonCondExpr, updateExpr));
        } else {
            String QUERY_FORMAT = (numPKs == 1) ? UPDATE_WITH_HASH_KEY : UPDATE_WITH_HASH_SORT_KEY;
            stmt = conn.prepareStatement(String.format(QUERY_FORMAT, "DDB", tableName, updateExpr));
        }
        return stmt;
    }
}
