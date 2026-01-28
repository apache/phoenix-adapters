package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.parse.AndParseNode;
import org.apache.phoenix.parse.BetweenParseNode;
import org.apache.phoenix.parse.BsonExpressionParser;
import org.apache.phoenix.parse.ComparisonParseNode;
import org.apache.phoenix.parse.LiteralParseNode;
import org.apache.phoenix.parse.ParseNode;
import org.apache.phoenix.parse.DocumentFieldBeginsWithParseNode;
import org.apache.phoenix.schema.PColumn;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

/**
 * Helper class to parse KeyConditionExpression provided in request:
 * partitionKeyName = :partitionkeyval AND SortKeyCondition
 *
 * SortKeyCondition can be either sortKeyName op :sortkeyval
 * where op can be =,<,>,<=,>=
 * OR
 * sortKeyName BETWEEN :sortkeyval1 AND :sortkeyval2
 * OR
 * begins_with ( sortKeyName, :sortkeyval )
 */
public class KeyConditionsHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(KeyConditionsHolder.class);

    private String keyCondExpr;
    private Map<String, String> exprAttrNames;
    private List<PColumn> pkCols;
    private boolean useIndex;
    private String partitionKeyName;
    private String partitionValue;
    private String beginsWithSortKey;
    private String beginsWithSortKeyVal;
    private String sortKeyName;
    private String sortKeyOperator;
    private String sortKeyValue1;
    private String sortKeyValue2;

    /**
     * keeping track of Primary Key PColumns here since we
     * need to get the BSON_VALUE name when using indexes.
     */
    private PColumn partitionKeyPKCol;
    private PColumn sortKeyPKCol;

    public KeyConditionsHolder(String keyCondExpr, Map<String, String> exprAttrNames,
                               List<PColumn> pkCols, boolean useIndex) {
        if (pkCols == null) {
            throw new IllegalArgumentException("pkCols cannot be null");
        }
        if (pkCols.isEmpty() || pkCols.size() > 2) {
            throw new IllegalArgumentException(
                "pkCols must contain 1 or 2 columns, but got " + pkCols.size());
        }

        this.keyCondExpr = keyCondExpr;
        this.exprAttrNames = (exprAttrNames != null) ? exprAttrNames : new HashMap<>();
        this.pkCols = pkCols;
        this.useIndex = useIndex;
        this.parse();
    }

    public String getPartitionKeyName() {
        return partitionKeyName;
    }

    public String getSortKeyName() {
        return sortKeyName;
    }

    public String getPartitionValue() {
        return partitionValue;
    }

    public boolean hasSortKey() {
        return sortKeyName != null;
    }

    public String getSortKeyOperator() {
        return sortKeyOperator;
    }

    public String getSortKeyValue1() {
        return sortKeyValue1;
    }

    public String getSortKeyValue2() {
        return sortKeyValue2;
    }

    public String getBeginsWithSortKeyVal() {
        return beginsWithSortKeyVal;
    }

    public PColumn getSortKeyPKCol() {
        return sortKeyPKCol;
    }

    public PColumn getPartitionKeyPKCol() { return partitionKeyPKCol; }

    public boolean hasBetween() {
        return sortKeyOperator.equals("BETWEEN") && sortKeyValue2 != null;
    }

    public boolean hasBeginsWith() {
        return beginsWithSortKey != null;
    }

    /**
     * Return the conditions for a SQL WHERE clause for a PreparedStatement.
     */
    public String getSQLWhereClause() {
        String partitionKeyName = useIndex
                ? this.partitionKeyPKCol.getName().getString().substring(1)
                : CommonServiceUtils.getEscapedArgument(this.partitionKeyName);
        String sortKeyName = "";
        if (hasSortKey()) {
            sortKeyName = useIndex
                    ? this.sortKeyPKCol.getName().getString().substring(1)
                    : CommonServiceUtils.getEscapedArgument(this.sortKeyName);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(partitionKeyName);
        sb.append(" = ? ");
        // PK1 = ? (AND PK2 op val1 [val2])
        if (hasSortKey()) {
            sb.append(" AND ");

            if (hasBeginsWith()) {
                if (this.pkCols.get(1).getDataType() instanceof PVarchar) {
                    sb.append(" SUBSTR( " + sortKeyName + ", 0, ?) = ? ");
                } else if (this.pkCols.get(1).getDataType() instanceof PVarbinaryEncoded) {
                    sb.append(" SUBBINARY( " + sortKeyName + ", 0, ?) = ?");
                }
            } else {
                sb.append(sortKeyName + " ");
                sb.append(sortKeyOperator);
                sb.append(" ? ");
                if (hasBetween()) {
                    sb.append(" AND ? ");
                }
            }
        }
        return sb.toString();
    }

    /**
     * Parse the provided KeyConditionExpression into individual components
     * using BsonExpressionParser and Phoenix ParseNode tree.
     *
     * Handles combinations like:
     * 1. pk = :v1
     * 2. pk = :v1 AND begins_with(sk, :v2)
     * 3. pk = :v1 AND sk BETWEEN :v3 AND :v4
     * 4. pk = :v1 AND sk = :v2 (and other comparison operators)
     * 5. sk = :v2 (and other comparison operators) AND pk = :v1
     * 6. begins_with(sk, :v2) AND  pk = :v1
     */
    private void parse() {
        BsonExpressionParser bsonExpressionParser = new BsonExpressionParser(keyCondExpr);
        ParseNode parseNode;
        try {
            parseNode = bsonExpressionParser.parseExpression();
            LOGGER.trace("Parsing KeyConditionExpression: {} -> ParseNode type: {}",
                        keyCondExpr, parseNode.getClass().getSimpleName());

            // Set up PK columns
            this.partitionKeyPKCol = pkCols.get(0);
            this.sortKeyPKCol = (pkCols.size() == 2) ? pkCols.get(1) : null;

            if (parseNode instanceof AndParseNode) {
                List<ParseNode> children = parseNode.getChildren();
                String partitionKeyColName = CommonServiceUtils.getKeyNameFromBsonValueFunc(
                        partitionKeyPKCol.getName().getString());

                // Process each condition
                for (ParseNode child : children) {
                    if (child instanceof ComparisonParseNode) {
                        processComparisonExpression((ComparisonParseNode) child, partitionKeyColName);
                    } else {
                        // Non-comparison nodes (BETWEEN, begins_with) are always sort key conditions
                        processParseNode(child);
                    }
                }
            }
            else {
                // Single condition - must be partition key with =
                processParseNode(parseNode);
            }
        } catch (SQLException e) {
            throw new RuntimeException(String.format("BsonExpressionParser failed for expression: %s", keyCondExpr), e);
        }
    }

    /**
     * Process a ParseNode and extract key condition information.
     * Based on Phoenix SQLComparisonExpressionUtils approach.
     */
    private void processParseNode(ParseNode parseNode) {
        if (parseNode instanceof ComparisonParseNode) {
            // Handle single comparison: pk = :v1
            String partitionKeyColName = CommonServiceUtils.getKeyNameFromBsonValueFunc(
                    partitionKeyPKCol.getName().getString());
            processComparisonExpression((ComparisonParseNode) parseNode, partitionKeyColName);
        } else if (parseNode instanceof BetweenParseNode) {
            // Handle BETWEEN: sk BETWEEN :v1 AND :v2
            processBetweenExpression((BetweenParseNode) parseNode);
        } else if (parseNode instanceof DocumentFieldBeginsWithParseNode) {
            // Handle document field begins_with
            processDocumentBeginsWithExpression(parseNode);
        }
        else {
          throw new RuntimeException(String.format("Invalid KeyConditionExpression: "
              + "%s", parseNode.getClass().getSimpleName()));
        }
    }

    /**
     * Process comparison expressions (=, <, >, <=, >=)
     */
    private void processComparisonExpression(ComparisonParseNode compNode, String partitionKeyColName) {
        List<ParseNode> children = compNode.getChildren();
        ParseNode leftNode = children.get(0);
        ParseNode rightNode = children.get(1);
        String operatorName = compNode.getFilterOp().toString();
        String sqlOperator = mapFilterOpToSqlOperator(operatorName);

        String columnName = extractColumnName(leftNode);
        String resolvedName = resolveAttributeName(columnName);
        String bindVariable = extractBindVariable(rightNode);

        LOGGER.trace("Comparison: {}='{}' {} (SQL: {}) {}='{}'",
                    leftNode.getClass().getSimpleName(), columnName,
                    operatorName, sqlOperator,
                    rightNode.getClass().getSimpleName(), bindVariable);

        // Identify partition key condition:
        // 1. Must use = operator
        // 2. Column matches partition key from schema
        if (sqlOperator.equals("=") && resolvedName.equals(partitionKeyColName)) {
            this.partitionKeyName = resolvedName;
            this.partitionValue = bindVariable;
            LOGGER.trace("Found partition key: {} = {}", this.partitionKeyName, this.partitionValue);
        } else {
            // Any other condition is sort key
            this.sortKeyName = resolvedName;
            this.sortKeyOperator = sqlOperator;
            this.sortKeyValue1 = bindVariable;
            LOGGER.trace("Found sort key: {} {} {}", this.sortKeyName, this.sortKeyOperator, this.sortKeyValue1);
        }
    }

    /**
     * Process BETWEEN expressions
     */
    private void processBetweenExpression(BetweenParseNode betweenNode) {
        ParseNode columnNode = betweenNode.getChildren().get(0);
        ParseNode lowerBoundNode = betweenNode.getChildren().get(1);
        ParseNode upperBoundNode = betweenNode.getChildren().get(2);

        String columnName = extractColumnName(columnNode);
        String lowerValue = extractBindVariable(lowerBoundNode);
        String upperValue = extractBindVariable(upperBoundNode);

        if (columnName != null && lowerValue != null && upperValue != null) {
            this.sortKeyName = resolveAttributeName(columnName);
            this.sortKeyOperator = "BETWEEN";
            this.sortKeyValue1 = lowerValue;
            this.sortKeyValue2 = upperValue;
            LOGGER.trace("Sort key: {} BETWEEN {} AND {}",
                        this.sortKeyName, this.sortKeyValue1, this.sortKeyValue2);
        }
    }

    /**
     * Process document field begins_with expressions (different ParseNode type)
     */
    private void processDocumentBeginsWithExpression(ParseNode node) {
        LOGGER.trace("Processing document begins_with: {}", node.getClass().getSimpleName());

        List<ParseNode> children = node.getChildren();

        String columnName = extractColumnName(children.get(0));
        String bindVariable = extractBindVariable(children.get(1));

        String resolvedColumnName = resolveAttributeName(columnName);
        this.beginsWithSortKey = resolvedColumnName;
        this.beginsWithSortKeyVal = bindVariable;
        this.sortKeyName = resolvedColumnName;
        LOGGER.trace("Set document begins_with: {} begins_with {}",
                    this.beginsWithSortKey, this.beginsWithSortKeyVal);
    }

    /**
     * Extract column name from a ParseNode.
     * key conditions always use string literals for column names.
     */
    private String extractColumnName(ParseNode node) {
        return (String) ((LiteralParseNode) node).getValue();
    }

    /**
     * Extract bind variable from a ParseNode.
     * key conditions always use string literals starting with ':' for bind variables.
     */
    private String extractBindVariable(ParseNode node) {
        return (String) ((LiteralParseNode) node).getValue();
    }

    /**
     * Map Phoenix FilterOp enum names to SQL operators
     */
    private String mapFilterOpToSqlOperator(String filterOpName) {
        if (filterOpName == null) {
            return "=";
        }

        switch (filterOpName.toUpperCase()) {
            case "EQUAL":
                return "=";
            case "GREATER":
                return ">";
            case "GREATER_OR_EQUAL":
                return ">=";
            case "LESS":
                return "<";
            case "LESS_OR_EQUAL":
                return "<=";
            default:
                throw new RuntimeException(String.format("Invalid operator used in "
                    + "KeyConditionExpression: %s", filterOpName.toUpperCase()));
        }
    }

    /**
     * Resolve attribute name using expression attribute names map
     */
    private String resolveAttributeName(String name) {
      return exprAttrNames.getOrDefault(name, name);
    }
}