package org.apache.phoenix.ddb.utils;

import org.apache.phoenix.schema.PColumn;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Helper class to parse KeyConditionExpression provided in a DynamoDB request:
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

    private final String KEY_REGEX
            = "([#\\w]+)\\s*=\\s*(:?\\w+)(?:\\s+AND\\s+(?:begins_with\\s*\\(\\s*([#\\w]+)\\s*,\\s*(:?\\w+)\\s*\\)|([#\\w]+)\\s*(=|>|<|<=|>=|BETWEEN)\\s*(:?\\w+)(?:\\s+AND\\s*(:?\\w+))?))?";

    private final Pattern KEY_PATTERN = Pattern.compile(KEY_REGEX);
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

    public KeyConditionsHolder(String keyCondExpr, Map<String, String> exprAttrNames) {
        this(keyCondExpr, exprAttrNames, null, false);
    }

    public KeyConditionsHolder(String keyCondExpr, Map<String, String> exprAttrNames,
                               List<PColumn> pkCols, boolean useIndex) {
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
            sb.append(sortKeyName + " ");
            if (hasBeginsWith()) {
                sb.append(" LIKE ?");
            } else {
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
     * Parse the provided KeyConditionExpression into individual components.
     */
    private void parse() {
        Matcher matcher = KEY_PATTERN.matcher(keyCondExpr);
        if (!matcher.find()) {
            throw new RuntimeException("Unable to parse Key Condition Expression: " + keyCondExpr);
        }
        String partitionKey = matcher.group(1);
        this.partitionValue = matcher.group(2);
        this.beginsWithSortKey = matcher.group(3);
        this.beginsWithSortKeyVal = matcher.group(4);
        String sortKey = matcher.group(5);
        this.sortKeyOperator = matcher.group(6);
        this.sortKeyValue1 = matcher.group(7);
        this.sortKeyValue2 = matcher.group(8);

        //partition key name
        this.partitionKeyName = exprAttrNames.getOrDefault(partitionKey, partitionKey);

        // sort key name
        this.sortKeyName = (sortKey != null)
                ? exprAttrNames.getOrDefault(sortKey, sortKey)
                : (beginsWithSortKey != null)
                ? exprAttrNames.getOrDefault(beginsWithSortKey, beginsWithSortKey)
                : null;

        if(pkCols != null) {
            this.partitionKeyPKCol = pkCols.get(0);
            this.sortKeyPKCol = (pkCols.size()==2) ? pkCols.get(1) : null;
        }
    }
}
