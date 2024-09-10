package org.apache.phoenix.ddb.utils;

import java.util.HashMap;
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
    private String partitionKeyName;
    private String partitionValue;
    private String beginsWithSortKey;
    private String beginsWithSortKeyVal;
    private String sortKeyName;
    private String sortKeyOperator;
    private String sortKeyValue1;
    private String sortKeyValue2;

    public KeyConditionsHolder(String keyCondExpr, Map<String, String> exprAttrNames) {
        this.keyCondExpr = keyCondExpr;
        this.exprAttrNames = (exprAttrNames != null) ? exprAttrNames : new HashMap<>();
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
        StringBuilder sb = new StringBuilder();
        sb.append(getPartitionKeyName());
        sb.append(" = ? ");
        // PK1 = ? (AND PK2 op val1 [val2])
        if (sortKeyName != null) {
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
    }
}
