package org.apache.phoenix.ddb.utils;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import org.apache.phoenix.ddb.bson.DdbAttributesToBsonDocument;
import org.bson.BsonDocument;
import org.bson.BsonString;

import java.util.Map;

public class BsonUtils {

    /**
     * Return string represenation of BSON Condition Expression based on dynamo condition
     * expression, expression attribute names and expression attribute values.
     */
    public static String getBsonConditionExpression(String condExpr,
                                                       Map<String, String> exprAttrNames,
                                                       Map<String, AttributeValue> exprAttrVals) {

        // TODO: remove this when phoenix can support attribute_ prefix
        condExpr = condExpr.replaceAll("attribute_exists", "field_exists");
        condExpr = condExpr.replaceAll("attribute_not_exists", "field_not_exists");

        // plug expression attribute names in $EXPR
        for (String k : exprAttrNames.keySet()) {
            condExpr = condExpr.replaceAll(k, exprAttrNames.get(k));
        }
        // BSON_CONDITION_EXPRESSION
        BsonDocument conditionDoc = new BsonDocument();
        conditionDoc.put("$EXPR", new BsonString(condExpr));
        conditionDoc.put("$VAL", DdbAttributesToBsonDocument.getBsonDocument(exprAttrVals));

        return conditionDoc.toJson();
    }
}
