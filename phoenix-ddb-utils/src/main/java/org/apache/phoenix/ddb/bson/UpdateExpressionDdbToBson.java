/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.phoenix.ddb.bson;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonNumber;
import org.bson.BsonString;
import org.bson.BsonValue;

import org.apache.phoenix.expression.util.bson.UpdateExpressionUtils;

/**
 * Utility to convert DynamoDB UpdateExpression into BSON Update Expression.
 */
public class UpdateExpressionDdbToBson {

  public static BsonDocument getBsonDocumentForUpdateExpression(
      final String updateExpression,
      final BsonDocument comparisonValue) {

    String setRegExPattern = "SET\\s+(.+?)(?=\\s+(REMOVE|ADD|DELETE)\\b|$)";
    String removeRegExPattern = "REMOVE\\s+(.+?)(?=\\s+(SET|ADD|DELETE)\\b|$)";
    String addRegExPattern = "ADD\\s+(.+?)(?=\\s+(SET|REMOVE|DELETE)\\b|$)";
    String deleteRegExPattern = "DELETE\\s+(.+?)(?=\\s+(SET|REMOVE|ADD)\\b|$)";

    String setString = "";
    String removeString = "";
    String addString = "";
    String deleteString = "";

    Pattern pattern = Pattern.compile(setRegExPattern);
    Matcher matcher = pattern.matcher(updateExpression);
    if (matcher.find()) {
      setString = matcher.group(1).trim();
    }

    pattern = Pattern.compile(removeRegExPattern);
    matcher = pattern.matcher(updateExpression);
    if (matcher.find()) {
      removeString = matcher.group(1).trim();
    }

    pattern = Pattern.compile(addRegExPattern);
    matcher = pattern.matcher(updateExpression);
    if (matcher.find()) {
      addString = matcher.group(1).trim();
    }

    pattern = Pattern.compile(deleteRegExPattern);
    matcher = pattern.matcher(updateExpression);
    if (matcher.find()) {
      deleteString = matcher.group(1).trim();
    }

    BsonDocument bsonDocument = new BsonDocument();
    if (!setString.isEmpty()) {
      BsonDocument setBsonDoc = new BsonDocument();
      String[] setExpressions = setString.split(",");
      for (int i = 0; i < setExpressions.length; i++) {
        String setExpression = setExpressions[i].trim();
        String[] keyVal = setExpression.split("\\s*=\\s*");
        if (keyVal.length == 2) {
          String attributeKey = keyVal[0].trim();
          String attributeVal = keyVal[1].trim();
          if (attributeVal.contains("+") || attributeVal.contains("-")) {
            setBsonDoc.put(attributeKey, getArithmeticExpVal(attributeVal, comparisonValue));
          } else if (attributeVal.startsWith("if_not_exists")) {
            // attributeVal = if_not_exists ( path
            String ifNotExistsPath = attributeVal.split("\\(")[1].trim();
            // next setExpression = value)
            String fallBackValue = setExpressions[i+1].split("\\)")[0].trim();
            BsonValue fallBackValueBson = comparisonValue.get(fallBackValue);
            BsonDocument fallBackDoc = new BsonDocument();
            fallBackDoc.put(ifNotExistsPath, fallBackValueBson);
            BsonDocument ifNotExistsDoc = new BsonDocument();
            ifNotExistsDoc.put("$IF_NOT_EXISTS", fallBackDoc);
            setBsonDoc.put(attributeKey, ifNotExistsDoc);
            i++;
          } else {
            setBsonDoc.put(attributeKey, comparisonValue.get(attributeVal));
          }
        }
        else {
          throw new RuntimeException(
              "SET Expression " + setString + " does not include key value pairs separated by =");
        }
      }
      bsonDocument.put("$SET", setBsonDoc);
    }
    if (!removeString.isEmpty()) {
      String[] removeExpressions = removeString.split(",");
      BsonDocument unsetBsonDoc = new BsonDocument();
      for (String removeAttribute : removeExpressions) {
        String attributeKey = removeAttribute.trim();
        unsetBsonDoc.put(attributeKey, new BsonNull());
      }
      bsonDocument.put("$UNSET", unsetBsonDoc);
    }
    if (!addString.isEmpty()) {
      String[] addExpressions = addString.split(",");
      BsonDocument addBsonDoc = new BsonDocument();
      for (String addExpression : addExpressions) {
        addExpression = addExpression.trim();
        String[] keyVal = addExpression.split("\\s+");
        if (keyVal.length == 2) {
          String attributeKey = keyVal[0].trim();
          String attributeVal = keyVal[1].trim();
          addBsonDoc.put(attributeKey, comparisonValue.get(attributeVal));
        } else {
          throw new RuntimeException("ADD Expression " + addString
              + " does not include key value pairs separated by space");
        }
      }
      bsonDocument.put("$ADD", addBsonDoc);
    }
    if (!deleteString.isEmpty()) {
      BsonDocument delBsonDoc = new BsonDocument();
      String[] deleteExpressions = deleteString.split(",");
      for (String deleteExpression : deleteExpressions) {
        deleteExpression = deleteExpression.trim();
        String[] keyVal = deleteExpression.split("\\s+");
        if (keyVal.length == 2) {
          String attributeKey = keyVal[0].trim();
          String attributeVal = keyVal[1].trim();
          delBsonDoc.put(attributeKey, comparisonValue.get(attributeVal));
        } else {
          throw new RuntimeException("DELETE Expression " + deleteString
              + " does not include key value pairs separated by space");
        }
      }
      bsonDocument.put("$DELETE_FROM_SET", delBsonDoc);
    }
    return bsonDocument;
  }

  private static BsonString getArithmeticExpVal(String attributeVal,
      BsonDocument comparisonValuesDocument) {
    String[] tokens = attributeVal.split("\\s+");
    //      Pattern pattern = Pattern.compile(":?[a-zA-Z0-9]+");
    Pattern pattern = Pattern.compile("[#:$]?[^\\s\\n]+");
    Number newNum = null;
    StringBuilder val = new StringBuilder();
    for (String token : tokens) {
      if (token.equals("+")) {
        val.append(" + ");
        continue;
      } else if (token.equals("-")) {
        val.append(" - ");
        continue;
      }
      Matcher matcher = pattern.matcher(token);
      if (matcher.find()) {
        String operand = matcher.group();
        if (operand.startsWith(":") || operand.startsWith("$") || operand.startsWith("#")) {
          BsonValue bsonValue = comparisonValuesDocument.get(operand);
          if (!bsonValue.isNumber() && !bsonValue.isDecimal128()) {
            throw new IllegalArgumentException(
                "Operand " + operand + " is not provided as number type");
          }
          Number numVal = UpdateExpressionUtils.getNumberFromBsonNumber((BsonNumber) bsonValue);
          val.append(numVal);
        } else {
          val.append(operand);
        }
      }
    }
    return new BsonString(val.toString());
  }

}
