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

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

/**
 * Utility to convert DynamoDB UpdateExpression into BSON Update Expression.
 */
public class UpdateExpressionDdbToBson {

  private static final String setRegExPattern = "SET\\s+(.+?)(?=\\s+(REMOVE|ADD|DELETE)\\b|$)";
  private static final String removeRegExPattern = "REMOVE\\s+(.+?)(?=\\s+(SET|ADD|DELETE)\\b|$)";
  private static final String addRegExPattern = "ADD\\s+(.+?)(?=\\s+(SET|REMOVE|DELETE)\\b|$)";
  private static final String deleteRegExPattern = "DELETE\\s+(.+?)(?=\\s+(SET|REMOVE|ADD)\\b|$)";
  private static final String ifNotExistsPattern = "if_not_exists\\s*\\(\\s*([^,]+)\\s*,\\s*([^)]+)\\s*\\)";
  private static final Pattern SET_PATTERN = Pattern.compile(setRegExPattern);
  private static final Pattern REMOVE_PATTERN = Pattern.compile(removeRegExPattern);
  private static final Pattern ADD_PATTERN = Pattern.compile(addRegExPattern);
  private static final Pattern DELETE_PATTERN = Pattern.compile(deleteRegExPattern);
  private static final Pattern IF_NOT_EXISTS_PATTERN = Pattern.compile(ifNotExistsPattern);

  public static BsonDocument getBsonDocumentForUpdateExpression(
      final String updateExpression,
      final BsonDocument comparisonValue) {

    String setString = "";
    String removeString = "";
    String addString = "";
    String deleteString = "";

    Matcher matcher = SET_PATTERN.matcher(updateExpression);
    if (matcher.find()) {
      setString = matcher.group(1).trim();
    }

    matcher = REMOVE_PATTERN.matcher(updateExpression);
    if (matcher.find()) {
      removeString = matcher.group(1).trim();
    }

    matcher = ADD_PATTERN.matcher(updateExpression);
    if (matcher.find()) {
      addString = matcher.group(1).trim();
    }

    matcher = DELETE_PATTERN.matcher(updateExpression);
    if (matcher.find()) {
      deleteString = matcher.group(1).trim();
    }

    BsonDocument bsonDocument = new BsonDocument();
    if (!setString.isEmpty()) {
      BsonDocument setBsonDoc = new BsonDocument();
      // split by comma only if comma is not within ()
      String[] setExpressions = setString.split(",(?![^()]*+\\))");
      for (int i = 0; i < setExpressions.length; i++) {
        String setExpression = setExpressions[i].trim();
        String[] keyVal = setExpression.split("\\s*=\\s*");
        if (keyVal.length == 2) {
          String attributeKey = keyVal[0].trim();
          String attributeVal = keyVal[1].trim();
          if (attributeVal.contains("+") || attributeVal.contains("-")) {
            setBsonDoc.put(attributeKey, getArithmeticDoc(attributeVal, comparisonValue));
          } else if (attributeVal.startsWith("if_not_exists")) {
            setBsonDoc.put(attributeKey, getIfNotExistsDoc(attributeVal, comparisonValue));
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

  private static BsonDocument getArithmeticDoc(String expr, BsonDocument comparisonValue) {
      BsonDocument arithmeticDoc = new BsonDocument();
      String op;
      String[] operands;
      if (expr.contains("+")) {
          op = "$ADD";
          operands = expr.split("\\+");
      } else if (expr.contains("-")) {
          op = "$SUBTRACT";
          operands = expr.split("-");
      } else {
          throw new IllegalArgumentException("Unsupported arithmetic operator for SET");
      }
      BsonArray bsonOperands = new BsonArray();
      for (String operand : operands) {
          operand = operand.trim();
          if (operand.startsWith("if_not_exists")) {
              bsonOperands.add(getIfNotExistsDoc(operand, comparisonValue));
          } else if (operand.startsWith(":") || operand.startsWith("$") || operand.startsWith("#")) {
              BsonValue bsonValue = comparisonValue.get(operand);
              if (!bsonValue.isNumber() && !bsonValue.isDecimal128()) {
                  throw new IllegalArgumentException(
                          "Operand " + operand + " is not provided as number type");
              }
              bsonOperands.add(bsonValue);
          } else {
              bsonOperands.add(new BsonString(operand));
          }
      }
      arithmeticDoc.put(op, bsonOperands);
      return arithmeticDoc;
  }

  private static BsonDocument getIfNotExistsDoc(String expr, BsonDocument comparisonValue) {
      Matcher m = IF_NOT_EXISTS_PATTERN.matcher(expr);
      if (m.find()) {
          String ifNotExistsPath = m.group(1).trim();
          String fallBackValue = m.group(2).trim();
          BsonValue fallBackValueBson = comparisonValue.get(fallBackValue);
          BsonDocument fallBackDoc = new BsonDocument();
          fallBackDoc.put(ifNotExistsPath, fallBackValueBson);
          BsonDocument ifNotExistsDoc = new BsonDocument();
          ifNotExistsDoc.put("$IF_NOT_EXISTS", fallBackDoc);
          return ifNotExistsDoc;
      } else {
          throw new RuntimeException("Invalid format for if_not_exists(path, value)");
      }
  }
}
