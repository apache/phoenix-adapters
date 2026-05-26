/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.phoenix.ddb.update;

import java.util.Map;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.bson.BsonDocument;

/**
 * ANTLR-driven entry point: translates a DynamoDB UpdateExpression string into the BSON
 * document shape consumed by {@code UpdateExpressionUtils} server-side. ExpressionAttributeNames
 * and ExpressionAttributeValues are resolved at AST-walk time so {@code #alias} tokens never
 * collide with clause keywords.
 */
public final class UpdateExpressionToBson {

    private UpdateExpressionToBson() {
    }

    public static BsonDocument toBsonUpdateDocument(String updateExpression,
                                                    BsonDocument expressionAttributeValues) {
        return toBsonUpdateDocument(updateExpression, expressionAttributeValues, null);
    }

    public static BsonDocument toBsonUpdateDocument(String updateExpression,
                                                    BsonDocument expressionAttributeValues,
                                                    Map<String, String> expressionAttributeNames) {
        if (updateExpression == null || updateExpression.trim().isEmpty()) {
            throw new UpdateExpressionSyntaxException(
                    "UpdateExpression must contain at least one of SET/REMOVE/ADD/DELETE");
        }

        UpdateExpressionLexer lexer = new UpdateExpressionLexer(CharStreams.fromString(updateExpression));
        lexer.removeErrorListeners();
        lexer.addErrorListener(ThrowingErrorListener.INSTANCE);

        UpdateExpressionParser parser = new UpdateExpressionParser(new CommonTokenStream(lexer));
        parser.removeErrorListeners();
        // ThrowingErrorListener throws UpdateExpressionSyntaxException directly with position
        // info; rely on the default error strategy so the listener actually fires.
        parser.addErrorListener(ThrowingErrorListener.INSTANCE);

        return new BsonEmittingVisitor(expressionAttributeValues, expressionAttributeNames)
                .emit(parser.updateExpression());
    }

    private static final class ThrowingErrorListener extends BaseErrorListener {
        static final ThrowingErrorListener INSTANCE = new ThrowingErrorListener();

        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol,
                                int line, int charPositionInLine,
                                String msg, RecognitionException e) {
            throw new UpdateExpressionSyntaxException(
                    "Invalid UpdateExpression at position " + charPositionInLine + ": " + msg);
        }
    }
}
