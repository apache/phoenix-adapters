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

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonString;
import org.bson.BsonValue;

import org.apache.phoenix.ddb.update.UpdateExpressionParser.AddActionContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.AddClauseContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.DeleteActionContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.DeleteClauseContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.IfNotExistsContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.IfNotExistsValuePathContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.IfNotExistsValuePlaceholderContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.ListAppendContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.ListAppendOperandContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.ListAppendOperandIfNotExistsContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.ListAppendOperandPathContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.ListAppendOperandPlaceholderContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.OperandContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.OperandIfNotExistsContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.OperandListAppendContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.OperandPathContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.OperandPlaceholderContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.RemoveActionContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.RemoveClauseContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.SetActionContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.SetClauseContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.SetValueAddContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.SetValueOperandContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.SetValueSubtractContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.UpdateExpressionContext;

/**
 * Walks the parsed UpdateExpression and emits the BSON document consumed by phoenix-1's
 * {@code UpdateExpressionUtils}: {@code {"$SET": ..., "$UNSET": ..., "$ADD": ..., "$DELETE_FROM_SET": ...}}.
 * Enforces the semantic rules the grammar can't (each clause keyword once, path overlap,
 * undeclared {@code :placeholder}, list_append operand types, arithmetic operand numeric).
 */
final class BsonEmittingVisitor {

    private enum Clause { SET, REMOVE, ADD, DELETE }

    private static final BsonDocument EMPTY_VALUES = new BsonDocument();

    private final BsonDocument values;
    private final PathResolver pathResolver;

    private final BsonDocument result = new BsonDocument();
    private final EnumSet<Clause> seenClauses = EnumSet.noneOf(Clause.class);
    private final Set<String> allPaths = new HashSet<>();

    BsonEmittingVisitor(BsonDocument values, Map<String, String> aliases) {
        this.values = values == null ? EMPTY_VALUES : values;
        this.pathResolver = new PathResolver(aliases);
    }

    BsonDocument emit(UpdateExpressionContext ctx) {
        for (org.antlr.v4.runtime.tree.ParseTree child : ctx.children) {
            if (child instanceof SetClauseContext) {
                visitSet((SetClauseContext) child);
            } else if (child instanceof RemoveClauseContext) {
                visitRemove((RemoveClauseContext) child);
            } else if (child instanceof AddClauseContext) {
                visitAdd((AddClauseContext) child);
            } else if (child instanceof DeleteClauseContext) {
                visitDelete((DeleteClauseContext) child);
            }
        }
        return result;
    }

    // --- clauses ---

    private void visitSet(SetClauseContext ctx) {
        markClauseSeen(Clause.SET);
        BsonDocument setDoc = subDoc("$SET");
        for (SetActionContext action : ctx.setAction()) {
            String key = pathResolver.resolve(action.path());
            recordPath(key, Clause.SET);
            setDoc.put(key, evaluateSetValue(action.setValue()));
        }
    }

    private void visitRemove(RemoveClauseContext ctx) {
        markClauseSeen(Clause.REMOVE);
        BsonDocument unsetDoc = subDoc("$UNSET");
        for (RemoveActionContext action : ctx.removeAction()) {
            String key = pathResolver.resolve(action.path());
            recordPath(key, Clause.REMOVE);
            unsetDoc.put(key, BsonNull.VALUE);
        }
    }

    private void visitAdd(AddClauseContext ctx) {
        markClauseSeen(Clause.ADD);
        BsonDocument addDoc = subDoc("$ADD");
        for (AddActionContext action : ctx.addAction()) {
            String key = pathResolver.resolve(action.path());
            recordPath(key, Clause.ADD);
            addDoc.put(key, resolvePlaceholderToken(action.placeholder().getText()));
        }
    }

    private void visitDelete(DeleteClauseContext ctx) {
        markClauseSeen(Clause.DELETE);
        BsonDocument delDoc = subDoc("$DELETE_FROM_SET");
        for (DeleteActionContext action : ctx.deleteAction()) {
            String key = pathResolver.resolve(action.path());
            recordPath(key, Clause.DELETE);
            delDoc.put(key, resolvePlaceholderToken(action.placeholder().getText()));
        }
    }

    // --- SET value (operand | operand + operand | operand - operand) ---

    private BsonValue evaluateSetValue(
            UpdateExpressionParser.SetValueContext ctx) {
        if (ctx instanceof SetValueOperandContext) {
            return evaluateOperand(((SetValueOperandContext) ctx).operand());
        }
        if (ctx instanceof SetValueAddContext) {
            SetValueAddContext add = (SetValueAddContext) ctx;
            return arithmeticDoc("$ADD", add.operand(0), add.operand(1));
        }
        if (ctx instanceof SetValueSubtractContext) {
            SetValueSubtractContext sub = (SetValueSubtractContext) ctx;
            return arithmeticDoc("$SUBTRACT", sub.operand(0), sub.operand(1));
        }
        throw new UpdateExpressionSyntaxException("Unsupported SET value: " + ctx.getText());
    }

    private BsonValue arithmeticDoc(String op, OperandContext left, OperandContext right) {
        BsonArray operands = new BsonArray();
        operands.add(arithmeticOperand(left));
        operands.add(arithmeticOperand(right));
        BsonDocument doc = new BsonDocument();
        doc.put(op, operands);
        return doc;
    }

    private BsonValue arithmeticOperand(OperandContext ctx) {
        if (ctx instanceof OperandPathContext) {
            return new BsonString(pathResolver.resolve(((OperandPathContext) ctx).path()));
        }
        if (ctx instanceof OperandPlaceholderContext) {
            String token = ((OperandPlaceholderContext) ctx).placeholder().getText();
            BsonValue v = resolvePlaceholderToken(token);
            if (!v.isNumber() && !v.isDecimal128()) {
                throw new UpdateExpressionSyntaxException(
                        "Operand " + token + " is not provided as number type");
            }
            return v;
        }
        if (ctx instanceof OperandIfNotExistsContext) {
            IfNotExistsContext inex = ((OperandIfNotExistsContext) ctx).ifNotExists();
            // If the fallback is a placeholder, anchor the numeric type-check at the parser
            // layer so the 400 points at the actual offending token (mirrors the bare-placeholder
            // branch above). Path fallbacks are resolved at runtime by the BSON evaluator.
            if (inex.ifNotExistsValue() instanceof IfNotExistsValuePlaceholderContext) {
                String token = ((IfNotExistsValuePlaceholderContext) inex.ifNotExistsValue())
                        .placeholder().getText();
                BsonValue v = resolvePlaceholderToken(token);
                if (!v.isNumber() && !v.isDecimal128()) {
                    throw new UpdateExpressionSyntaxException(
                            "Operand " + token + " is not provided as number type");
                }
            }
            return ifNotExistsDoc(inex);
        }
        throw new UpdateExpressionSyntaxException(
                "list_append is not a valid operand for arithmetic: " + ctx.getText());
    }

    // --- generic operand for plain SET (no arithmetic) ---

    private BsonValue evaluateOperand(OperandContext ctx) {
        if (ctx instanceof OperandPathContext) {
            return new BsonString(pathResolver.resolve(((OperandPathContext) ctx).path()));
        }
        if (ctx instanceof OperandPlaceholderContext) {
            return resolvePlaceholderToken(
                    ((OperandPlaceholderContext) ctx).placeholder().getText());
        }
        if (ctx instanceof OperandIfNotExistsContext) {
            return ifNotExistsDoc(((OperandIfNotExistsContext) ctx).ifNotExists());
        }
        if (ctx instanceof OperandListAppendContext) {
            return listAppendDoc(((OperandListAppendContext) ctx).listAppend());
        }
        throw new UpdateExpressionSyntaxException("Unsupported operand: " + ctx.getText());
    }

    // --- functions ---

    private BsonDocument ifNotExistsDoc(IfNotExistsContext ctx) {
        String fieldKey = pathResolver.resolve(ctx.path());
        BsonValue fallback;
        if (ctx.ifNotExistsValue() instanceof IfNotExistsValuePathContext) {
            fallback = new BsonString(pathResolver.resolve(
                    ((IfNotExistsValuePathContext) ctx.ifNotExistsValue()).path()));
        } else {
            String token = ((IfNotExistsValuePlaceholderContext) ctx.ifNotExistsValue())
                    .placeholder().getText();
            fallback = resolvePlaceholderToken(token);
        }
        BsonDocument inner = new BsonDocument();
        inner.put(fieldKey, fallback);
        BsonDocument doc = new BsonDocument();
        doc.put("$IF_NOT_EXISTS", inner);
        return doc;
    }

    private BsonDocument listAppendDoc(ListAppendContext ctx) {
        BsonArray operands = new BsonArray();
        operands.add(listAppendOperand(ctx.listAppendOperand(0)));
        operands.add(listAppendOperand(ctx.listAppendOperand(1)));
        BsonDocument doc = new BsonDocument();
        doc.put("$LIST_APPEND", operands);
        return doc;
    }

    private BsonValue listAppendOperand(ListAppendOperandContext ctx) {
        if (ctx instanceof ListAppendOperandPathContext) {
            return new BsonString(pathResolver.resolve(
                    ((ListAppendOperandPathContext) ctx).path()));
        }
        if (ctx instanceof ListAppendOperandPlaceholderContext) {
            String token = ((ListAppendOperandPlaceholderContext) ctx).placeholder().getText();
            BsonValue v = resolvePlaceholderToken(token);
            if (!v.isArray()) {
                throw new UpdateExpressionSyntaxException(
                        "Operand " + token + " for list_append must resolve to a List type");
            }
            return v;
        }
        // ListAppendOperandIfNotExistsContext: validate that the fallback is list-typed.
        BsonDocument doc = ifNotExistsDoc(
                ((ListAppendOperandIfNotExistsContext) ctx).ifNotExists());
        BsonValue fallback = doc.getDocument("$IF_NOT_EXISTS").values().iterator().next();
        if (fallback != null && !fallback.isNull() && !fallback.isArray()) {
            throw new UpdateExpressionSyntaxException(
                    "if_not_exists fallback inside list_append must resolve to a List type"
                            + " but got: " + ctx.getText());
        }
        return doc;
    }

    // --- helpers ---

    private BsonValue resolvePlaceholderToken(String token) {
        BsonValue v = values.get(token);
        if (v == null) {
            throw new UpdateExpressionSyntaxException(
                    "Expression attribute value " + token
                            + " not declared in ExpressionAttributeValues");
        }
        return v;
    }

    private void markClauseSeen(Clause clause) {
        if (!seenClauses.add(clause)) {
            throw new UpdateExpressionSyntaxException(
                    "Clause keyword " + clause + " appears more than once in UpdateExpression");
        }
    }

    private void recordPath(String path, Clause clause) {
        for (String existing : allPaths) {
            if (PathResolver.pathsOverlap(existing, path)) {
                throw new UpdateExpressionSyntaxException(
                        "Two document paths overlap in UpdateExpression "
                                + clause + " clause: '" + existing + "' and '" + path + "'");
            }
        }
        allPaths.add(path);
    }

    private BsonDocument subDoc(String key) {
        // markClauseSeen guarantees each clause is visited at most once, so result.get(key)
        // is null at this call site.
        BsonDocument fresh = new BsonDocument();
        result.put(key, fresh);
        return fresh;
    }
}
