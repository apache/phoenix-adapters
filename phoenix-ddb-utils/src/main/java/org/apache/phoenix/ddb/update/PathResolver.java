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

import org.apache.phoenix.ddb.update.UpdateExpressionParser.PathContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.PathStepContext;
import org.apache.phoenix.ddb.update.UpdateExpressionParser.PathSuffixContext;

/**
 * Builds the dotted-and-bracketed BSON key string from a parsed {@code path} node, expanding
 * {@code #alias} tokens against ExpressionAttributeNames. Also exposes the overlap check used
 * by the visitor to enforce DDB's "two document paths overlap" rule across all clauses.
 */
final class PathResolver {

    private final Map<String, String> aliases;

    PathResolver(Map<String, String> aliases) {
        this.aliases = aliases;
    }

    String resolve(PathContext ctx) {
        StringBuilder sb = new StringBuilder();
        sb.append(resolveStep(ctx.pathStep()));
        for (PathSuffixContext suffix : ctx.pathSuffix()) {
            if (suffix.pathStep() != null) {
                sb.append('.').append(resolveStep(suffix.pathStep()));
            } else {
                sb.append('[').append(suffix.INT().getText()).append(']');
            }
        }
        return sb.toString();
    }

    private String resolveStep(PathStepContext step) {
        if (step.ALIAS() != null) {
            String alias = step.ALIAS().getText();
            String resolved = aliases == null ? null : aliases.get(alias);
            if (resolved == null) {
                throw new UpdateExpressionSyntaxException(
                        "Expression attribute name " + alias
                                + " not declared in ExpressionAttributeNames");
            }
            return resolved;
        }
        return step.NAME().getText();
    }

    /** True if {@code a} == {@code b} or one is a strict prefix of the other on a path boundary. */
    static boolean pathsOverlap(String a, String b) {
        if (a.equals(b)) {
            return true;
        }
        return isStrictPrefix(a, b) || isStrictPrefix(b, a);
    }

    private static boolean isStrictPrefix(String shorter, String longer) {
        if (longer.length() <= shorter.length() || !longer.startsWith(shorter)) {
            return false;
        }
        char boundary = longer.charAt(shorter.length());
        return boundary == '.' || boundary == '[';
    }
}
