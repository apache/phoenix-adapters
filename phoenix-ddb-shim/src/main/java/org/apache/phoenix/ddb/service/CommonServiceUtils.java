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

package org.apache.phoenix.ddb.service;

import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;

import org.apache.phoenix.schema.types.PDataType;
import org.apache.phoenix.schema.types.PDecimal;
import org.apache.phoenix.schema.types.PDouble;
import org.apache.phoenix.schema.types.PVarbinaryEncoded;
import org.apache.phoenix.schema.types.PVarchar;

public class CommonServiceUtils {

    public static ScalarAttributeType getScalarAttributeFromPDataType(PDataType<?> pDataType) {
        if (pDataType == PVarchar.INSTANCE) {
            return ScalarAttributeType.S;
        } else if (pDataType == PDouble.INSTANCE || pDataType == PDecimal.INSTANCE) {
            return ScalarAttributeType.N;
        } else if (pDataType == PVarbinaryEncoded.INSTANCE) {
            return ScalarAttributeType.B;
        } else {
            throw new IllegalStateException("Invalid data type: " + pDataType.toString());
        }
    }

    public static String getKeyNameFromBsonValueFunc(String keyName) {
        if (keyName.contains("BSON_VALUE")) {
            keyName = keyName.split("BSON_VALUE")[1].split(",")[1];
            if (keyName.charAt(0) == '\'' && keyName.charAt(keyName.length() - 1) == '\'') {
                StringBuilder sb = new StringBuilder(keyName);
                sb.deleteCharAt(sb.length() - 1);
                sb.deleteCharAt(0);
                keyName = sb.toString();
            }
        } else if (keyName.startsWith(":")) {
            keyName = keyName.split(":")[1];
        }
        return keyName;
    }

}
