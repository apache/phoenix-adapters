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
package org.apache.phoenix.ddb;

import java.util.Arrays;
import java.util.Collection;

import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/**
 * Segment scan tests - Part 2.
 */
@RunWith(Parameterized.class)
public class SegmentScan2IT extends SegmentScanIT {

    public SegmentScan2IT(String hashKeyName, ScalarAttributeType hashKeyType, String sortKeyName,
            ScalarAttributeType sortKeyType, boolean useFilter) {
        super(hashKeyName, hashKeyType, sortKeyName, sortKeyType, useFilter);
    }

    @Parameterized.Parameters(name = "Hash_{1}_Sort_{3}_Filter_{4}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                // Binary Hash Key combinations
                {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.S, false},
                {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.N, true},
                {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.B, false},

                // Additional combinations
                {"pk", ScalarAttributeType.S, null, null, true},
                {"pk", ScalarAttributeType.N, null, null, false},
                {"pk", ScalarAttributeType.B, null, null, true},
                {"pk", ScalarAttributeType.B, "sk", ScalarAttributeType.S, false},
                {"pk", ScalarAttributeType.S, "sk", ScalarAttributeType.N, true},
                {"pk", ScalarAttributeType.N, "sk", ScalarAttributeType.B, false}
        });
    }
}
