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

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.text.ParseException;

import org.bson.BsonDecimal128;
import org.bson.BsonDouble;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonNumber;
import org.bson.types.Decimal128;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class BsonNumberConversionUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(BsonNumberConversionUtil.class);

    static Number getNumberFromBsonNumber(BsonNumber bsonNumber) {
        if (bsonNumber instanceof BsonInt32) {
            return ((BsonInt32) bsonNumber).getValue();
        } else if (bsonNumber instanceof BsonInt64) {
            return ((BsonInt64) bsonNumber).getValue();
        } else if (bsonNumber instanceof BsonDouble) {
            return ((BsonDouble) bsonNumber).getValue();
        } else if (bsonNumber instanceof BsonDecimal128) {
            return ((BsonDecimal128) bsonNumber).getValue().bigDecimalValue();
        } else {
            LOGGER.error("Unsupported BsonNumber type: {}", bsonNumber);
            throw new IllegalArgumentException("Unsupported BsonNumber type: " + bsonNumber);
        }
    }

    /**
     * Convert the given Number to String.
     *
     * @param number The Number object.
     * @return String represented number value.
     */
    static String numberToString(Number number) {
        if (number instanceof Integer || number instanceof Short || number instanceof Byte) {
            return Integer.toString(number.intValue());
        } else if (number instanceof Long) {
            return Long.toString(number.longValue());
        } else if (number instanceof Double) {
            return Double.toString(number.doubleValue());
        } else if (number instanceof Float) {
            return Float.toString(number.floatValue());
        }
        throw new RuntimeException("Number type is not known for number: " + number);
    }

    /**
     * Convert the given String to Number.
     *
     * @param number The String represented numeric value.
     * @return The Number object.
     */
    static Number stringToNumber(String number) {
        try {
            return Integer.parseInt(number);
        } catch (NumberFormatException e) {
            // no-op
        }
        try {
            return Long.parseLong(number);
        } catch (NumberFormatException e) {
            // no-op
        }
        try {
            return Double.parseDouble(number);
        } catch (NumberFormatException e) {
            // no-op
        }
        try {
            return NumberFormat.getInstance().parse(number);
        } catch (ParseException e) {
            LOGGER.error("Unable to parse number string {} to number", number);
            return null;
        }
    }

    static BsonNumber getBsonNumberFromNumber(String strNum) {
        Number number = stringToNumber(strNum);
        BsonNumber bsonNumber;
        if (number instanceof Integer || number instanceof Short || number instanceof Byte) {
            bsonNumber = new BsonInt32(number.intValue());
        } else if (number instanceof Long) {
            bsonNumber = new BsonInt64(number.longValue());
        } else if (number instanceof Double || number instanceof Float) {
            bsonNumber = new BsonDouble(number.doubleValue());
        } else if (number instanceof BigDecimal) {
            bsonNumber = new BsonDecimal128(new Decimal128((BigDecimal) number));
        } else {
            LOGGER.error("Error while converting {} into BsonNumber", strNum);
            throw new IllegalArgumentException("Unsupported Number type: " + number);
        }
        return bsonNumber;
    }
}
