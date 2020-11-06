/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.calcite.sqlfunction.datefunction;


import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.sql.Time;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Period;
import java.util.concurrent.TimeUnit;


public class TimestampDiffFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimestampDiffFunction.class,
            "timestampDiff");
    public static TimestampDiffFunction INSTANCE = new TimestampDiffFunction();


    public TimestampDiffFunction() {
        super("TIMESTAMPDIFF", scalarFunction);
    }

    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static Long timestampDiff(String unit, LocalDateTime datetime_expr1, LocalDateTime datetime_expr2) {
        if (unit == null || datetime_expr1 == null || datetime_expr2 == null) {
            return null;
        }
        //MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.
        switch (unit.toUpperCase()) {
            case "MICROSECOND": {
                Duration between = Duration.between(datetime_expr2, datetime_expr1);
                return TimeUnit.NANOSECONDS.toMicros(between.getNano());
            }
            case "SECOND": {
                Duration between = Duration.between(datetime_expr2, datetime_expr1);
                return (long) (between.getSeconds());
            }
            case "MINUTE": {
                Duration between = Duration.between(datetime_expr2, datetime_expr1);
                return (long) (between.toMinutes());
            }
            case "HOUR": {
                Duration between = Duration.between(datetime_expr2, datetime_expr1);
                return (long) (between.toHours());
            }
            case "DAY":
                return (long) (datetime_expr2.getDayOfMonth()
                        -
                        datetime_expr1.getDayOfMonth());
            case "WEEK":
                return (long) (datetime_expr2.getDayOfMonth()
                        -
                        datetime_expr1.getDayOfMonth());
            case "MONTH":
                return (long) (datetime_expr2.getMonth().getValue()
                        -
                        datetime_expr1.getMonth().getValue());
            case "QUARTER":
                return (long) (datetime_expr2.getMonth().getValue()
                        -
                        datetime_expr1.getMonth().getValue()) / 3;
            case "YEAR":
                return (long) (datetime_expr2.getYear()
                        -
                        datetime_expr1.getYear());
            default:
        }
        throw new UnsupportedOperationException("diff :" + unit);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return super.deriveType(validator, scope, call);
    }
}

