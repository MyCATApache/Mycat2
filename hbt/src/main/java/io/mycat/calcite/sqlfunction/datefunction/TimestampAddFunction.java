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


import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.mycat.MycatBuiltInMethodImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;
import java.util.concurrent.TimeUnit;


public class TimestampAddFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimestampAddFunction.class,
            "timestampAdd");
    public static TimestampAddFunction INSTANCE = new TimestampAddFunction();


    public TimestampAddFunction() {
        super("TIMESTAMPADD", scalarFunction);
    }

    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static LocalDateTime timestampAdd(String  unit,Long interval,LocalDateTime datetime_expr) {
        if (unit == null|| interval == null|| datetime_expr == null) {
            return null;
        }
        //MICROSECOND, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, QUARTER, or YEAR.
        switch (unit.toUpperCase()) {
            case "MICROSECOND":
                return datetime_expr.plus(Duration.ofNanos(TimeUnit.MICROSECONDS.toNanos(interval)));
            case "SECOND":
                return datetime_expr.plusSeconds(interval);
            case "MINUTE":
                return datetime_expr.plusMinutes(interval);
            case "HOUR":
                return datetime_expr.plusHours(interval);
            case "DAY":
                return datetime_expr.plusDays(interval);
            case "WEEK":
                return datetime_expr.plusWeeks(interval);
            case "MONTH":
                return datetime_expr.plusMonths(interval);
            case "QUARTER":
                return datetime_expr.plusMonths(3*interval);
            case "YEAR":
                return datetime_expr.plusYears(interval);
            default:
                throw new UnsupportedOperationException("plus :"+unit);
        }
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return super.deriveType(validator, scope, call);
    }
}

