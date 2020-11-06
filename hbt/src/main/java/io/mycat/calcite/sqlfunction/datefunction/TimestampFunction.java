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


public class TimestampFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(TimestampFunction.class,
            "timestamp");
    public static TimestampFunction INSTANCE = new TimestampFunction();


    public TimestampFunction() {
        super("TIMESTAMP", scalarFunction);
    }

    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static LocalDateTime timestamp(LocalDate date) {
        if (date == null) {
            return null;
        }
        return date.atStartOfDay();
    }

    public static String addTime(String time, String tmp, boolean sub) {
        if (time == null || tmp == null) {
            return null;
        }
        Duration duration = MycatBuiltInMethodImpl.timeStringToTimeDuration(tmp);
        Temporal temporal;
        if (time.contains(":") && !time.contains("-")) {//time
            Duration duration1 = MycatBuiltInMethodImpl.timeStringToTimeDuration(time);
            duration1 = !sub ? duration1.plus(duration) : duration1.minus(duration);
            long days1 = duration1.toDays();
            if (days1 == 0) {
                long hours = TimeUnit.SECONDS.toHours(duration1.getSeconds());
                int SECONDS_PER_HOUR = 60 * 60;
                int SECONDS_PER_MINUTE = 60;
                int minutes = (int) ((duration1.getSeconds() % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
                int secs = (int) (duration1.getSeconds() % SECONDS_PER_MINUTE);
                int nano = duration1.getNano();
                //01:00:00.999999
                return String.format("%02d:%02d:%02d.%09d", hours, minutes, secs, nano);
            } else {
                long hours = TimeUnit.SECONDS.toHours(duration1.getSeconds());
                int SECONDS_PER_HOUR = 60 * 60;
                int SECONDS_PER_MINUTE = 60;
                int minutes = (int) ((duration1.getSeconds() % SECONDS_PER_HOUR) / SECONDS_PER_MINUTE);
                int secs = (int) (duration1.getSeconds() % SECONDS_PER_MINUTE);
                int nano = duration1.getNano();
                return String.format("%02d:%02d:%02d:%02d.%09d", days1, hours, minutes, secs, nano);
            }
        }
        temporal = MycatBuiltInMethodImpl.timestampStringToTimestamp(time);

        Temporal res = !sub ? addTime(temporal, duration) : subTime(temporal, duration);
        if (res instanceof LocalDateTime) {
            LocalDateTime res1 = (LocalDateTime) res;
            return res1.toLocalDate().toString() + " " + res1.toLocalTime().toString();
        }
        if (res instanceof LocalTime) {
            LocalTime res1 = (LocalTime) res;
            return res1.toString();
        }
        return res.toString();
    }

    private static Temporal addTime(Temporal temporal, Duration duration) {
        if (temporal == null || duration == null) {
            return null;
        }
        Temporal plus = temporal.plus(duration);
        return plus;
    }

    private static Temporal subTime(Temporal temporal, Duration duration) {
        if (temporal == null || duration == null) {
            return null;
        }
        Temporal plus = temporal.minus(duration);
        return plus;
    }


    public static void main(String[] args) {
        Duration duration = Duration.ofDays(1).plusHours(1).plusMillis(1).plusNanos(1);
        System.out.println(duration);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return super.deriveType(validator, scope, call);
    }
}

