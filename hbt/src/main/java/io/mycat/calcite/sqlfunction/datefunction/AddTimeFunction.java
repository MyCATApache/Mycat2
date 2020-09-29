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
import org.apache.commons.lang.time.DateFormatUtils;

import java.text.DateFormat;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.Temporal;


public class AddTimeFunction extends MycatDateFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(AddTimeFunction.class,
            "addTime");
    public static AddTimeFunction INSTANCE = new AddTimeFunction();


    public AddTimeFunction() {
        super("ADDTIME", scalarFunction);
    }

    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static String addTime(String time, String tmp) {
        if (time == null || tmp == null) {
            return null;
        }
        Duration duration = MycatBuiltInMethodImpl.timeStringToTimeDuration(tmp);
        Temporal temporal;
        if (time.contains(":") && !time.contains("-")) {//time
            Duration duration1 = MycatBuiltInMethodImpl.timeStringToTimeDuration(time);
            duration1 = duration1.plus(duration);
            long days = duration1.toDays();
            long hours = duration1.toHours();
            long minutes = duration1.toMinutes();
            long seconds = duration1.getSeconds();
            int nano = duration1.getNano();

            if (days > 1) {
                hours = +(days * 24);
            }
            //01:00:00.999999
            return String.format("%02d:%02d:%02d.%d", hours, minutes, seconds, nano);
        }
        temporal = MycatBuiltInMethodImpl.timestampStringToUnixTimestamp(time);

        Temporal res = addTime(temporal, duration);
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


    public static void main(String[] args) {
        Duration duration = Duration.ofDays(1).plusHours(1).plusMillis(1).plusNanos(1);
        System.out.println(duration);
    }

    @Override
    public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
        return super.deriveType(validator, scope, call);
    }
}

