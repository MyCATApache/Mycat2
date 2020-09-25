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

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

import java.time.Duration;
import java.time.LocalDateTime;


public class AddTimeFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(AddTimeFunction.class,
            "addTime");
    public static AddTimeFunction INSTANCE = new AddTimeFunction();


    public AddTimeFunction() {
        super(new SqlIdentifier("ADDTIME", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.TIMESTAMP),
                InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.TIMESTAMP, SqlTypeFamily.TIMESTAMP),
                ImmutableList.of(), scalarFunction);
    }

    //SqlParserUtil
    //DateTimeUtils
    //SqlLiteral
    public static Long addTime(Long time, Long tmp) {
        if (time == null || tmp == null) {
            return null;
        }
        return tmp + time;
//        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC);
//        localDateTime = localDateTime.plus(tmp, ChronoUnit.MILLIS);
//        return Timestamp.valueOf(localDateTime).getTime();
    }

    public static LocalDateTime addTime(LocalDateTime time, Duration tmp) {
        if (time == null || tmp == null) {
            return null;
        }
        return time.plus(tmp);
    }


    public static void main(String[] args){
        Duration duration = Duration.ofDays(1).plusHours(1).plusMillis(1).plusNanos(1);
        System.out.println(duration);
    }
}

