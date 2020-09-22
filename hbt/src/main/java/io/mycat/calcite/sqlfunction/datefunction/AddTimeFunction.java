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

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;


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
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(time), ZoneOffset.UTC);
        localDateTime = localDateTime.plus(tmp, ChronoUnit.MILLIS);
        return Timestamp.valueOf(localDateTime).getTime();
    }

    private static int getMillisInSecond(String v) {
        switch (v.length()) {
            case 19: // "1999-12-31 12:34:56"
                return 0;
            case 21: // "1999-12-31 12:34:56.7"
                return Integer.valueOf(v.substring(20)) * 100;
            case 22: // "1999-12-31 12:34:56.78"
                return Integer.valueOf(v.substring(20)) * 10;
            case 23: // "1999-12-31 12:34:56.789"
            default:  // "1999-12-31 12:34:56.789123456"
                return Integer.valueOf(v.substring(20, 23));
        }
    }
}

