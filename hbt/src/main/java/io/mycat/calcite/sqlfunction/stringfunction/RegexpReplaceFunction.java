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
package io.mycat.calcite.sqlfunction.stringfunction;

import io.mycat.calcite.MycatScalarFunction;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class RegexpReplaceFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = MycatScalarFunction.create(RegexpReplaceFunction.class,
            "regexpReplace", 3);
    public static RegexpReplaceFunction INSTANCE = new RegexpReplaceFunction();


    public RegexpReplaceFunction() {
        super(new SqlIdentifier("regexp_replace", SqlParserPos.ZERO),
                ReturnTypes.DYADIC_STRING_SUM_PRECISION_NULLABLE,
                InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING, SqlTypeFamily.STRING, SqlTypeFamily.STRING),
                getRelDataType(scalarFunction), scalarFunction);
    }

    public static String regexpReplace(String expr, String pat, String replace) {
        if (expr == null || pat == null || replace == null) {
            return null;
        }
        Matcher matcher = Pattern.compile(pat).matcher(expr);
        if (matcher.find()) {
            return matcher.replaceAll(replace);
        }
        return expr;
    }
}

