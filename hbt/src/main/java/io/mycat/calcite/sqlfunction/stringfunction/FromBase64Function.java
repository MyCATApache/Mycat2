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
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class FromBase64Function extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = MycatScalarFunction.create(FromBase64Function.class,
            "fromBase64", 1);
    public static FromBase64Function INSTANCE = new FromBase64Function();


    public FromBase64Function() {
        super(new SqlIdentifier("FROM_BASE64", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.BINARY),
                InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.family(SqlTypeFamily.STRING), getRelDataType(scalarFunction), scalarFunction);
    }

    public static ByteString fromBase64(String expr) {
        if (expr == null) {
            return null;
        }
        return ByteString.ofBase64(expr);
    }
}

