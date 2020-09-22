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

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;
import org.apache.calcite.sql.validate.SqlValidator;


public class EltFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(EltFunction.class,
            "elt");
    public static EltFunction INSTANCE = new EltFunction();


    public EltFunction() {
        super(new SqlIdentifier("elt", SqlParserPos.ZERO),
                ReturnTypes.explicit(SqlTypeName.VARCHAR),
                null,
                OperandTypes.repeat(SqlOperandCountRanges.any()), ImmutableList.of(), scalarFunction);
    }

    public static String elt(Integer n, String... strs) {
        if (n < 1 || n > (strs.length)) {
            return null;
        }
        return strs[n - 1];
    }

    @Override
    public boolean checkOperandTypes(SqlCallBinding callBinding, boolean throwOnFailure) {
        boolean b = super.checkOperandTypes(callBinding, throwOnFailure);
        return b;
    }

    @Override
    protected void checkOperandCount(SqlValidator validator, SqlOperandTypeChecker argType, SqlCall call) {
        super.checkOperandCount(validator, argType, call);
    }
}

