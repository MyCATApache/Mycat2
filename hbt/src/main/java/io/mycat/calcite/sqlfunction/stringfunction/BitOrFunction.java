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


import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;

import java.lang.reflect.Method;
import java.util.List;

public class BitOrFunction extends MycatSqlDefinedFunction {


    public static final BitOrFunction INSTANCE = new BitOrFunction();

    public BitOrFunction() {
        super("|", ReturnTypes.ARG0, InferTypes.FIRST_KNOWN, OperandTypes.family(
                SqlTypeFamily.NUMERIC,
                SqlTypeFamily.NUMERIC
                ), null, SqlFunctionCategory.NUMERIC);

    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        List<Expression> expressions = translator.translateList(call.getOperands(), nullAs);
        if(expressions.get(0).getType() == Long.class){
            Method bitOr = Types.lookupMethod(BitOrFunction.class,
                    "bitOr", Long.class, Long.class);
            return Expressions.call(bitOr,expressions);
        }
        Method bitOr = Types.lookupMethod(BitOrFunction.class,
                "bitOr", Integer.class, Integer.class);
        return Expressions.call(bitOr,expressions);
    }

    public static Long bitOr(Long left,Long right) {
        if (left == null || right == null) {
            return null;
        }
        return left|right;
    }
    public static Integer bitOr(Integer left,Integer right) {
        if (left == null || right == null) {
            return null;
        }
        return left|right;
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlUtil.unparseBinarySyntax(this, call, writer, leftPrec, rightPrec);
    }
}