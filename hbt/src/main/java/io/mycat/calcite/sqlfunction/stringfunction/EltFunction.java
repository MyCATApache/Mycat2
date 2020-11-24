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
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.validate.SqlValidator;

import java.lang.reflect.Method;
import java.util.List;


public class EltFunction extends MycatSqlDefinedFunction {

    public static EltFunction INSTANCE = new EltFunction();


    public EltFunction() {
        super("elt", ReturnTypes.VARCHAR_2000, InferTypes.VARCHAR_1024, OperandTypes.SAME_VARIADIC, null, SqlFunctionCategory.STRING);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        Method method = Types.lookupMethod(EltFunction.class, "elt",Integer.class,String[].class);
        List<Expression> expressions = translator.translateList(call.getOperands(),nullAs);
        return Expressions.call(method, expressions);
    }

    public static String elt(Number n, String... strs) {
        if (n.intValue() < 1 || n.intValue() > (strs.length)) {
            return null;
        }
        return strs[n.intValue() - 1];
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

