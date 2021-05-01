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
package io.mycat.calcite.sqlfunction.mathfunction;

import io.mycat.calcite.MycatScalarFunction;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.SqlFunctions;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class TruncateFunction extends MycatSqlDefinedFunction {
    public static TruncateFunction INSTANCE = new TruncateFunction();

    public TruncateFunction() {
        super("TRUNCATE",
                ReturnTypes.ARG0_NULLABLE,
                null,
                OperandTypes.NUMERIC_OPTIONAL_INTEGER,

               null, SqlFunctionCategory.SYSTEM);
    }


    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        List<Expression> expressions = translator.translateList(call.getOperands(), nullAs);
        Class[] classes = expressions.stream().map(i -> i.getType()).toArray(n->new Class[n]);
        Method struncate = Types.lookupMethod(SqlFunctions.class, "struncate", classes);
        return Expressions.call(struncate,expressions);
    }

}
