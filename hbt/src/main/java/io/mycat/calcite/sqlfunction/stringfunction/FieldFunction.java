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
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.Types;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Method;

public class FieldFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(FieldFunction.class,
            "field");
    public static final FieldFunction INSTANCE = new FieldFunction();

    public FieldFunction() {
        super("field",
                ReturnTypes.VARCHAR_2000, InferTypes.VARCHAR_1024, OperandTypes.SAME_VARIADIC, null, SqlFunctionCategory.STRING);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        Method method = Types.lookupMethod(FieldFunction.class, "field", String[].class);
        return Expressions.call(method, translator.translateList(call.getOperands(),nullAs));
    }

    @SneakyThrows
    public static Integer field(String... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("1582");
        }
        String pat = args[0];
        if (pat == null) {
            return null;
        }
        for (int i = 1; i < args.length; i++) {
            if (pat.equalsIgnoreCase(args[i])) {
                return i;
            }
        }
        return 0;
    }

    @SneakyThrows
    public static Integer field(Number... args) {
        if (args.length < 2) {
            throw new IllegalArgumentException("1582");
        }
        Number pat = args[0];
        if (pat == null) {
            return null;
        }
        for (int i = 1; i < args.length; i++) {
            if (pat.equals(args[i])) {
                return i;
            }
        }
        return 0;
    }
}