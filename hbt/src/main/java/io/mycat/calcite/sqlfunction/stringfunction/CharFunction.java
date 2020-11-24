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


import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;


public class CharFunction extends MycatSqlDefinedFunction {
    public static CharFunction INSTANCE = new CharFunction();
    private static final Logger LOGGER = LoggerFactory.getLogger(CharFunction.class);
    private final Method charFunction;

    @SneakyThrows
    public CharFunction() {
        super("char",
                ReturnTypes.VARCHAR_2000, InferTypes.VARCHAR_1024, OperandTypes.SAME_VARIADIC, null, SqlFunctionCategory.STRING);
        this.charFunction = CharFunction.class.getMethod("charFunction", Object[].class);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        List<Expression> expressions = translator.translateList(call.getOperands(),nullAs);
        return Expressions.call(charFunction,expressions);
    }
//
//    public CharFunction() {
//        super("char");
//    }

    public static String charFunction(Object... exprs) {
        if (exprs == null || exprs.length == 0) {
            return null;
        }
        try {
            Object mayUsing = exprs[exprs.length - 1];
            Charset charset = StandardCharsets.US_ASCII;
            if (mayUsing != null && mayUsing instanceof String) {
                try {
                    charset = Charset.forName((String) mayUsing);
                    exprs = Arrays.copyOf(exprs, exprs.length - 1);
                } catch (UnsupportedCharsetException e) {

                }
            }
            int[] ints = Arrays.stream(exprs).filter(i -> i != null).map(i -> {
                return Objects.toString(i);
            }).mapToInt(i -> {
                if (i.startsWith("0x")) {
                    return Integer.parseInt(i, 16);
                } else {
                    return Integer.parseInt(i);
                }
            }).toArray();
            byte[] res = new byte[ints.length];
            int index = 0;
            for (int anInt : ints) {
                res[index] = (byte) anInt;
                ++index;
            }
            return new String(res, charset);
        } catch (Throwable e) {
            LOGGER.warn("", e);
            return null;
        }
    }

}

