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
import org.apache.calcite.sql.type.*;

public class LpadFunction extends MycatSqlDefinedFunction {

    public static final LpadFunction INSTANCE = new LpadFunction();

    public LpadFunction() {
        super("lpad", ReturnTypes.VARCHAR_2000,
                InferTypes.FIRST_KNOWN,
                OperandTypes.VARIADIC,
                null,
                SqlFunctionCategory.STRING);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        if(call.getOperands().size()==3){
            return Expressions.call(
                    Types.lookupMethod(LpadFunction.class,"lpad",String.class,Integer.class,String.class),
                    translator.translateList(call.getOperands(),nullAs));
        }
        return Expressions.call(
                Types.lookupMethod(LpadFunction.class,"lpad",String.class,Integer.class,String.class),
                translator.translateList(call.getOperands(),nullAs));

    }

    public static String lpad(String str, Integer len, String padstr) {
        if (str == null || len == null || len < 0 || padstr == null || padstr.isEmpty()) {
            return null;
        }
        if (len < str.length()) {
            return str.substring(0, len);
        }
        StringBuilder sb = new StringBuilder();
        int count = len - str.length();

        boolean conti = true;
        while (conti) {
            int length = padstr.length();
            for (int i = 0; i < length; i++) {
                if (sb.length() < count) {
                    sb.append(padstr.charAt(i));
                }else {
                    conti = false;
                    break;
                }
            }
        }
        return sb.append(str).toString();
    }

    public static String lpad(String str, Integer len) {
        return lpad(str, len, " ");
    }
}