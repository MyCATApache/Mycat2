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
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

public class SubStringFunction extends MycatSqlDefinedFunction {

    public static final SubStringFunction INSTANCE = new SubStringFunction();

    public SubStringFunction() {
        super("subString",
                ReturnTypes.VARCHAR_2000, InferTypes.FIRST_KNOWN, OperandTypes.VARIADIC, null, SqlFunctionCategory.STRING);

    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        return Expressions.call(Types.lookupMethod(SubStringFunction.class,
                "subString",Object[].class),
                translator.translateList(call.getOperands(),nullAs));
    }

    public static String subString(Object... args){
        if (args.length == 3){
            return subString2((String) args[0],(Number)args[1],(Number)args[2]);
        }else {
            return subString((String) args[0],(Number)args[1]);
        }
    }
    public static String subString(String str, Number posText) {
        if (str == null || posText == null) {
            return null;
        }
        int pos = posText.intValue();
        pos = (pos < 0) ? str.length() + pos : pos - 1;
        return str.substring(pos);
    }

    public static String subString2(String str, Number posText, Number lenText) {
        if (str == null || posText == null || lenText == null || lenText.intValue() <= 0) {
            return null;
        }
        int pos = posText.intValue();
        pos = (pos < 0) ? str.length() + pos : pos - 1;
        return str.substring(pos, pos + lenText.intValue());
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        super.unparse(writer, call, leftPrec, rightPrec);
    }
}