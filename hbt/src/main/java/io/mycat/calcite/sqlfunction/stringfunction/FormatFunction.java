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
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Locale;

/**
 * SELECT FORMAT(1234567890.09876543210, 4) AS 'Format'; 支持
 * SELECT FORMAT('1234567890.09876543210', 4) AS 'Format'; 支持
 */
public class FormatFunction extends MycatSqlDefinedFunction {

    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(FormatFunction.class,
            "format");
    public static final FormatFunction INSTANCE = new FormatFunction();

    public FormatFunction() {
        super("format",
                ReturnTypes.VARCHAR_2000, InferTypes.VARCHAR_1024, OperandTypes.SAME_VARIADIC, null, SqlFunctionCategory.STRING);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        Method format = Types.lookupMethod(FormatFunction.class, "format", Object[].class);
        return Expressions.call(format, translator.translateList(call.getOperands(),nullAs));
    }

    public static String format(Object... args) {
        Object num = args[0];
        Number decimal_position = ((Number) args[1]);
        String locale = Locale.ROOT.getLanguage();
        if (args.length == 3) {
            locale = (String) args[2];
        }
        if (num == null || decimal_position == null) {
            return null;
        }
        NumberFormat numberFormat = DecimalFormat.getNumberInstance(new Locale(locale));
        int i = decimal_position.intValue();
        numberFormat.setMaximumFractionDigits(i < 0 ? 0 : i);
        numberFormat.setRoundingMode(RoundingMode.HALF_UP);
        BigDecimal bigDecimal = new BigDecimal(num.toString());


        return numberFormat.format(bigDecimal);
    }
}