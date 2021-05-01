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
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

public class Log2Function extends MycatSqlDefinedFunction {
    public static Log2Function INSTANCE = new Log2Function();

    public Log2Function() {
        super("LOG2", ReturnTypes.DOUBLE, InferTypes.FIRST_KNOWN, OperandTypes.NUMERIC,
                MycatScalarFunction.create(Log2Function.class, "log2", 1), SqlFunctionCategory.SYSTEM);
    }

    public static Double log2(Double number) {
        return Math.log(number)/Math.log(2);
    }
}
