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
package io.mycat.calcite.sqlfunction.jsonfunction;


import io.mycat.calcite.MycatScalarFunction;
import io.mycat.calcite.sqlfunction.stringfunction.MycatStringFunction;
import io.mycat.calcite.sqlfunction.stringfunction.NotRegexpFunction;
import io.mycat.calcite.sqlfunction.stringfunction.RegexpFunction;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWriter;


public class SubGtFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = MycatScalarFunction.create(NotRegexpFunction.class,
            "subGet", 2);
    public static NotRegexpFunction INSTANCE = new NotRegexpFunction();


    public SubGtFunction() {
        super("->", scalarFunction);
    }

    public static Boolean subGet(String expr, String pat) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        SqlUtil.unparseBinarySyntax(this, call, writer, leftPrec, rightPrec);
    }
}
