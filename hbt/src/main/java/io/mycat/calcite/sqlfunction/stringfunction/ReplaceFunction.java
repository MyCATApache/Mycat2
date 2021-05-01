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


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;


public class ReplaceFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(ReplaceFunction.class,
            "repeat");

    public static final ReplaceFunction INSTANCE = new ReplaceFunction();

    public ReplaceFunction() {
        super("REPLACE", scalarFunction);
    }

    public static String replace(String str, String from_str,String to_str) {
        if (str == null || from_str == null||to_str == null) {
            return null;
        }
        return str.replace(from_str, to_str);
    }
}