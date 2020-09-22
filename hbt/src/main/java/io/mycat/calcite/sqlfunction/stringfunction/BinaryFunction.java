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

import io.mycat.calcite.MycatSqlDefinedFunction;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.BitString;


public class BinaryFunction extends MycatSqlDefinedFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(BinaryFunction.class,
            "binary");
    public static BinaryFunction INSTANCE = new BinaryFunction();


    public BinaryFunction() {
        super(new SqlIdentifier("binary", SqlParserPos.ZERO),
                ReturnTypes.ARG0_NULLABLE,
                InferTypes.explicit(getRelDataType(scalarFunction)),
                OperandTypes.STRING, getRelDataType(scalarFunction), scalarFunction);
    }

    public static ByteString binary(String expr) {
        if (expr == null){
            return null;
        }
        byte[] asByteArray = BitString.createFromHexString(expr).getAsByteArray();
        ByteString byteString = new ByteString(asByteArray);
        return byteString;
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.print("binary");
        call.unparse(writer,leftPrec,rightPrec);
    }
}

