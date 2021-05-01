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
import org.apache.calcite.sql.type.*;

import java.util.zip.CRC32;

public class CRC32Function extends MycatSqlDefinedFunction {
    public static final CRC32Function INSTANCE = new CRC32Function();

    public CRC32Function() {
        super("CRC32", ReturnTypes.BIGINT,
                InferTypes.FIRST_KNOWN, OperandTypes.STRING,
                MycatScalarFunction.create(CRC32Function.class,
                        "crc32", 1),
                SqlFunctionCategory.STRING);
    }

    public static Long crc32(String arg0) {
        if (arg0 == null) {
            return null;
        }
        CRC32 crc32 = new CRC32();
        crc32.update(arg0.getBytes());

        return crc32.getValue();
    }
}
