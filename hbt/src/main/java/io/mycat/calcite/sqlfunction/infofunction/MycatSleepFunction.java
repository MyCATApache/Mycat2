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
package io.mycat.calcite.sqlfunction.infofunction;

import io.mycat.calcite.MycatScalarFunction;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import java.util.concurrent.TimeUnit;

public class MycatSleepFunction extends MycatSqlDefinedFunction {
    public final static MycatSleepFunction INSTANCE = new MycatSleepFunction();

    public MycatSleepFunction() {
        super("SLEEP",
                ReturnTypes.INTEGER,
                InferTypes.RETURN_TYPE, OperandTypes.NUMERIC,
                MycatScalarFunction.create(MycatSleepFunction.class, "sleep", 1), SqlFunctionCategory.SYSTEM);
    }
//
//    @Override
//    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
//        Method sleep = Types.lookupMethod(MycatSleepFunction.class, "sleep", Number.class);
//        return Expressions.call(sleep);
//    }

    public static int sleep(Number value){
        try{
           Thread.sleep(TimeUnit.SECONDS.toMillis(value.longValue()));
        }catch (InterruptedException exception){
            return 1;
        }
        return 0;
    }
}
