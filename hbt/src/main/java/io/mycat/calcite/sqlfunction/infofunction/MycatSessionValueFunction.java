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

import org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.mycat.MycatSqlDefinedFunction;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlReturnTypeInference;

import java.util.List;

public class MycatSessionValueFunction extends MycatSqlDefinedFunction {

    public static final MycatSessionValueFunction STRING_TYPE_INSTANCE = createString();

    public static final MycatSessionValueFunction BIGINT_TYPE_INSTANCE = createNumber();

    public MycatSessionValueFunction(String name,SqlReturnTypeInference sqlReturnTypeInference) {
        super(name, sqlReturnTypeInference, InferTypes.RETURN_TYPE, OperandTypes.ANY, null,
                SqlFunctionCategory.SYSTEM);
    }

    @Override
    public Expression implement(RexToLixTranslator translator, RexCall call, RexImpTable.NullAs nullAs) {
        List<Expression> argValueList = translator.translateList(call.getOperands(), nullAs);
        return Expressions.call(
                NewMycatDataContext.ROOT,
                "getSessionVariable",
                argValueList.get(0));
    }

    private static MycatSessionValueFunction createString() {
        return new MycatSessionValueFunction("MYCATSESSION_STRING_VALUE",ReturnTypes.VARCHAR_2000);
    }

    private static MycatSessionValueFunction createNumber() {
        return new MycatSessionValueFunction("MYCATSESSION_INT_VALUE",ReturnTypes.BIGINT_NULLABLE);
    }
}

