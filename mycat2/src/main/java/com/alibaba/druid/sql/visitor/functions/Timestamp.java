/*
 * Copyright 1999-2017 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.druid.sql.visitor.functions;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.visitor.SQLEvalVisitor;


public class Timestamp implements Function {
    public final static Timestamp instance = new Timestamp();
    
    public Object eval(SQLEvalVisitor visitor, SQLMethodInvokeExpr x) {
        SQLExpr sqlExpr = x.getArguments().get(0);

        if (sqlExpr instanceof SQLVariantRefExpr){
            int index = ((SQLVariantRefExpr) sqlExpr).getIndex();
            Object o = visitor.getParameters().get(index);
            if (o instanceof String){
                return o;
            }
        }
        throw new UnsupportedOperationException("unsupport " +x+ " yet");
    }
}
