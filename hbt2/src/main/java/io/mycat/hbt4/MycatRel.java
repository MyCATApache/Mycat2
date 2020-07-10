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
package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import java.util.List;

/**
 * Relational expression that uses JDBC calling convention.
 */
public interface MycatRel extends RelNode {

    ExplainWriter explain(ExplainWriter writer);

    Executor implement(ExecutorImplementor implementor);

    public static ExplainWriter explainJoin(Join join, String name, ExplainWriter writer) {
        writer.name(name);
        List<String> fieldList = join.getRowType().getFieldNames();
        writer.item("columns", String.join(",", fieldList));
        SqlImplementor.Context context = explainRex(fieldList);
        SqlNode sqlNode = context.toSql(null, join.getCondition());
        writer.item("condition", sqlNode);
        writer.into();
        ((MycatRel) join.getLeft()).explain(writer);
        ((MycatRel) join.getRight()).explain(writer);
        return writer.ret();
    }

    public static SqlImplementor.Context explainRex(List<String> fieldList) {
        return new SqlImplementor.Context(MysqlSqlDialect.DEFAULT, fieldList.size()) {
                @Override
                public SqlNode field(int ordinal) {
                    String fieldName = fieldList.get(ordinal);
                    return new SqlIdentifier(ImmutableList.of(fieldName), SqlImplementor.POS);
                }
            };
    }
}
