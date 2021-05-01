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
package io.mycat.calcite.table;

import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.NewMycatDataContext;

import java.lang.reflect.Method;
import java.util.List;

import static io.mycat.calcite.logical.MycatView.toEnumerable;
import static io.mycat.calcite.logical.MycatView.toRows;

public class MycatTransientSQLTableScan extends AbstractRelNode implements MycatRel {
    final String sql;
    final String targetName;

    public MycatTransientSQLTableScan(RelOptCluster cluster, RelDataType relDataType, String targetName, String sql) {
        super(cluster, cluster.traitSetOf(MycatConvention.INSTANCE));
        this.targetName = targetName;
        this.sql = sql;
        this.rowType = relDataType;
    }


    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatTransientSQLTableScan(getCluster(), this.rowType, targetName, sql);
    }

    public String getSql() {
        return sql;
    }

    public String getTargetName() {
        return targetName;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw).item("target",targetName).item("sql",sql);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatTransientSQLTableScan").into()
                .item("target", targetName)
                .item("sql", sql)
                .ret();
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final PhysType physType =
                PhysTypeImpl.of(
                        implementor.getTypeFactory(),
                        getRowType(),
                        JavaRowFormat.ARRAY);
        ParameterExpression root = implementor.getRootExpression();
        Expression mycatViewStash = Expressions.constant(this.getDigest());
        Method getEnumerable = Types.lookupMethod(NewMycatDataContext.class, "getEnumerable", String.class);
        final Expression expression2 = toEnumerable(
                Expressions.call(root, getEnumerable, mycatViewStash));
        assert Types.isAssignableFrom(Enumerable.class, expression2.getType());
        builder.add(toRows(physType, expression2,getRowType().getFieldCount()));
        return implementor.result(physType, builder.toBlock());
    }
}