/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.physical;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Util;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class MycatSemiHashJoin extends Join implements MycatRel {


    protected MycatSemiHashJoin(RelOptCluster cluster,
                                RelTraitSet traitSet,
                                RelNode left,
                                RelNode right,
                                RexNode condition,
                                Set<CorrelationId> variablesSet,
                                JoinRelType joinType) {
        super(cluster,  Objects.requireNonNull(traitSet).replace(MycatConvention.INSTANCE), ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new MycatSemiHashJoin(getCluster(), traitSet, left, right, conditionExpr, getVariablesSet(), joinType);
    }

    public static MycatSemiHashJoin create( RelNode left, RelNode right, RexNode condition, JoinRelType joinType) {
        RelOptCluster cluster = left.getCluster();
        RelMetadataQuery metadataQuery = cluster.getMetadataQuery();
        return new MycatSemiHashJoin(left.getCluster(),
                cluster.traitSetOf(MycatConvention.INSTANCE)
                .replaceIfs(
                        RelCollationTraitDef.INSTANCE,
                        () -> RelMdCollation.enumerableHashJoin(metadataQuery, left,right,joinType)),
                left,
                right,
                condition,
                ImmutableSet.of(),
                joinType
                );
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatSemiHashJoin");
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        EnumerableHashJoin enumerableHashJoin = EnumerableHashJoin.create(left, right, condition, variablesSet, joinType);
        Result result = enumerableHashJoin.implement(implementor, pref);
        return result;
    }
    @Override
    public boolean isSupportStream() {
        return false;
    }
}