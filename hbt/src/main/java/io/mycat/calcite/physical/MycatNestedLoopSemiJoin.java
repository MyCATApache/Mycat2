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


import com.google.common.collect.ImmutableSet;
import io.mycat.calcite.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.List;
import java.util.Objects;

public class MycatNestedLoopSemiJoin extends Join implements MycatRel {

    protected MycatNestedLoopSemiJoin(RelOptCluster cluster,
                                      RelTraitSet traitSet,
                                      List<RelHint> hints,
                                      RelNode left,
                                      RelNode right,
                                      RexNode condition,
                                      JoinRelType joinType) {
        super(cluster,  Objects.requireNonNull(traitSet).replace(MycatConvention.INSTANCE), hints, left, right, condition, ImmutableSet.of(), joinType);
    }

    @Override
    public MycatNestedLoopSemiJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new MycatNestedLoopSemiJoin(getCluster(), traitSet, getHints(), left, right, conditionExpr, joinType);
    }

    public static MycatNestedLoopSemiJoin create(
            List<RelHint> hints,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType) {
        RelOptCluster cluster = left.getCluster();
        RelMetadataQuery metadataQuery = cluster.getMetadataQuery();
        return new MycatNestedLoopSemiJoin(left.getCluster(),
                traitSet.replace(MycatConvention.INSTANCE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.enumerableNestedLoopJoin(metadataQuery, left, right, joinType)),
                hints,
                left,
                right,
                condition,
                joinType
        );
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatMaterializedSemiJoin");
        for (RelNode input : getInputs()) {
            MycatRel mycatRel = (MycatRel) input;
            mycatRel.explain(writer);
        }

        return writer.ret();
    }

    @Override
    public Result implement(MycatEnumerableRelImplementor implementor, Prefer pref) {
        MycatNestedLoopJoin mycatNestedLoopJoin = MycatNestedLoopJoin.create(getTraitSet(), getLeft(), getRight(), getCondition(), joinType);
        return mycatNestedLoopJoin.implement(implementor, pref);
    }
    @Override
    public boolean isSupportStream() {
        return false;
    }
}