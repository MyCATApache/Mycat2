/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt4.logical.rel;


import com.google.common.collect.ImmutableSet;
import io.mycat.hbt4.*;
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

public class MycatMaterializedSemiJoin extends Join implements MycatRel {

    protected MycatMaterializedSemiJoin(RelOptCluster cluster,
                                        RelTraitSet traitSet,
                                        List<RelHint> hints,
                                        RelNode left,
                                        RelNode right,
                                        RexNode condition,
                                        JoinRelType joinType) {
        super(cluster, traitSet, hints, left, right, condition, ImmutableSet.of(), joinType);
    }

    @Override
    public MycatMaterializedSemiJoin copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new MycatMaterializedSemiJoin(getCluster(),traitSet,getHints(),left,right,conditionExpr,joinType);
    }

    public static MycatMaterializedSemiJoin create(
            List<RelHint> hints,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            RexNode condition,
            JoinRelType joinType) {
        RelOptCluster cluster = left.getCluster();
        RelMetadataQuery metadataQuery = cluster.getMetadataQuery();
        return new MycatMaterializedSemiJoin(left.getCluster(),
                traitSet.replace(MycatConvention.INSTANCE)
                        .replaceIfs(
                                RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.enumerableNestedLoopJoin(metadataQuery, left,right,joinType)),
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
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

}