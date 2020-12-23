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
package io.mycat.calcite.physical;


import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.calcite.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

public class MycatSortMergeSemiJoin extends Join implements MycatRel {
    /**
     * Creates a MycatJoin.
     */
    protected MycatSortMergeSemiJoin(RelOptCluster cluster, RelTraitSet traitSet,
                                     RelNode left, RelNode right, RexNode condition,
                                     Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    public static MycatSortMergeSemiJoin create(RelTraitSet traitSet,
                                                RelNode left, RelNode right, RexNode condition,
                                                JoinRelType joinType) {
        RelOptCluster cluster = left.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        traitSet= traitSet.replace(MycatConvention.INSTANCE);
        traitSet=traitSet .replaceIfs(
                RelCollationTraitDef.INSTANCE,
                () ->mq.collations(left));//MycatSortMergeSemiJoin结果也是已经排序的
        return new MycatSortMergeSemiJoin(
                cluster,
                traitSet,
                left,
                right,
                condition,
                ImmutableSet.of(),
                joinType);
    }

    @Override
    public MycatSortMergeSemiJoin copy(RelTraitSet traitSet, RexNode condition,
                                       RelNode left, RelNode right, JoinRelType joinType,
                                       boolean semiJoinDone) {
        return new MycatSortMergeSemiJoin(getCluster(), traitSet, left, right,
                condition, variablesSet, joinType);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        // We always "build" the
        double rowCount = mq.getRowCount(this);

        return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        final double leftRowCount = left.estimateRowCount(mq);
        final double rightRowCount = right.estimateRowCount(mq);
        return Math.max(leftRowCount, rightRowCount);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatSortMergeSemiJoin").item("joinType", joinType).item("condition", condition).into();
        for (RelNode input : getInputs()) {
            MycatRel rel = (MycatRel) input;
            rel.explain(writer);
        }
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

}