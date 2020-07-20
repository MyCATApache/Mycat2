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
package io.mycat.hbt4.physical;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExecutorImplementor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.hbt4.MycatRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import java.util.Set;

public class MycatSortMergeJoin extends Join implements MycatRel {
    public MycatSortMergeJoin(RelOptCluster cluster,
                              RelTraitSet traitSet,
                              RelNode left,
                              RelNode right,
                              RexNode condition,
                              Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return MycatRel.explainJoin(this, "SortMergeJoin",writer);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // We assume that the inputs are sorted. The price of sorting them has
        // already been paid. The cost of the join is therefore proportional to the
        // input and output size.
        final double rightRowCount = right.estimateRowCount(mq);
        final double leftRowCount = left.estimateRowCount(mq);
        final double rowCount = mq.getRowCount(this);
        final double d = leftRowCount + rightRowCount + rowCount;
        RelOptCost relOptCost = planner.getCostFactory().makeCost(d, 0, 0);
        return relOptCost;
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
        return new MycatSortMergeJoin(getCluster(),traitSet,left,right,conditionExpr, getVariablesSet(), joinType);
    }
}