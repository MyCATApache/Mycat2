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
package io.mycat.calcite.executor;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Set;

/**
 * the right of input must be Lookup Executor
 */
public class MycatBatchNestedLoopJoin extends Join implements MycatRel {

    private final ImmutableBitSet requiredColumns;

    protected MycatBatchNestedLoopJoin(
            RelOptCluster cluster,
            RelTraitSet traits,
            RelNode left,
            RelNode right,
            RexNode condition,
            Set<CorrelationId> variablesSet,
            ImmutableBitSet requiredColumns,
            JoinRelType joinType) {
        super(cluster,
                traits,
                ImmutableList.of(),
                left,
                right,
                condition,
                variablesSet,
                joinType);
        this.requiredColumns = requiredColumns;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatBatchNestedLoopJoin");
        writer.into();
        List<RelNode> inputs = getInputs();
        for (RelNode input : inputs) {
            ((MycatRel) input).explain(writer);
        }
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }


    @Override
    public MycatBatchNestedLoopJoin copy(RelTraitSet traitSet,
                                         RexNode condition, RelNode left, RelNode right, JoinRelType joinType,
                                         boolean semiJoinDone) {
        return new MycatBatchNestedLoopJoin(getCluster(), traitSet,
                left, right, condition, variablesSet, requiredColumns, joinType);
    }

    public static MycatBatchNestedLoopJoin create(
            RelNode left,
            RelNode right,
            RexNode condition,
            ImmutableBitSet requiredColumns,
            Set<CorrelationId> variablesSet,
            JoinRelType joinType) {
        final RelOptCluster cluster = left.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSetOf(MycatConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.enumerableBatchNestedLoopJoin(mq, left, right, joinType));
        return new MycatBatchNestedLoopJoin(
                cluster,
                traitSet,
                left,
                right,
                condition,
                variablesSet,
                requiredColumns,
                joinType);
    }

    public ImmutableBitSet getRequiredColumns() {
        return requiredColumns;
    }
}