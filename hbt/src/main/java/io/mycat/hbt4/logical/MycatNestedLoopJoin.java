package io.mycat.hbt4.logical;

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

/**
 * Join operator implemented in Mycat convention.
 */
public class MycatNestedLoopJoin extends Join implements MycatRel {
    /**
     * Creates a MycatJoin.
     */
    public MycatNestedLoopJoin(RelOptCluster cluster, RelTraitSet traitSet,
                               RelNode left, RelNode right, RexNode condition,
                               Set<CorrelationId> variablesSet, JoinRelType joinType) {
        super(cluster, traitSet, ImmutableList.of(), left, right, condition, variablesSet, joinType);
    }

    @Override
    public MycatNestedLoopJoin copy(RelTraitSet traitSet, RexNode condition,
                                    RelNode left, RelNode right, JoinRelType joinType,
                                    boolean semiJoinDone) {
        return new MycatNestedLoopJoin(getCluster(), traitSet, left, right,
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
        writer.name("MycatJoin").item("joinType", joinType).item("condition", condition).into();
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