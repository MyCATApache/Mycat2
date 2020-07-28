package io.mycat.hbt4.executor;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.*;
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
        super(cluster, traits, ImmutableList.of(), left, right, condition, variablesSet, joinType);
        this.requiredColumns = requiredColumns;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatBatchNestedLoopJoin");
        writer.into();
        List<RelNode> inputs = getInputs();
        for (RelNode input : inputs) {
            ( (MycatRel) input).explain(writer);
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