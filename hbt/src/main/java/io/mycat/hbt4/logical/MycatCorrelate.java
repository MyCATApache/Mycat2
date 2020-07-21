package io.mycat.hbt4.logical;

import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.ImmutableBitSet;

public class MycatCorrelate extends Correlate implements MycatRel {
    public MycatCorrelate(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) {
        super(cluster, traitSet.replace(MycatConvention.INSTANCE), left, right, correlationId, requiredColumns, joinType);
    }

    @Override
    public Correlate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) {
        return new MycatCorrelate(
                getCluster(),
                traitSet.replace(MycatConvention.INSTANCE),
                left,
                right,
                correlationId,
                requiredColumns,
                joinType);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatCorrelate");
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}