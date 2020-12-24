package io.mycat.calcite.physical;

import io.mycat.calcite.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

public class MycatCorrelate extends Correlate implements MycatRel {
    protected MycatCorrelate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode left,
            RelNode right,
            CorrelationId correlationId,
            ImmutableBitSet requiredColumns,
            JoinRelType joinType) {
        super(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
    }

    public static MycatCorrelate create(RelTraitSet traitSet,
                                 RelNode left,
                                 RelNode right,
                                 CorrelationId correlationId,
                                 ImmutableBitSet requiredColumns,
                                 JoinRelType joinType){
        RelOptCluster cluster = left.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        traitSet = traitSet.replace(MycatConvention.INSTANCE);
        traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.enumerableCorrelate(mq, left, right,joinType));
        return new MycatCorrelate(cluster,traitSet,left,right,correlationId,requiredColumns,joinType);
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
         writer.name("MycatCorrelate");
        for (RelNode input : this.getInputs()) {
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