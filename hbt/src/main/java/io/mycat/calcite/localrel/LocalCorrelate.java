package io.mycat.calcite.localrel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.util.ImmutableBitSet;

public class LocalCorrelate  extends Correlate  implements LocalRel {
    protected LocalCorrelate(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) {
        super(cluster, traitSet, left, right, correlationId, requiredColumns, joinType);
    }

    @Override
    public LocalCorrelate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) {
        return new  LocalCorrelate(getCluster(),traitSet,left,right,correlationId,requiredColumns,joinType);
    }
    static final RelFactories.CorrelateFactory CORRELATE_FACTORY =
            (left, right, correlationId, requiredColumns, joinType) -> {
                throw new UnsupportedOperationException("LocalCorrelate");
            };

}
