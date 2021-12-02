package io.mycat.calcite.localrel;

import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.MycatRelDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.ImmutableBitSet;

public class LocalCorrelate extends Correlate implements LocalRel {
    protected LocalCorrelate(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) {
        super(cluster, traitSet.replace(LocalConvention.INSTANCE), left, right, correlationId, requiredColumns, joinType);
    }

    public LocalCorrelate(RelInput input) {
        this(input.getCluster(), input.getTraitSet(), input.getInputs().get(0),
                input.getInputs().get(1),
                new CorrelationId((Integer) input.get("correlation")),
                input.getBitSet("requiredColumns"),
                input.getEnum("joinType", JoinRelType.class));
    }

    public static LocalCorrelate create(Correlate correlate, RelNode left, RelNode right) {
        return new LocalCorrelate(correlate.getCluster(), correlate.getTraitSet(), left, right, correlate.getCorrelationId(), correlate.getRequiredColumns(), correlate.getJoinType());
    }

    @Override
    public LocalCorrelate copy(RelTraitSet traitSet, RelNode left, RelNode right, CorrelationId correlationId, ImmutableBitSet requiredColumns, JoinRelType joinType) {
        return new LocalCorrelate(getCluster(), traitSet, left, right, correlationId, requiredColumns, joinType);
    }

    static final RelFactories.CorrelateFactory CORRELATE_FACTORY =
            (left, right, correlationId, requiredColumns, joinType) -> {
                throw new UnsupportedOperationException("LocalCorrelate");
            };

    @Override
    public MycatRelDataType getMycatRelDataType() {
        LocalRel left = (LocalRel) getLeft();
        return left.getMycatRelDataType();
    }
}
