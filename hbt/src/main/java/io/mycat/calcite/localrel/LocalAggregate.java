package io.mycat.calcite.localrel;

import io.mycat.beans.mycat.MycatRelDataType;
import io.mycat.calcite.MycatRelDataTypeUtil;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LocalAggregate extends Aggregate implements LocalRel {
    protected LocalAggregate(RelOptCluster cluster,
                             RelTraitSet traitSet,
                             List<RelHint> hints,
                             RelNode input,
                             ImmutableBitSet groupSet,
                             List<ImmutableBitSet> groupSets,
                             List<AggregateCall> aggCalls) {
        super(cluster, traitSet.replace(LocalConvention.INSTANCE), hints, input, groupSet, groupSets, aggCalls);
    }

    public static LocalAggregate create(Aggregate aggregate, RelNode input) {
        return new LocalAggregate(aggregate.getCluster(), aggregate.getTraitSet(), aggregate.getHints(), input, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
    }

    public LocalAggregate(RelInput input) {
        super(input);
    }

    @Override
    public LocalAggregate copy(RelTraitSet traitSet,
                               RelNode input,
                               ImmutableBitSet groupSet,
                               List<ImmutableBitSet> groupSets,
                               List<AggregateCall> aggCalls) {
        return new LocalAggregate(getCluster(), traitSet, getHints(), input, groupSet, groupSets, aggCalls);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.5);
    }

    public static final RelFactories.AggregateFactory AGGREGATE_FACTORY =
            (input, hints, groupSet, groupSets, aggCalls) -> {
                final RelOptCluster cluster = input.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(
                        requireNonNull(input.getConvention(), "input.getConvention()"));
                return new LocalAggregate(cluster, traitSet, hints, input, groupSet,
                        groupSets, aggCalls);
            };

    @Override
    public MycatRelDataType getMycatRelDataType() {
        return MycatRelDataTypeUtil.getMycatRelDataType(getRowType());
    }
}
