package io.mycat.calcite.localrel;

import com.google.common.base.Preconditions;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class LocalFilter  extends Filter implements LocalRel {
    protected LocalFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits.replace(LocalConvention.INSTANCE), child, condition);
    }

    public LocalFilter(RelInput input) {
        super(input);
    }

    public static LocalFilter create(Filter filter, RelNode input) {
        return new LocalFilter(filter.getCluster(), filter.getTraitSet(), input, filter.getCondition());
    }

    @Override
    public LocalFilter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new LocalFilter(getCluster(),traitSet,input,condition);
    }
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        RelOptCost relOptCost = super.computeSelfCost(planner, mq).multiplyBy(.9);
        return relOptCost;
    }
    static final RelFactories.FilterFactory FILTER_FACTORY =
            (input, condition, variablesSet) -> {
                Preconditions.checkArgument(variablesSet.isEmpty(),
                        "JdbcFilter does not allow variables");
                return new JdbcRules.JdbcFilter(input.getCluster(),
                        input.getTraitSet(), input, condition);
            };
}
