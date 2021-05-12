package io.mycat.calcite.localrel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexLiteral;

import java.util.List;

public class LocalValues extends Values implements LocalRel{
    protected LocalValues(RelOptCluster cluster, RelDataType rowType, ImmutableList<ImmutableList<RexLiteral>> tuples, RelTraitSet traits) {
        super(cluster, rowType, tuples, traits);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new LocalValues(getCluster(),getRowType(),getTuples(),traitSet);
    }
    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.9);
    }
    public static final RelFactories.ValuesFactory VALUES_FACTORY =
            (cluster, rowType, tuples) -> {
                throw new UnsupportedOperationException();
            };
}
