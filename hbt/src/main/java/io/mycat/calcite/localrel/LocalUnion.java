package io.mycat.calcite.localrel;

import io.mycat.beans.mycat.MycatRelDataType;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class LocalUnion extends Union implements LocalRel {
    protected LocalUnion(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs, boolean all) {
        super(cluster, traits.replace(LocalConvention.INSTANCE), inputs, all);
    }

    protected LocalUnion(RelInput input) {
        super(input);
    }

    public static LocalUnion create(Union logicalUnion, List<RelNode> inputs) {
        return new LocalUnion(logicalUnion.getCluster(), logicalUnion.getTraitSet(), inputs, logicalUnion.all);
    }

    @Override
    public LocalUnion copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new LocalUnion(getCluster(), traitSet, inputs, all);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(.9);
    }

    public static final RelFactories.SetOpFactory SET_OP_FACTORY =
            (kind, inputs, all) -> {
                RelNode input = inputs.get(0);
                RelOptCluster cluster = input.getCluster();
                final RelTraitSet traitSet = cluster.traitSetOf(
                        requireNonNull(input.getConvention(), "input.getConvention()"));
                switch (kind) {
                    case UNION:
                        return new LocalUnion(cluster, traitSet, inputs, all);
                    default:
                        throw new AssertionError("unknown: " + kind);
                }
            };

    @Override
    public MycatRelDataType getMycatRelDataType() {
        return ((LocalRel) getInput(0)).getMycatRelDataType();
    }
}
