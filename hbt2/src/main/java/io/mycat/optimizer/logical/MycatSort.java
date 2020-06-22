package io.mycat.optimizer.logical;

import io.mycat.optimizer.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Sort operator implemented in Mycat convention.
 */
public class MycatSort
        extends Sort
        implements MycatRel {
    public MycatSort(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
        assert getConvention() instanceof MycatConvention;
        assert getConvention() == input.getConvention();
    }

    @Override
    public MycatSort copy(RelTraitSet traitSet, RelNode newInput,
                          RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new MycatSort(getCluster(), traitSet, newInput, newCollation,
                offset, fetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.9);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatSort").item("offset", offset).item("limit", fetch).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}
