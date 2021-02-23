package io.mycat.calcite.physical;

import io.mycat.calcite.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;

public class MycatGather extends SingleRel implements MycatRel {

    protected MycatGather(RelOptCluster cluster, RelTraitSet traits, RelNode input) {
        super(cluster, traits, input);
    }


    public static MycatGather create(RelNode input) {
        RelOptCluster cluster = input.getCluster();
        return new MycatGather(cluster, input.getTraitSet().replace(MycatConvention.INSTANCE), input);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatGather");
        for (RelNode input : getInputs()) {
            MycatRel mycatRel = (MycatRel) input;
            mycatRel.explain(writer);
        }
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatGather(getCluster(), traitSet, inputs.get(0));
    }

    @Override
    public boolean isSupportStream() {
        return true;
    }
}