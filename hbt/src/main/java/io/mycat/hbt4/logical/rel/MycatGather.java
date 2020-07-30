package io.mycat.hbt4.logical.rel;

import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;

import java.util.List;

public class MycatGather extends Union implements MycatRel {

    protected MycatGather(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
        super(cluster, traits, inputs, true);
    }

    public static MycatGather create(RelTraitSet traits, List<RelNode> inputs) {
        assert !inputs.isEmpty();
        RelOptCluster cluster = inputs.get(0).getCluster();
        return new MycatGather(cluster, traits.replace(MycatConvention.INSTANCE), inputs);
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
    public MycatGather copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        assert !all;
        return new MycatGather(getCluster(), traitSet, inputs);
    }
}