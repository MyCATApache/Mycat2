package io.mycat.optimizer;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;

import java.util.List;

public class GatherView extends Union implements MycatRel{
    protected GatherView(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
        super(cluster, traits, inputs, true);
    }

    @Override
    public GatherView copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new GatherView(getCluster(),traitSet,inputs);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
         writer.name("Gather").into();
        List<RelNode> inputs = getInputs();
        for (RelNode input : inputs) {
            MycatRel rel = (MycatRel) input;
            rel.explain(writer);
        }
       return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}