package io.mycat.hbt4;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;

import java.util.List;

public class QueryView extends Union implements MycatRel{
    protected QueryView(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
        super(cluster, traits, inputs, true);
    }

    @Override
    public QueryView copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new QueryView(getCluster(),traitSet,inputs);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
         writer.name("Query").into();
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