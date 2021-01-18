package io.mycat.calcite.physical;

import io.mycat.calcite.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

public class MycatMatierial extends SingleRel implements MycatRel {

    private final MycatRel input;

    protected MycatMatierial(RelOptCluster cluster, RelTraitSet traits, MycatRel input) {
        super(cluster, traits, input);
        this.input = input;
        this.rowType = input.getRowType();
    }

    public static final MycatMatierial create(RelOptCluster cluster, RelTraitSet traits, MycatRel input){
        return new MycatMatierial(cluster,traits,input);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatMatierial").into();
        for (RelNode relNode : getInputs()) {
            MycatRel relNode1 = (MycatRel) relNode;
            relNode1.explain(writer);
        }
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return input.implement(implementor);
    }

    @Override
    public boolean isSupportStream() {
        return true;
    }
}
