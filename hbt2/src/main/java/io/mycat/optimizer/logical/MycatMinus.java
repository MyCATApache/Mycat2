package io.mycat.optimizer.logical;

import io.mycat.optimizer.Executor;
import io.mycat.optimizer.ExecutorImplementor;
import io.mycat.optimizer.ExplainWriter;
import io.mycat.optimizer.MycatRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;

import java.util.List;

/**
 * Minus operator implemented in Mycat convention.
 */
public class MycatMinus extends Minus implements MycatRel {
    public MycatMinus(RelOptCluster cluster, RelTraitSet traitSet,
                      List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
        assert !all;
    }

    public MycatMinus copy(RelTraitSet traitSet, List<RelNode> inputs,
                           boolean all) {
        return new MycatMinus(getCluster(), traitSet, inputs, all);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatMinus").into();
        for (RelNode input : getInputs()) {
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