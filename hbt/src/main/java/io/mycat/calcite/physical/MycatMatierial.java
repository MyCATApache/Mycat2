package io.mycat.calcite.physical;

import io.mycat.calcite.Executor;
import io.mycat.calcite.ExecutorImplementor;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.RxBuiltInMethod;

public class MycatMatierial extends SingleRel implements MycatRel {

    private final MycatRel input;

    protected MycatMatierial(RelOptCluster cluster, RelTraitSet traits, MycatRel input) {
        super(cluster, traits, input);
        this.input = input;
        this.rowType = input.getRowType();
    }
    public static final MycatMatierial create( MycatRel input){
        return create(input.getCluster(),input.getTraitSet(),input);
    }
    public static final MycatMatierial create(RelOptCluster cluster, RelTraitSet traits, MycatRel input) {
        return new MycatMatierial(cluster, traits, input);
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
        return false;
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final BlockBuilder builder = new BlockBuilder();
        final Result result =
                input.implement(implementor, pref);
        Expression input = builder.append("child", result.block);
        final Expression childExp = toEnumerate(input);
        builder.add(Expressions.call(RxBuiltInMethod.ENUMERABLE_MATIERIAL.method, childExp));
        return implementor.result(result.physType, builder.toBlock());
    }
}
