package io.mycat.calcite.physical;

import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.util.RxBuiltInMethod;
import org.apache.calcite.util.RxBuiltInMethodImpl;

import java.lang.reflect.Type;

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

    @Override
    public Result implementStream(StreamMycatEnumerableRelImplementor implementor, Prefer pref) {
        final JavaTypeFactory typeFactory = implementor.getTypeFactory();
        final BlockBuilder builder = new BlockBuilder();
        final EnumerableRel child = (EnumerableRel) getInput();

        final Result result =
                implementor.visitChild(this, 0, child, pref);

        final PhysType physType =
                PhysTypeImpl.of(
                        typeFactory, getRowType(), pref.prefer(result.format));

        Expression inputObservalbe = builder.append(
                "inputObservalbe", result.block, false);
        builder.add( Expressions.call(inputObservalbe,
                RxBuiltInMethod.OBSERVABLE_TO_ENUMERABLE.getMethodName(),inputObservalbe));
        return implementor.result(physType, builder.toBlock());
    }
}
