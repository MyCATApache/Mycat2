package io.mycat.hbt3;

import io.mycat.hbt4.*;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;

public class MycatLookUpView extends AbstractRelNode implements MycatRel {

    private final RelNode relNode;

    public MycatLookUpView(RelNode relNode) {
        super(relNode.getCluster(), relNode.getTraitSet().replace(MycatConvention.INSTANCE));
        this.relNode = relNode;
        this.rowType = relNode.getRowType();
    }


    public static MycatLookUpView create(RelNode relNode){
        return new MycatLookUpView(relNode);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatLookUpView").into().ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    public RelNode getRelNode() {
        return relNode;
    }
}