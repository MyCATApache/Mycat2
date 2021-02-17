package io.mycat.calcite.physical;

import io.mycat.calcite.*;
import io.mycat.calcite.logical.MycatView;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import java.util.Collections;
import java.util.List;

public class MycatLookUpView extends AbstractRelNode implements MycatRel {

    private final MycatView relNode;

    public MycatLookUpView(MycatView relNode) {
        super(relNode.getCluster(), relNode.getTraitSet().replace(MycatConvention.INSTANCE));
        this.relNode = relNode;
        this.rowType = relNode.getRowType();
    }


    public static MycatLookUpView create(MycatView relNode){
        return new MycatLookUpView(relNode);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatLookUpView")
                .item("sql", MycatCalciteSupport.INSTANCE
                        .convertToSql(relNode, MycatSqlDialect.DEFAULT,false))
                .into().ret();
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw);
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    public MycatView getRelNode() {
        return relNode;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatLookUpView(relNode);
    }

    @Override
    public boolean isSupportStream() {
        return false;
    }
}