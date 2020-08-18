package io.mycat.hbt3;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatSqlDialect;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;

import java.util.Collections;
import java.util.List;

public class MycatLookUpView extends AbstractRelNode implements MycatRel {

    private final View relNode;

    public MycatLookUpView(View relNode) {
        super(relNode.getCluster(), relNode.getTraitSet().replace(MycatConvention.INSTANCE));
        this.relNode = relNode;
        this.rowType = relNode.getRowType();
    }


    public static MycatLookUpView create(View relNode){
        return new MycatLookUpView(relNode);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatLookUpView")
                .item("sql", MycatCalciteSupport.INSTANCE.convertToSql(relNode, MycatSqlDialect.DEFAULT,false, Collections.emptyList()))
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

    public View getRelNode() {
        return relNode;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatLookUpView(relNode);
    }
}