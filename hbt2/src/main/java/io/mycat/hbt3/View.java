package io.mycat.hbt3;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;

import java.util.List;
import java.util.Map;


public class View extends AbstractRelNode implements MycatRel {
    RelNode relNode;
    PartInfo dataNode;

    public View(RelTraitSet relTrait, RelNode input, PartInfo dataNode) {
        super(input.getCluster(), input.getTraitSet());
        this.dataNode = dataNode;
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = relTrait;
    }

    public static View of(RelNode input) {
        return new View(input.getTraitSet().replace(MycatConvention.INSTANCE),input,null);
    }

    public static RelNode of(RelNode input, PartInfo dataNodeInfo) {
        return new View(input.getTraitSet().replace(MycatConvention.INSTANCE),input,dataNodeInfo);
    }


    public String getSql() {
        return MycatCalciteSupport.INSTANCE.convertToSql(relNode,
                MysqlSqlDialect.DEFAULT, false);
    }
    public String getSql(Map<String,Object> context) {
        return getSql();//@todo
    }

//    @Override
//    public String toString() {
//        return super.toString() +" "+ dataNode+ " -> " + getSql();
//    }

    public RelNode getRelNode() {
        return relNode;
    }

    public PartInfo getDataNode() {
        return dataNode;
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        if (!inputs.isEmpty()){
            throw new AssertionError();
        }
        return new View(traitSet,relNode, dataNode);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("View").into().item("sql",getSql()).ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}