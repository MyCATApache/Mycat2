package io.mycat.hbt3;

import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.jetbrains.annotations.NotNull;

import java.util.List;


public class View extends AbstractRelNode implements MycatRel {
    RelNode relNode;
    DataNodeInfo dataNode;

    public View(RelTraitSet relTrait,RelNode input,DataNodeInfo dataNode) {
        super(input.getCluster(), input.getTraitSet());
        this.dataNode = dataNode;
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = relTrait;
    }

    public static View of(RelNode input) {
        return new View(input.getTraitSet().replace(MycatConvention.INSTANCE),input,null);
    }

    public static RelNode of(RelNode input, DataNodeInfo dataNodeInfo) {
        return new View(input.getTraitSet().replace(MycatConvention.INSTANCE),input,dataNodeInfo);
    }

    @NotNull
    public String getSql() {
        return MycatCalciteSupport.INSTANCE.convertToSql(relNode,
                MysqlSqlDialect.DEFAULT, false);
    }

//    @Override
//    public String toString() {
//        return super.toString() +" "+ dataNode+ " -> " + getSql();
//    }

    public RelNode getRelNode() {
        return relNode;
    }

    public DataNodeInfo getDataNode() {
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