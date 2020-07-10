package io.mycat.hbt3;

import io.mycat.calcite.MycatCalciteSupport;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.jetbrains.annotations.NotNull;

import java.util.List;


public class View extends AbstractRelNode {
    RelNode relNode;
    DataNodeInfo dataNode;

    protected View(RelNode input,DataNodeInfo dataNode) {
        super(input.getCluster(), input.getTraitSet());
        this.dataNode = dataNode;
        this.rowType = input.getRowType();
        this.relNode = input;
        this.traitSet = input.getTraitSet();
    }

    public static View of(RelNode input) {
        return new View(input,null);
    }

    public static RelNode of(RelNode input, DataNodeInfo dataNodeInfo) {
        return new View(input,dataNodeInfo);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        String sql = getSql();
        return super.explainTerms(pw).item("sql", sql);
    }

    @NotNull
    public String getSql() {
        return MycatCalciteSupport.INSTANCE.convertToSql(relNode,
                MysqlSqlDialect.DEFAULT, false);
    }

    @Override
    public String toString() {
        return super.toString() +" "+ dataNode+ " -> " + getSql();
    }

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
        View view = new View(relNode, dataNode);
        view.traitSet = traitSet;
        return view;
    }
}