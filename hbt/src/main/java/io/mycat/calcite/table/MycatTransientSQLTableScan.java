package io.mycat.calcite.table;

import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExecutorImplementor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.hbt4.MycatRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class MycatTransientSQLTableScan extends AbstractRelNode implements MycatRel {
    final String sql;
    final String targetName;

    public MycatTransientSQLTableScan(RelOptCluster cluster, RelDataType relDataType, String targetName, String sql) {
        super(cluster, cluster.traitSetOf(io.mycat.hbt4.MycatConvention.INSTANCE));
        this.targetName = targetName;
        this.sql = sql;
        this.rowType = relDataType;
    }


    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatTransientSQLTableScan(getCluster(), this.rowType, targetName, sql);
    }

    public String getSql() {
        return sql;
    }

    public String getTargetName() {
        return targetName;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return writer.name("MycatTransientSQLTableScan").into()
                .item("target", targetName)
                .item("sql", sql)
                .ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}