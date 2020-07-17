package io.mycat.calcite.table;

import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExecutorImplementor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.hbt4.MycatRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;

public class MycatTransientSQLTableScan extends TableScan implements MycatRel {
    final String sql;
    final String targetName;

    public MycatTransientSQLTableScan(RelOptCluster cluster, String targetName, RelOptTable relOptTable, String sql) {
        super(cluster, cluster.traitSetOf(io.mycat.hbt4.MycatConvention.INSTANCE), relOptTable);
        this.sql = sql;
        this.targetName = targetName;
    }


    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new MycatTransientSQLTableScan(
                getCluster(), targetName, getTable(), sql);
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
                .item("target",targetName)
                .item("sql",sql)
                .ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}