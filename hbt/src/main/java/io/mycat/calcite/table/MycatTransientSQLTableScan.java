package io.mycat.calcite.table;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;

import java.util.List;
import java.util.function.Supplier;

public class MycatTransientSQLTableScan extends TableScan {
    final Supplier<String> sql;
    final String targetName;

    public MycatTransientSQLTableScan(RelOptCluster cluster, String targetName, RelOptTable relOptTable, Supplier<String> sql) {
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
        return sql.get();
    }

}