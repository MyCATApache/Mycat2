package io.mycat.calcite.table;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.Table;

import java.util.List;
import java.util.function.Supplier;

public class MycatTransientSQLTableScan extends TableScan {
    final Supplier<String> sql;
    private final MycatConvention convention;

    public MycatTransientSQLTableScan(RelOptCluster cluster, MycatConvention convention, RelOptTable relOptTable, Supplier<String> sql) {
        super(cluster, cluster.traitSetOf(convention).replace(Convention.NONE)
                .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                    final Table table = relOptTable.unwrap(Table.class);
                    if (table != null) {
                        return table.getStatistic().getCollations();
                    }
                    return ImmutableList.of();
                }), relOptTable);
        this.convention = convention;
        this.sql = sql;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        return pw.item("sql", getSql());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new MycatTransientSQLTableScan(
                getCluster(), getMycatConvention(), getTable(), sql);
    }

    public String getSql() {
        return sql.get();
    }

    private MycatConvention getMycatConvention() {
        return (MycatConvention) getConvention();
    }
}