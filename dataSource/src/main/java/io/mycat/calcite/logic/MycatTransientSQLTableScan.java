package io.mycat.calcite.logic;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatImplementor;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.schema.Table;

import java.util.List;

public class MycatTransientSQLTableScan extends TableScan {
    private final RelNode input;
    public MycatTransientSQLTableScan(RelOptCluster cluster, MycatConvention convention, RelOptTable relOptTable, RelNode input) {
        super(cluster, cluster.traitSetOf(convention).replace(Convention.NONE)
                .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
                    final Table table = relOptTable.unwrap(Table.class);
                    if (table != null) {
                        return table.getStatistic().getCollations();
                    }
                    return ImmutableList.of();
                }), relOptTable);
        this.input = input;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
       return pw.item("sql",getTable().unwrap(MycatTransientSQLTable.class).getExplainSQL());
    }
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();
        return new MycatTransientSQLTableScan(
                getCluster(), getMycatConvention(),  getTable(),input);
    }

    public SqlImplementor.Result implement() {
        MycatConvention convention = getMycatConvention();
        MycatImplementor mycatImplementor = new MycatImplementor(convention.dialect);
        return mycatImplementor.implement(input);
    }

    private MycatConvention getMycatConvention() {
        return (MycatConvention) getConvention();
    }

    public SqlImplementor.Result implement(MycatImplementor implementor) {
        return implementor.implement(input);
    }
}