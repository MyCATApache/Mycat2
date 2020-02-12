package io.mycat.calcite.logic;

import io.mycat.QueryBackendTask;
import io.mycat.calcite.MyCatResultSetEnumerable;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatImplementor;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

import java.util.List;


/**
 * chenjunwen
 */
public class MycatTransientSQLTable extends AbstractTable
        implements TransientTable, ProjectableFilterableTable, TranslatableTable {
    private final MycatConvention convention;
    private final RelNode input;

    public MycatTransientSQLTable(MycatConvention convention, RelNode input) {
        this.input = input;
        this.convention = convention;
    }

    public String getExplainSQL() {
        return getExplainSQL(input);
    }

    public String getExplainSQL(RelNode input) {
        String sql = new MycatImplementor(convention.dialect).implement(input).asStatement().toSqlString(convention.dialect, false).getSql();
        sql = sql.replaceAll("\r", " ");
        sql = sql.replaceAll("\n", " ");
        return sql;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.copyType(input.getRowType());
    }

    public String getTargetName() {
        return convention.targetName;
    }

    public RelNode getRelNode() {
        return input;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new MycatTransientSQLTableScan(context.getCluster(), convention, relOptTable, input);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters, int[] projects) {
        String sql = getExplainSQL();
        return new MyCatResultSetEnumerable((MycatCalciteDataContext) root, new QueryBackendTask(sql, convention.targetName));
    }
}