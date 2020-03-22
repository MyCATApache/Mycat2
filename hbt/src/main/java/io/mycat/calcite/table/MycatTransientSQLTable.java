package io.mycat.calcite.table;

import io.mycat.QueryBackendTask;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.resultset.MyCatResultSetEnumerable;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlDialect;

import java.util.Collections;
import java.util.List;


/**
 * chenjunwen
 */
public class MycatTransientSQLTable extends PreComputationSQLTable
        implements TransientTable, TranslatableTable {
    private final MycatConvention convention;
    private final RelNode input;
    private boolean forUpdate;

    public MycatTransientSQLTable(MycatConvention convention, RelNode input,boolean forUpdate) {
        this.input = input;
        this.convention = convention;
        this.forUpdate = forUpdate;
    }

    public String getExplainSQL() {
        return getExplainSQL(input);
    }

    public String getExplainSQL(RelNode input) {
        SqlDialect dialect = convention.dialect;
        return MycatCalciteSupport.INSTANCE.convertToSql(input, dialect,forUpdate);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.copyType(input.getRowType());
    }

    public String getTargetName() {
        return convention.targetName;
    }

    @Override
    public String getSql() {
        return getExplainSQL();
    }

    @Override
    public List<Object> params() {
        return Collections.emptyList();
    }

    public RelNode getRelNode() {
        return input;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new MycatTransientSQLTableScan(context.getCluster(), convention, relOptTable, () -> getExplainSQL());
    }


    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        MycatCalciteDataContext root1 = (MycatCalciteDataContext) root;
        Enumerable<Object[]> preComputation = root1.getPreComputation(this);
        if (preComputation!=null){
            return preComputation;
        }
        String sql = getExplainSQL();
        return new MyCatResultSetEnumerable(root1, new QueryBackendTask(sql, convention.targetName));

    }

}