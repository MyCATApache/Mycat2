package io.mycat.calcite.logic;

import io.mycat.QueryBackendTask;
import io.mycat.calcite.MyCatResultSetEnumerable;
import io.mycat.calcite.MycatCalciteDataContext;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.TransientTable;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;

public class MycatSQLTableScan extends AbstractTable implements ScannableTable, TransientTable, TranslatableTable {
    final RelDataType relDataType;
    final String sql;
    final MycatConvention convention;

    public MycatSQLTableScan(MycatConvention convention,RelDataType relDataType,  String sql) {
        this.relDataType = relDataType;
        this.sql = sql;
        this.convention = convention;
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new MyCatResultSetEnumerable((MycatCalciteDataContext) root, new QueryBackendTask(sql, convention.targetName));
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return relDataType;
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        return new MycatTransientSQLTableScan(context.getCluster(), convention, relOptTable,()-> sql);
    }
    public String getTargetName(){
        return convention.targetName;
    }
    public String getSql(){
        return sql;
    }
}