package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.logic.MycatConvention;
import io.mycat.calcite.relBuilder.MycatTransientSQLTable;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.tools.RelBuilder;

public class MycatRelBuilder extends RelBuilder {
    public MycatRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }
    public static RelNode makeTransientSQLScan(RelBuilder relBuilder, String targetName, RelNode input) {
        RelDataType rowType = input.getRowType();
        MycatConvention convention = MycatConvention.of(targetName, MysqlSqlDialect.DEFAULT);
        MycatTransientSQLTable transientTable = new MycatTransientSQLTable(convention, input);
        RelOptTable relOptTable = RelOptTableImpl.create(
                relBuilder.getRelOptSchema(),
                rowType,
                transientTable,
                ImmutableList.of(targetName,String.valueOf(input.getId())));
        return LogicalTableScan.create(relBuilder.getCluster(), relOptTable);
    }
    public  RelNode makeTransientSQLScan(String targetName, RelNode input) {
        return makeTransientSQLScan(this,targetName,input);
    }

}