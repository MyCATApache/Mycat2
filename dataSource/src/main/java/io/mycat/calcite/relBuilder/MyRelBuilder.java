package io.mycat.calcite.relBuilder;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.logic.MycatConvention;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.tools.RelBuilder;


/**
 * chen junwen
 */
public class MyRelBuilder {
    public static RelNode makeTransientSQLScan(RelBuilder relBuilder,String targetName, RelNode input) {
        RelDataType rowType = input.getRowType();
        MycatConvention convention = MycatConvention.of(targetName, MysqlSqlDialect.DEFAULT);
        MycatTransientSQLTable transientTable = new MycatTransientSQLTable(convention, input);
        RelOptTable relOptTable = RelOptTableImpl.create(
                relBuilder.getRelOptSchema(),
                rowType,
                transientTable,
                ImmutableList.of(targetName,String.valueOf(input.getId())));
        return new MycatTransientSQLTableScan(relBuilder.getCluster(), convention, relOptTable, input);
    }
}