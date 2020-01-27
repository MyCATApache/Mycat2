package io.mycat.calcite.relBuilder;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.tools.RelBuilder;


/**
 * chen junwen
 */
public class MyRelBuilder {
    public static RelNode makeTransientSQLScan(RelBuilder relBuilder, String tableName, RelNode input) {
        tableName = tableName + "$" + input.getId();
        RelDataType rowType = input.getRowType();
        MycatTransientSQLTable transientTable = new MycatTransientSQLTable(tableName, input);
        RelOptTable relOptTable = RelOptTableImpl.create(
                relBuilder.getRelOptSchema(),
                rowType,
                transientTable,
                ImmutableList.of(tableName));
        RelNode scan = relBuilder.getScanFactory().createScan(relBuilder.getCluster(), relOptTable);
        relBuilder.push(scan);
        relBuilder.rename(rowType.getFieldNames());
        return relBuilder.build();
    }
}