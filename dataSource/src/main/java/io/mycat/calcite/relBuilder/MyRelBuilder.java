package io.mycat.calcite.relBuilder;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.DataNodeSqlConverter;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.SqlImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.RelBuilder;


/**
 * chen junwen
 */
public class MyRelBuilder {
    public static RelNode makeTransientSQLScan(RelBuilder relBuilder,String targetName, String tableName, RelNode input) {
        tableName = tableName + "$" + input.getId();
        RelDataType rowType = input.getRowType();
        DataNodeSqlConverter dataNodeSqlConverter = new DataNodeSqlConverter();
        SqlImplementor.Result visit = dataNodeSqlConverter.visitChild(0, input);
        SqlNode sqlNode = visit.asStatement();
        MycatTransientSQLTable transientTable = new MycatTransientSQLTable(targetName,tableName, input,sqlNode.toString());
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