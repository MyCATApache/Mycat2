package io.mycat.optimizer;

import com.google.common.collect.ImmutableList;
import io.mycat.TableHandler;
import io.mycat.calcite.table.MycatLogicTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.TranslatableTable;

public class MycatTable extends MycatLogicTable implements TranslatableTable , ProjectableFilterableTable {

    public MycatTable(TableHandler t) {
        super(t);
    }

    @Override
    public RelNode toRel(
            RelOptTable.ToRelContext context,
            RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return  BottomView.create(cluster,relOptTable);
    }
}