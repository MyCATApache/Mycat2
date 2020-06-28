package io.mycat.optimizer;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.TableHandler;
import io.mycat.calcite.table.MycatLogicTable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.schema.ProjectableFilterableTable;
import org.apache.calcite.schema.TranslatableTable;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.stream.Collectors;

import static io.mycat.optimizer.BottomView.makeTransient;

public class MycatTable extends MycatLogicTable implements TranslatableTable , ProjectableFilterableTable {

    public MycatTable(TableHandler t) {
        super(t);
    }

    @Override
    public RelNode toRel(
            RelOptTable.ToRelContext context,
            RelOptTable relOptTable) {
//        final RelOptCluster cluster = context.getCluster();
//        RelNode relNode = BottomTable.create(cluster, relOptTable);
//        List<DataNode> dataNodes = getDataNodes();
//        return makeTransient(relOptTable.getRelOptSchema(), relNode, dataNodes);
       return LogicalTableScan.create(context.getCluster(),relOptTable,ImmutableList.of());
    }


    public List<DataNode> getDataNodes(){
        return this.getPhysicalTableMap().values().stream().map(i->i.getBackendTableInfo()).collect(Collectors.toList());
    }
}