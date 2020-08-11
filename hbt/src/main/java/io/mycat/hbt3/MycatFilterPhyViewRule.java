package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;

public class MycatFilterPhyViewRule extends RelOptRule {
    public final static MycatFilterPhyViewRule INSTANCE = new MycatFilterPhyViewRule();

    public MycatFilterPhyViewRule() {
        super(operand(LogicalFilter.class, operand(LogicalTableScan.class, none())), "MycatFilterPhyViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalTableScan input = call.rel(1);
        RexNode condition = filter.getCondition();
        MycatLogicTable nodes = input.getTable().unwrap(MycatLogicTable.class);
        if (nodes!=null){
            Distribution distribution = nodes.computeDataNode(ImmutableList.of(condition));
            List<DataNode> dataNodes = distribution.getDataNodes();
            RelBuilder builder = call.builder();
            for (DataNode dataNode : dataNodes) {
                MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(nodes , dataNode);
                RelOptTableImpl relOptTable1 = RelOptTableImpl.create(call.builder().getRelOptSchema(),
                        input.getRowType(),
                        mycatPhysicalTable,
                        ImmutableList.of(dataNode.getTargetName(), dataNode.getSchema(), dataNode.getTable())
                );
                builder.push(LogicalTableScan.create(input.getCluster(), relOptTable1, ImmutableList.of())
                ).filter(filter.getCondition());
            }
            call.transformTo(builder.union(true, dataNodes.size()).build());
        }


    }
}