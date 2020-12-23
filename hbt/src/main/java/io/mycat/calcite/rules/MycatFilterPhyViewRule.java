package io.mycat.calcite.rules;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.OptimizationContext;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

public class MycatFilterPhyViewRule extends RelOptRule {

    private OptimizationContext optimizationContext;

    public MycatFilterPhyViewRule(OptimizationContext optimizationContext) {
        super(operand(LogicalFilter.class, operand(LogicalTableScan.class, none())), "MycatFilterPhyViewRule");
        this.optimizationContext = optimizationContext;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        LogicalTableScan input = call.rel(1);
        RexNode condition = filter.getCondition();
        MycatLogicTable mycatLogicTable = input.getTable().unwrap(MycatLogicTable.class);
        if (mycatLogicTable!=null){
            Distribution distribution = mycatLogicTable.computeDataNode(ImmutableList.of(condition));
            if (optimizationContext!=null&&distribution.isPartial()){
                optimizationContext.setPredicateOnPhyView(true);
            }
            Iterable<DataNode> dataNodes;
            if (optimizationContext!=null){
                optimizationContext.setPredicateOnPhyView(true);
                dataNodes= distribution.getDataNodes(optimizationContext.params);
            }else {
                dataNodes = distribution.getDataNodes();
            }
            RelBuilder builder = call.builder();
            builder.clear();
            int count  = 0;
            for (DataNode dataNode : dataNodes) {
                MycatPhysicalTable mycatPhysicalTable = new MycatPhysicalTable(mycatLogicTable , dataNode);
                RelOptTableImpl relOptTable1 = RelOptTableImpl.create(call.builder().getRelOptSchema(),
                        input.getRowType(),
                        mycatPhysicalTable,
                        ImmutableList.of(dataNode.getTargetName(),dataNode.getSchema(), dataNode.getTable())
                );
                builder.push(LogicalTableScan.create(input.getCluster(), relOptTable1, ImmutableList.of())
                ).filter(filter.getCondition());
                count++;
            }
            call.transformTo(builder.union(true, count).build());
        }


    }
}