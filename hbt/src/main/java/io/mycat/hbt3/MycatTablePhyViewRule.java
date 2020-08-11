package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class MycatTablePhyViewRule extends RelOptRule {
    public final static MycatTablePhyViewRule INSTANCE = new MycatTablePhyViewRule();

    public MycatTablePhyViewRule() {
        super(operand(LogicalTableScan.class, none()), "MycatTableViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        Distribution partInfo;
        RelOptTable table = rel.getTable();
        AbstractMycatTable mycatTable = table.unwrap(AbstractMycatTable.class);
        if (mycatTable != null) {
            partInfo = mycatTable.computeDataNode();
            call.transformTo(View.of(rel, partInfo).expandToPhyRelNode());
        }
    }
}