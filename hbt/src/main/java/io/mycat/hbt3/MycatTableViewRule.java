package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class MycatTableViewRule extends RelOptRule {
    public final static MycatTableViewRule INSTANCE = new MycatTableViewRule();

    public MycatTableViewRule() {
        super(operand(LogicalTableScan.class, none()), "MycatTableViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        Distribution partInfo;
        View view;
        RelOptTable table = rel.getTable();
        AbstractMycatTable mycatTable = table.unwrap(AbstractMycatTable.class);
        partInfo = mycatTable.computeDataNode();
        view = View.of(rel, partInfo);
        call.transformTo(view);
    }
}