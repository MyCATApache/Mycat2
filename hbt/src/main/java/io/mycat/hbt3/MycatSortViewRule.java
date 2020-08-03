package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;

public class MycatSortViewRule extends RelOptRule {
    public final static MycatSortViewRule INSTANCE = new MycatSortViewRule();

    public MycatSortViewRule() {
        super(operand(LogicalSort.class,
                operand(View.class, none())), "MycatFilterViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalSort up = call.rel(0);
        RelNode view = call.rel(1);
        if (RelMdSqlViews.sort(up)) {
            RelNode res = SQLRBORewriter.sort(view,up);
            if (res != null) {
                call.transformTo(res);
            }
        }
    }
}