package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;

public class MycatFilterViewRule extends RelOptRule {
    public final static MycatFilterViewRule INSTANCE = new MycatFilterViewRule();

    public MycatFilterViewRule() {
        super(operand(LogicalFilter.class, operand(View.class, none())), "MycatFilterViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter up = call.rel(0);
        RelNode base = call.rel(1);
        if (RelMdSqlViews.filter(base)) {
            RelNode res = SQLRBORewriter.filter(base, up);
            if (res != null) {
                call.transformTo(res);
            }
        }
    }
}