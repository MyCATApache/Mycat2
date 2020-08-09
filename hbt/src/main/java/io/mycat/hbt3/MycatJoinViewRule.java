package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;

public class MycatJoinViewRule extends RelOptRule {
    public final static MycatJoinViewRule INSTANCE = new MycatJoinViewRule();

    public MycatJoinViewRule() {
        super(operand(LogicalJoin.class,
                operand(View.class, none()),
                operand(View.class, none())), "MycatFilterViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin up = call.rel(0);
        RelNode leftBase = call.rel(1);
        RelNode rightBase = call.rel(2);
        if (RelMdSqlViews.join(leftBase)&&RelMdSqlViews.join(rightBase)) {
            RelNode res = SQLRBORewriter.join(leftBase,rightBase,up);
            if (res != null) {
                call.transformTo(res);
            }
        }
    }
}