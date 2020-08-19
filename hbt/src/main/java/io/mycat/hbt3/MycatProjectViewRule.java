package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;

public class MycatProjectViewRule extends RelOptRule {
    public final static MycatProjectViewRule INSTANCE = new MycatProjectViewRule();

    public MycatProjectViewRule() {
        super(operand(LogicalProject.class, operand(View.class, none())), "MycatFilterViewRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject up = call.rel(0);
        RelNode base = call.rel(1);
        if (RelMdSqlViews.project(base)) {
            RelNode res = SQLRBORewriter.project(base, up);
            if (res != null) {
                call.transformTo(res);
            }
        }
    }
}