package io.mycat.calcite.rules;

import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.OptimizationContext;
import io.mycat.calcite.rewriter.RelMdSqlViews;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;

public class MycatFilterViewRule extends RelOptRule {

    private OptimizationContext optimizationContext;

    public MycatFilterViewRule(OptimizationContext optimizationContext) {
        super(operand(LogicalFilter.class, operand(MycatView.class, none())), "MycatFilterViewRule");
        this.optimizationContext = optimizationContext;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter up = call.rel(0);
        RelNode base = call.rel(1);
        if (RelMdSqlViews.filter(base)) {
            RelNode res = SQLRBORewriter.filter(base, up,optimizationContext);
            if (res != null) {
                call.transformTo(res);
            }
        }
    }
}