package io.mycat.calcite.rewriter;

import io.mycat.calcite.logical.MycatView;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.Optional;

public class JoinClusteringRule extends RelRule<JoinClusteringRule.Config> {

    public JoinClusteringRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalJoin origJoin = call.rel(0);
        final RelNode left = call.rel(1);
        final RelNode right = call.rel(2);
        Optional<RelNode> joinOptional = SQLRBORewriter.bottomJoin(left, right, origJoin);
        if (joinOptional.isPresent()) {
            call.transformTo(joinOptional.get());
        }
    }

    public interface Config extends RelRule.Config {
        JoinClusteringRule.Config DEFAULT = EMPTY.as(JoinClusteringRule.Config.class)
                .withOperandFor(LogicalJoin.class);

        @Override
        default JoinClusteringRule toRule() {
            return new JoinClusteringRule(this);
        }

        default JoinClusteringRule.Config withOperandFor(Class<? extends Join> joinClass) {
            return withOperandSupplier(b0 ->
                    b0.operand(joinClass).inputs(
                            b2 -> b2.operand(MycatView.class).anyInputs(),
                            b3 -> b3.operand(MycatView.class).anyInputs()))
                    .as(JoinClusteringRule.Config.class);
        }
    }
}
