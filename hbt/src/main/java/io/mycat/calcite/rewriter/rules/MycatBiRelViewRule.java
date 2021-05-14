package io.mycat.calcite.rewriter.rules;

import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.*;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public abstract class MycatBiRelViewRule extends RelRule<MycatBiRelViewRule.Config> {

    public MycatBiRelViewRule(MycatBiRelViewRule.Config config) {
        super(config);
    }

    public MycatBiRelViewRule(Class<? extends BiRel> up) {
        super(MycatBiRelViewRule.Config.EMPTY.as(MycatBiRelViewRule.Config.class)
                .withOperandFor(up));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        SQLRBORewriter.on(call.rel(1), call.rel(2), call.rel(0)).ifPresent(relNode -> call.transformTo(relNode));
    }

    public interface Config extends RelRule.Config {
        default MycatBiRelViewRule.Config withOperandFor(Class<? extends BiRel> up) {
            return withOperandSupplier(b0 ->
                    b0.operand(up).inputs(b1 -> b1.operand(MycatView.class).noInputs(),
                            b1 -> b1.operand(MycatView.class).anyInputs()))
                    .withDescription("MycatBiRelViewRule_" + up.getName())
                    .as(MycatBiRelViewRule.Config.class);
        }
    }

    public static final List<RelRule> RULES = Arrays.asList(
            new MycatBiRelViewRule((Join.class)) {
            }
    );
}
