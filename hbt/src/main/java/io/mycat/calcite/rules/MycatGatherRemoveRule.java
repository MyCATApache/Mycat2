package io.mycat.calcite.rules;

import io.mycat.calcite.physical.MycatGather;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class MycatGatherRemoveRule extends RelOptRule {
    public final static MycatGatherRemoveRule INSTANCE = new MycatGatherRemoveRule();

    public MycatGatherRemoveRule() {
        super(operand(MycatGather.class, operand(MycatGather.class, any())), "MycatGatherRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MycatGather first = call.rel(0);
        MycatGather second = call.rel(1);
        if (first.getInputs().size() == 1 && second.getInputs().size() == 1) {
            call.transformTo(second);
        }
    }
}