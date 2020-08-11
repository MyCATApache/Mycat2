package io.mycat.hbt3;

import com.google.common.collect.ImmutableMap;
import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.logical.rel.MycatGather;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;

public class MycatGatherCreateRule extends RelOptRule {
    public final static MycatGatherCreateRule ON_VIEW =
            new MycatGatherCreateRule(operandJ(View.class, MycatConvention.INSTANCE,(v)-> {
                return !v.isGather();
            },none()),"MycatGatherCreateRule");
//    public final static MycatGatherCreateRule ON_VIEW =
//            new MycatGatherCreateRule(operand(View.class,none()),"MycatGatherCreateRule");

    public MycatGatherCreateRule(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        View rel = call.rel(0);
        return !rel.isGather();
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        View rel = call.rel(0);
        if (!rel.isGather()&&rel.getDistribution().isSingle()){
            View view = View.of(rel.getTraitSet(),rel.getRelNode(), rel.getDistribution(), true);
            MycatGather mycatGather = MycatGather.create(rel.getTraitSet(),view);
            call.transformTo(mycatGather, ImmutableMap.of(rel,mycatGather));
        }
    }
}