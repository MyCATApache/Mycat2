package io.mycat.hbt3;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalUnion;

import java.util.List;

public class MycatUnionMegreRule extends RelOptRule {
    public static MycatUnionMegreRule INSTANCE = new MycatUnionMegreRule();

    public MycatUnionMegreRule() {
        super(operand(LogicalUnion.class, unordered(operand(LogicalUnion.class, any()))));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalUnion union = call.rel(0);
        LogicalUnion bunion = call.rel(1);
        return union.all && bunion.all;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalUnion union = call.rel(0);
        List<RelNode> childRels = call.getChildRels(union);
        for (RelNode childRel : childRels) {

        }

    }
}