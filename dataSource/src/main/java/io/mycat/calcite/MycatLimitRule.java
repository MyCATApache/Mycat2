package io.mycat.calcite;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;

public class MycatLimitRule extends RelOptRule {
    public MycatLimitRule() {
        super(operand(Sort.class, any()), MycatLimitRule.class.getName());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final Sort sort = call.rel(0);
        if (sort.offset == null && sort.fetch == null) {
            return;
        }
        RelNode input = sort.getInput();
        if (!sort.getCollation().getFieldCollations().isEmpty()) {
            // Create a sort with the same sort key, but no offset or fetch.
            input = sort.copy(
                    sort.getTraitSet(),
                    input,
                    sort.getCollation(),
                    null,
                    null);
        }
    }
}