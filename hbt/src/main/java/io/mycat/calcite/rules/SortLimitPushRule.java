package io.mycat.calcite.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.tools.RelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * chenjunwen
 * <p>
 */
public class SortLimitPushRule extends RelOptRule {
    private static final Logger LOGGER = LoggerFactory.getLogger(SortLimitPushRule.class);
    boolean apply = false;
    public SortLimitPushRule() {
        super(
                operand(Sort.class,
                        operand(Union.class, any())),
                RelFactories.LOGICAL_BUILDER, "SortLimitPushRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        if (apply){
            return;
        }
        List<RelNode> parents = call.getParents();
        RelBuilder builder = call.builder();
        builder.clear();
        final Sort sort = call.rel(0);
        if (parents != null || sort.isDistinct() ||
                (sort.getChildExps() == null || sort.getChildExps().isEmpty())) {
            return;
        }
        final Union union = call.rel(1);
        if (!union.isDistinct()) {
            int size = union.getInputs().size();
            if (!union.getInputs().isEmpty()){
                RelNode input = union.getInput(0);
                if(input instanceof Sort) {
                    return;
                }
            }
            ArrayList<RelNode> newNodes = new ArrayList<>(2);
            for (RelNode input : union.getInputs()) {
                newNodes.add(sort.copy(sort.getTraitSet(), ImmutableList.of(input)));
            }
            if (!newNodes.isEmpty()) {
                builder.pushAll(newNodes);
                builder.union(true, newNodes.size());
                call.transformTo(sort.copy(sort.getTraitSet(), ImmutableList.of(builder.build())));
                apply = true;
            }
        }

    }
}
