package io.mycat.optimizer.logical.rules;

import io.mycat.optimizer.MycatConvention;
import io.mycat.optimizer.MycatConverterRule;
import io.mycat.optimizer.MycatRules;
import io.mycat.optimizer.logical.MycatJoin;
import org.apache.calcite.rel.InvalidRelException;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Rule that converts a join to Mycat.
 */
public class MycatJoinRule extends MycatConverterRule {

    /**
     * Creates a MycatJoinRule.
     */
    public MycatJoinRule(MycatConvention out,
                         RelBuilderFactory relBuilderFactory) {
        super(Join.class, (Predicate<RelNode>) r -> true, MycatRules.convention,
                out, relBuilderFactory, "MycatJoinRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final Join join = (Join) rel;
                return convert(join, true);
    }

    /**
     * Converts a {@code Join} into a {@code MycatJoin}.
     *
     * @param join               Join operator to convert
     * @param convertInputTraits Whether to convert input to {@code join}'s
     *                           Mycat convention
     * @return A new MycatJoin
     */
    public RelNode convert(Join join, boolean convertInputTraits) {
        final List<RelNode> newInputs = join.getInputs();
//        for (RelNode input : join.getInputs()) {
//            if (convertInputTraits && input.getConvention() != getOutTrait()) {
//                input =
//                        convert(input,
//                                input.getTraitSet().replace(out));
//            }
//            newInputs.add(input);
//        }
//        if (convertInputTraits && !canJoinOnCondition(join.getCondition())) {
//            return null;
//        }
        try {
            return new MycatJoin(
                    join.getCluster(),
                    join.getTraitSet().replace(out),
                    newInputs.get(0),
                    newInputs.get(1),
                    join.getCondition(),
                    join.getVariablesSet(),
                    join.getJoinType());
        } catch (InvalidRelException e) {

            return null;
        }
    }

    /**
     * Returns whether a condition is supported by {@link MycatJoin}.
     *
     * <p>Corresponds to the capabilities of
     *
     * @param node Condition
     * @return Whether condition is supported
     */
    private boolean canJoinOnCondition(RexNode node) {
        final List<RexNode> operands;
        switch (node.getKind()) {
            case AND:
            case OR:
                operands = ((RexCall) node).getOperands();
                for (RexNode operand : operands) {
                    if (!canJoinOnCondition(operand)) {
                        return false;
                    }
                }
                return true;

            case EQUALS:
            case IS_NOT_DISTINCT_FROM:
            case NOT_EQUALS:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
                operands = ((RexCall) node).getOperands();
                if ((operands.get(0) instanceof RexInputRef)
                        && (operands.get(1) instanceof RexInputRef)) {
                    return true;
                }
                // fall through

            default:
                return false;
        }
    }
}