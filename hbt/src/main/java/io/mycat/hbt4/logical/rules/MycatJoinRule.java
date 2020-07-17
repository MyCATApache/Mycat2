/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hbt4.logical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.MycatHashJoin;
import io.mycat.hbt4.logical.MycatNestedLoopJoin;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rex.*;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.ArrayList;
import java.util.Arrays;
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
        JoinInfo info = join.analyzeCondition();
        RelOptCluster cluster = join.getCluster();
        RexBuilder rexBuilder = cluster.getRexBuilder();
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        final List<RelNode> newInputs = new ArrayList<>();
        for (RelNode input : join.getInputs()) {
            if (convertInputTraits && input.getConvention() != getOutTrait()) {
                input = convert(input, input.getTraitSet().replace(out));
            }
            newInputs.add(input);
        }
        left = newInputs.get(0);
        right = newInputs.get(1);

        final boolean hasEquiKeys = !info.leftKeys.isEmpty()
                && !info.rightKeys.isEmpty();
        if (hasEquiKeys) {
            final RexNode equi = info.getEquiCondition(left, right, rexBuilder);
            final RexNode condition;
            if (info.isEqui()) {
                condition = equi;
            } else {
                final RexNode nonEqui = RexUtil.composeConjunction(rexBuilder, info.nonEquiConditions);
                condition = RexUtil.composeConjunction(rexBuilder, Arrays.asList(equi, nonEqui));
            }
            return MycatHashJoin.create(
                    left,
                    right,
                    condition,
                    join.getVariablesSet(),
                    join.getJoinType());
        }
        return new MycatNestedLoopJoin(
                join.getCluster(),
                join.getTraitSet().replace(out),
                left,
                right,
                join.getCondition(),
                join.getVariablesSet(),
                join.getJoinType());
    }

    /**
     * Returns whether a condition is supported by {@link MycatNestedLoopJoin}.
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