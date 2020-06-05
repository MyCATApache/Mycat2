/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.tools.RelBuilder;

import java.util.HashSet;
import java.util.List;

/**
 * @author Junwen Chen
 **/
/*
RexSimplify 简化行表达式
SubstitutionVisitor 物化结合
MaterializedViewSubstitutionVisitor
 */
public class UnionRule extends RelOptRule {
    final HashSet<String> context = new HashSet<>();

    public UnionRule() {
        super(operandJ(Union.class, null, input -> input.getInputs().size() > 2, any()), "UnionRule");
    }


    /**
     * @param call todo result set with order，backend
     */
    @Override
    public void onMatch(RelOptRuleCall call) {
        RelBuilder builder = call.builder();
        Union union = (Union) call.rels[0];
        List<RelNode> inputs = union.getInputs();

        if (inputs.size() > 2) {
            call.transformTo(inputs.stream().reduce((relNode1, relNode2) -> {
                builder.clear();
                builder.push(relNode1);
                builder.push(relNode2);
                return builder.union(!union.isDistinct()).build();
            }).get());
        }
    }
}