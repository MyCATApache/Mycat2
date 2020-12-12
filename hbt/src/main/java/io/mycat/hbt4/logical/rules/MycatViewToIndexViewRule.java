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


import io.mycat.hbt3.Distribution;
import io.mycat.hbt3.IndexRBORewriter;
import io.mycat.hbt3.OptimizationContext;
import io.mycat.hbt3.View;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Rule to convert a {@link View} to
 * an {@link MycatViewToIndexViewRule}.
 */
public class MycatViewToIndexViewRule extends RelOptRule {
    private final OptimizationContext optimizationContext;
    private final List<Object> params;

    public MycatViewToIndexViewRule(OptimizationContext optimizationContext, List<Object> params) {
        super(operand(View.class, none()), "MycatFilterViewRule");
        this.optimizationContext = optimizationContext;
        this.params = params;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        View view = call.rel(0);
        Distribution distribution = view.getDistribution();
        if (!distribution.isSharding()) {
            return;
        }
        RelNode relNode = view.getRelNode();
        IndexRBORewriter<Object> indexRboRewriter = new IndexRBORewriter<>(optimizationContext, params);
        RelNode res = relNode.accept(indexRboRewriter);
        if (indexRboRewriter.isApply()) {
            call.transformTo(res);
            return;
        }
    }
}