/**
 * Copyright (C) <2021>  <chen junwen>
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


import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.IndexRBORewriter;
import io.mycat.calcite.rewriter.OptimizationContext;
import io.mycat.calcite.logical.MycatView;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Rule to convert a {@link MycatView} to
 * an {@link MycatViewToIndexViewRule}.
 */
public class MycatViewToIndexViewRule extends RelOptRule {
    private final OptimizationContext optimizationContext;
    private final List<Object> params;

    public MycatViewToIndexViewRule(OptimizationContext optimizationContext, List<Object> params) {
        super(operand(MycatView.class, none()), "MycatFilterViewRule");
        this.optimizationContext = optimizationContext;
        this.params = params;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MycatView view = call.rel(0);
        Distribution distribution = view.getDistribution();
        Distribution.Type type = distribution.type();
        if (!(type==Distribution.Type.Sharding)) {
            return;
        }
        RelNode relNode = view.getRelNode();
        IndexRBORewriter<Object> indexRboRewriter = new IndexRBORewriter<>();
        RelNode res = relNode.accept(indexRboRewriter);
        if (indexRboRewriter.isApply()) {
            call.transformTo(res);
            return;
        }
    }
}