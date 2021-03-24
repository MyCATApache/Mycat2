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
package io.mycat.calcite.rewriter;

import io.mycat.calcite.logical.MycatView;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;

import java.util.Optional;

public class MycatJoinClusteringRule extends RelRule<MycatJoinClusteringRule.Config> {

    public MycatJoinClusteringRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalJoin origJoin = call.rel(0);
        final RelNode left = call.rel(1);
        final RelNode right = call.rel(2);
        Optional<RelNode> joinOptional = SQLRBORewriter.bottomJoin(left, right, origJoin);
        if (joinOptional.isPresent()) {
            call.transformTo(joinOptional.get());
        }
    }

    public interface Config extends RelRule.Config {
        MycatJoinClusteringRule.Config DEFAULT = EMPTY.as(MycatJoinClusteringRule.Config.class)
                .withOperandFor(LogicalJoin.class);

        @Override
        default MycatJoinClusteringRule toRule() {
            return new MycatJoinClusteringRule(this);
        }

        default MycatJoinClusteringRule.Config withOperandFor(Class<? extends Join> joinClass) {
            return withOperandSupplier(b0 ->
                    b0.operand(joinClass).inputs(
                            b2 -> b2.operand(MycatView.class).anyInputs(),
                            b3 -> b3.operand(MycatView.class).anyInputs()))
                    .as(MycatJoinClusteringRule.Config.class);
        }
    }
}
