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

import io.mycat.calcite.physical.MycatGather;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class MycatGatherRemoveRule extends RelOptRule {
    public final static MycatGatherRemoveRule INSTANCE = new MycatGatherRemoveRule();

    public MycatGatherRemoveRule() {
        super(operand(MycatGather.class, operand(MycatGather.class, any())), "MycatGatherRule");
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MycatGather first = call.rel(0);
        MycatGather second = call.rel(1);
        if (first.getInputs().size() == 1 && second.getInputs().size() == 1) {
            call.transformTo(second);
        }
    }
}