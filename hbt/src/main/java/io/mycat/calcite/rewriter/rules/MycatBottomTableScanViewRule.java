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
package io.mycat.calcite.rewriter.rules;

import io.mycat.calcite.rewriter.SQLRBORewriter;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class MycatBottomTableScanViewRule extends RelRule<MycatBottomTableScanViewRule.Config> {

    public MycatBottomTableScanViewRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final TableScan logicalTableScan = call.rel(0);
        SQLRBORewriter.view(logicalTableScan).ifPresent(relNode -> call.transformTo(relNode));

    }

    public interface Config extends RelRule.Config {
        MycatBottomTableScanViewRule.Config DEFAULT = EMPTY.as(MycatBottomTableScanViewRule.Config.class)
                .withOperandFor();

        @Override
        default MycatBottomTableScanViewRule toRule() {
            return new MycatBottomTableScanViewRule(this);
        }

        default MycatBottomTableScanViewRule.Config withOperandFor() {
            return withOperandSupplier(b0 ->
                    b0.operand(TableScan.class).anyInputs())
                    .as(MycatBottomTableScanViewRule.Config.class);
        }
    }
}
