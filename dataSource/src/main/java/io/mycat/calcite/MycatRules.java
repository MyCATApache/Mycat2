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
package io.mycat.calcite;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.FilterTableScanRule;

import static org.apache.calcite.plan.RelOptRule.*;

public class MycatRules {
    public static final FilterTableScanRule INSTANCE =
            new FilterTableScanRule(
                    operand(Filter.class,
                            operandJ(TableScan.class, null, FilterTableScanRule::test,
                                    none())),
                    RelFactories.LOGICAL_BUILDER,
                    "FilterTableScanRule") {
                public void onMatch(RelOptRuleCall call) {
                    //org.apache.calcite.rel.rules.FilterTableScanRule
                    final Filter filter = call.rel(0);
                    final TableScan scan = call.rel(1);
                    apply(call, filter, scan);
                }

                @Override
                protected void apply(RelOptRuleCall call, Filter filter, TableScan scan) {
                    super.apply(call, filter, scan);
                }
            };
}