package io.mycat.calcite.relBuilder;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.rules.FilterTableScanRule;

import static org.apache.calcite.plan.RelOptRule.*;

public class MycatRules  {
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