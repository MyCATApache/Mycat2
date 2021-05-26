package io.mycat.calcite.localrel.rules;

import io.mycat.calcite.localrel.LocalTableScan;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;

public class ToViewTableScanRule extends RelRule<ToViewTableScanRule.Config> {

        public ToViewTableScanRule(ToViewTableScanRule.Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final TableScan logicalTableScan = call.rel(0);
            LocalTableScan localTableScan = LocalTableScan.create(logicalTableScan);
            call.transformTo(localTableScan);
        }

      static   public interface Config extends RelRule.Config {
          Config DEFAULT = EMPTY.as(ToViewTableScanRule.Config.class)
                    .withOperandFor();

            @Override
            default ToViewTableScanRule toRule() {
                return new ToViewTableScanRule(this);
            }

            default Config withOperandFor() {
                return withOperandSupplier(b0 ->
                        b0.operand(LogicalTableScan.class).anyInputs())
                        .as(ToViewTableScanRule.Config.class);
            }
        }
    }
