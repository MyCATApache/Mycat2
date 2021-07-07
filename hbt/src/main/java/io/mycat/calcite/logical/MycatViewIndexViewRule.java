package io.mycat.calcite.logical;

import com.google.common.collect.ImmutableMap;
import io.mycat.calcite.localrel.LocalRules;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableScan;

public class MycatViewIndexViewRule extends RelRule<MycatViewIndexViewRule.Config> {

    public static final MycatViewIndexViewRule.Config DEFAULT_CONFIG = LocalRules.CalcViewRule.Config.EMPTY.as(MycatViewIndexViewRule.Config.class).withOperandFor();

    public MycatViewIndexViewRule(MycatViewIndexViewRule.Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        MycatView mycatRel = call.rel(0);
        ImmutableMap.Builder<RelNode, RelNode> builder = ImmutableMap.builder();
        for (RelNode relNode : mycatRel.produceIndexViews(call.builder())) {
            builder.put(relNode,mycatRel);
        }
        call.transformTo(mycatRel,builder.build());
    }

    public interface Config extends RelRule.Config {
        @Override
        default MycatViewIndexViewRule toRule() {
            return new MycatViewIndexViewRule(this);
        }

        default MycatViewIndexViewRule.Config withOperandFor() {
            return withOperandSupplier(b0 ->
                    b0.operand(MycatView.class).predicate(mycatView -> {
                        RelNode relNode = mycatView.getRelNode();
                        if(relNode instanceof Filter){
                            return ((Filter) relNode).getInput() instanceof TableScan;
                        }
                        return false;
                    }).noInputs())
                    .withDescription("MycatViewIndexViewRule")
                    .as(MycatViewIndexViewRule.Config.class);
        }
    }
}
