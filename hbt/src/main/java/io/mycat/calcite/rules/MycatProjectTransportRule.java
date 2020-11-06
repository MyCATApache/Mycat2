package io.mycat.calcite.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rex.RexOver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MycatProjectTransportRule extends ProjectSetOpTransposeRule {
    public static final MycatProjectTransportRule INSTANCE = new MycatProjectTransportRule();
    private static final Logger LOGGER = LoggerFactory.getLogger(MycatProjectTransportRule.class);

    public MycatProjectTransportRule() {
        super(expr -> !(expr instanceof RexOver),
                RelFactories.LOGICAL_BUILDER);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        try {
            super.onMatch(call);
        } catch (Throwable e) {
            LOGGER.error("", e);
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        try {
            return super.matches(call);
        } catch (Throwable e) {
            LOGGER.error("", e);
        }
        return false;
    }
}