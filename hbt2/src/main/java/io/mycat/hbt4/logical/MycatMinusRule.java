package io.mycat.hbt4.logical;

import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Minus} to a
 * {@link MycatMinusRule}.
 */
public class MycatMinusRule extends MycatConverterRule {
    /**
     * Creates a MycatMinusRule.
     */
    public MycatMinusRule(MycatConvention out,
                          RelBuilderFactory relBuilderFactory) {
        super(Minus.class, (Predicate<RelNode>) r -> true, MycatRules.convention, out,
                relBuilderFactory, "MycatMinusRule");
    }

    public RelNode convert(RelNode rel) {
        final Minus minus = (Minus) rel;
        if (minus.all) {
            return null; // EXCEPT ALL not implemented
        }
        final RelTraitSet traitSet =
                rel.getTraitSet().replace(out);
        return new MycatMinus(rel.getCluster(), traitSet,
                convertList(minus.getInputs(), out), false);
    }
}
