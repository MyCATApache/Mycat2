package io.mycat.hbt4.logical.rules;

import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.rel.MycatMinus;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class MycatMinusRule extends MycatConverterRule {
    /**
     * Creates a MycatMinusRule.
     */
    public MycatMinusRule(MycatConvention out,
                          RelBuilderFactory relBuilderFactory) {
        super(Minus.class, (Predicate<RelNode>) r -> true, MycatRules.convention,
                out, relBuilderFactory, "MycatMinusRule");
    }

    public RelNode convert(RelNode rel) {
        final Minus minus = (Minus) rel;
        final RelTraitSet traitSet =
                minus.getTraitSet().replace(out);
        return MycatMinus.create(traitSet,
                convertList(minus.getInputs(), out), minus.all);
    }
}
