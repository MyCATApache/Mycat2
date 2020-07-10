package io.mycat.hbt4.logical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.MycatCalc;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rex.RexMultisetUtil;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
     * Rule to convert a {@link Calc} to an
     * {@link MycatCalcRule}.
     */
    public  class MycatCalcRule extends MycatConverterRule {
        /**
         * Creates a MycatCalcRule.
         */
        public MycatCalcRule(MycatConvention out,
                             RelBuilderFactory relBuilderFactory) {
            super(Calc.class, (Predicate<RelNode>) r -> true, MycatRules.convention,
                    out, relBuilderFactory, "MycatCalcRule");
        }

        public RelNode convert(RelNode rel) {
            final Calc calc = (Calc) rel;

            // If there's a multiset, let FarragoMultisetSplitter work on it
            // first.
            if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
                return null;
            }

            return new MycatCalc(rel.getCluster(), rel.getTraitSet().replace(out),
                    convert(calc.getInput(), calc.getTraitSet().replace(out)),
                    calc.getProgram());
        }
    }
