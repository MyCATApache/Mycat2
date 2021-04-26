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


import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatConverterRule;
import io.mycat.calcite.MycatRules;
import io.mycat.calcite.physical.MycatCalc;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rex.RexMultisetUtil;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Calc} to an
 * {@link MycatCalcRule}.
 */
public class MycatCalcRule extends MycatConverterRule {

    public static final MycatCalcRule INSTANCE = new MycatCalcRule(MycatConvention.INSTANCE, RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a MycatCalcRule.
     */
    public MycatCalcRule(MycatConvention out,
                         RelBuilderFactory relBuilderFactory) {
        super(Calc.class, (Predicate<Calc>) r -> !r.containsOver(), MycatRules.IN_CONVENTION,
                out, relBuilderFactory, "MycatCalcRule");
    }

    public RelNode convert(RelNode rel) {
        final Calc calc = (Calc) rel;

        // If there's a multiset, let FarragoMultisetSplitter work on it
        // first.
        if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
            return null;
        }

        return MycatCalc.create(rel.getTraitSet().replace(out),
                convert(calc.getInput(), calc.getTraitSet().replace(out)),
                calc.getProgram());
    }
}
