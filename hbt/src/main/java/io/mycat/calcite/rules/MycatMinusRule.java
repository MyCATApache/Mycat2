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
import io.mycat.calcite.physical.MycatMinus;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class MycatMinusRule extends MycatConverterRule {


    public static final MycatMinusRule INSTANCE = new MycatMinusRule(MycatConvention.INSTANCE, RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a MycatMinusRule.
     */
    public MycatMinusRule(MycatConvention out,
                          RelBuilderFactory relBuilderFactory) {
        super(Minus.class, (Predicate<RelNode>) r -> true, MycatRules.IN_CONVENTION,
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
