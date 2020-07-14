/**
 * Copyright (C) <2020>  <chen junwen>
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
