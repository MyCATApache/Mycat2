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
package io.mycat.calcite.rules;


import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatConverterRule;
import io.mycat.calcite.MycatRules;
import io.mycat.calcite.physical.MycatUnion;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert an {@link Union} to a
 * {@link MycatUnionRule}.
 */
public class MycatUnionRule extends MycatConverterRule {

    /**
     * Creates a MycatUnionRule.
     */
    public MycatUnionRule(MycatConvention out,
                          RelBuilderFactory relBuilderFactory) {
        super(Union.class, (Predicate<RelNode>) r -> true, MycatRules.convention, out,
                relBuilderFactory, "MycatUnionRule");
    }

    public RelNode convert(RelNode rel) {
        final Union union = (Union) rel;
        final RelTraitSet traitSet =
                union.getTraitSet().replace(out);
        return  MycatUnion.create( traitSet,
                convertList(union.getInputs(), out), union.all);
    }
}
