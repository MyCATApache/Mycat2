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
import io.mycat.calcite.physical.MycatMemSort;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Sort} to an
 * {@link MycatMemSortRule}.
 */
public class MycatMemSortRule extends MycatConverterRule {

    public static final MycatMemSortRule INSTANCE = new MycatMemSortRule(MycatConvention.INSTANCE, RelFactories.LOGICAL_BUILDER);
    /**
     * Creates a MycatSortRule.
     */
    public MycatMemSortRule(MycatConvention out,
                            RelBuilderFactory relBuilderFactory) {
        super(Sort.class, (Predicate<RelNode>) r -> true, MycatRules.IN_CONVENTION, out,
                relBuilderFactory, "MycatSortRule");
    }

    public RelNode convert(RelNode rel) {
        return convert((Sort) rel);
    }

    /**
     * Converts a {@code Sort} into a {@code MycatSort}.
     *
     * @param sort Sort operator to convert
     * @return A new MycatSort
     */
    public RelNode convert(Sort sort) {
        final RelTraitSet traitSet = sort.getTraitSet().replace(out);

        final RelNode input;
        input = sort.getInput();
        return MycatMemSort.create(traitSet,
                convert(input, out), sort.getCollation(), sort.offset, sort.fetch);
    }
}

  