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
package io.mycat.hbt4.logical.rules;


import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.MycatConverterRule;
import io.mycat.hbt4.MycatRules;
import io.mycat.hbt4.logical.MycatFilter;
import io.mycat.hbt4.physical.rules.CheckingUserDefinedFunctionVisitor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Filter} to
 * an {@link MycatFilterRule}.
 */
public class MycatFilterRule extends MycatConverterRule {

    /**
     * Creates a MycatFilterRule.
     */
    public MycatFilterRule(MycatConvention out,
                           RelBuilderFactory relBuilderFactory) {
        super(Filter.class,
                (Predicate<Filter>) r -> !userDefinedFunctionInFilter(r),
                MycatRules.convention, out, relBuilderFactory, "MycatFilterRule");
    }

    private static boolean userDefinedFunctionInFilter(Filter filter) {
        CheckingUserDefinedFunctionVisitor visitor = new CheckingUserDefinedFunctionVisitor();
        filter.getCondition().accept(visitor);
        return visitor.containsUserDefinedFunction();
    }

    public RelNode convert(RelNode rel) {
        final Filter filter = (Filter) rel;

        return new MycatFilter(
                rel.getCluster(),
                rel.getTraitSet().replace(out),
                convert(filter.getInput(),
                        filter.getInput().getTraitSet().replace(out)),
                filter.getCondition());
    }
}

