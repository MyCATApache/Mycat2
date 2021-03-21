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
import io.mycat.calcite.physical.MycatHashAggregate;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule to convert a {@link Aggregate}
 * to a {@link MycatAggregateRule}.
 */
public class MycatAggregateRule extends MycatConverterRule {

    /**
     * Creates a MycatAggregateRule.
     */
    public MycatAggregateRule(MycatConvention out,
                              RelBuilderFactory relBuilderFactory) {
        super(Aggregate.class, (Predicate<RelNode>) r -> true, MycatRules.IN_CONVENTION,
                out, relBuilderFactory, "MycatAggregateRule");
    }

    public RelNode convert(RelNode rel) {
        final Aggregate agg = (Aggregate) rel;
        final RelTraitSet traitSet =
                agg.getTraitSet().replace(out);
        return MycatHashAggregate.create(traitSet,
                convert(agg.getInput(), out), agg.getGroupSet(),
                agg.getGroupSets(), agg.getAggCallList());
    }
}
