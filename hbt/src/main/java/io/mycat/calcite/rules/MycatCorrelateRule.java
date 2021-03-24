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
import io.mycat.calcite.physical.MycatCorrelate;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

public class MycatCorrelateRule extends MycatConverterRule {
    /**
     * Creates a MycatSortRule.
     */
    public MycatCorrelateRule(MycatConvention out,
                         RelBuilderFactory relBuilderFactory) {
        super(Correlate.class, (Predicate<RelNode>) r -> true, MycatRules.IN_CONVENTION, out,
                relBuilderFactory, "MycatCorrelateRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        Correlate correlate = (Correlate) rel;
        RelOptCluster cluster = rel.getCluster();
        return  MycatCorrelate.create(
                rel.getTraitSet().replace(MycatConvention.INSTANCE),
                convert(correlate.getLeft(),MycatConvention.INSTANCE),
                convert(correlate.getRight(),MycatConvention.INSTANCE),
                correlate.getCorrelationId(),
                correlate.getRequiredColumns(),
                correlate.getJoinType()
                );
    }
}