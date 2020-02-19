/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite.logic;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * @author Junwen Chen
 **/
public class MycatToEnumerableConverterRule extends ConverterRule {
    public static final MycatToEnumerableConverterRule INSTANCE =
            new MycatToEnumerableConverterRule(RelFactories.LOGICAL_BUILDER);

    /**
     * Creates a CassandraToEnumerableConverterRule.
     *
     * @param relBuilderFactory Builder for relational expressions
     */
    public MycatToEnumerableConverterRule(
            RelBuilderFactory relBuilderFactory) {
        super(RelNode.class, (Predicate<RelNode>) r -> true,
                Convention.NONE, EnumerableConvention.INSTANCE,
                relBuilderFactory, "MycatToEnumerableConverterRule");
    }
    @Override
    public RelNode convert(RelNode rel) {
        RelTraitSet newTraitSet = rel.getTraitSet().replace(getOutConvention());
        return new MycatToEnumerableConverter(rel.getCluster(), newTraitSet, rel);
    }
}