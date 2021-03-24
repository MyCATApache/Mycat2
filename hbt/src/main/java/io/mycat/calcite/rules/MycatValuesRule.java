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
import io.mycat.calcite.physical.MycatValues;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule that converts a values operator to Mycat.
 */
public class MycatValuesRule extends MycatConverterRule {
    /**
     * Creates a MycatValuesRule.
     */
    public MycatValuesRule(MycatConvention out,
                           RelBuilderFactory relBuilderFactory) {
        super(Values.class, (Predicate<RelNode>) r -> true, MycatRules.IN_CONVENTION,
                out, relBuilderFactory, "MycatValuesRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        Values values = (Values) rel;
        return  MycatValues.create(values.getCluster(), values.getRowType(),
                values.getTuples(), values.getTraitSet().replace(out));
    }
}