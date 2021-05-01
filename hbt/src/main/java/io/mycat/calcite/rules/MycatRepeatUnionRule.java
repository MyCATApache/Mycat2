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
import io.mycat.calcite.physical.MycatRepeatUnion;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.tools.RelBuilderFactory;

public class MycatRepeatUnionRule extends MycatConverterRule {

    public static final MycatRepeatUnionRule INSTANCE = new MycatRepeatUnionRule(MycatConvention.INSTANCE, RelFactories.LOGICAL_BUILDER);
    /** Called from the Config. */
    public MycatRepeatUnionRule(final MycatConvention out,
                                RelBuilderFactory relBuilderFactory) {
        super(LogicalRepeatUnion.class,(p)->true,   MycatRules.IN_CONVENTION, out, relBuilderFactory, "MycatRepeatUnionRule");
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalRepeatUnion union = (LogicalRepeatUnion) rel;
        RelTraitSet traitSet = union.getTraitSet().replace(out);
        RelNode seedRel = union.getSeedRel();
        RelNode iterativeRel = union.getIterativeRel();

        return new MycatRepeatUnion(
                rel.getCluster(),
                traitSet,
                convert(seedRel, seedRel.getTraitSet().replace(out)),
                convert(iterativeRel, iterativeRel.getTraitSet().replace(out)),
                union.all,
                union.iterationLimit);
    }
}
