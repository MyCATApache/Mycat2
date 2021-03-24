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
import io.mycat.calcite.physical.MycatTableSpool;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableTableSpool;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalRepeatUnion;
import org.apache.calcite.rel.logical.LogicalTableSpool;
import org.apache.calcite.tools.RelBuilderFactory;

public class MycatTableSpoolRule extends MycatConverterRule {


    public MycatTableSpoolRule(final MycatConvention out,
                               RelBuilderFactory relBuilderFactory) {
        super(LogicalTableSpool.class,(p)->true,   MycatRules.IN_CONVENTION, out, relBuilderFactory, "MycatTableSpoolRule");
    }

    @Override public RelNode convert(RelNode rel) {
        LogicalTableSpool spool = (LogicalTableSpool) rel;
        return MycatTableSpool.create(
                convert(spool.getInput(),
                        spool.getInput().getTraitSet().replace(MycatConvention.INSTANCE)),
                spool.readType,
                spool.writeType,
                spool.getTable());
    }
}
