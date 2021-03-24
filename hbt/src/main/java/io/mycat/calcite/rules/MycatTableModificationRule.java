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
import io.mycat.calcite.physical.MycatTableModify;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Rule that converts a table-modification to Mycat.
 */
public  class MycatTableModificationRule extends MycatConverterRule {
    /**
     * Creates a MycatTableModificationRule.
     */
    public MycatTableModificationRule(MycatConvention out,
                                      RelBuilderFactory relBuilderFactory) {
        super(TableModify.class, (Predicate<RelNode>) r -> true,
                MycatRules.IN_CONVENTION, out, relBuilderFactory, "MycatTableModificationRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
        final TableModify modify =
                (TableModify) rel;
        final RelTraitSet traitSet =
                modify.getTraitSet().replace(out);
        return new MycatTableModify(
                modify.getCluster(), traitSet,
                modify.getTable(),
                modify.getCatalogReader(),
                convert(modify.getInput(), traitSet),
                modify.getOperation(),
                modify.getUpdateColumnList(),
                modify.getSourceExpressionList(),
                modify.isFlattened());
    }

}
