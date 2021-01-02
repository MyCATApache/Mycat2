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
package io.mycat.calcite.physical;

import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.EnumerableSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Sort operator implemented in Mycat convention.
 */
public class MycatMemSort
        extends Sort
        implements MycatRel {
    protected MycatMemSort(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch) {
        super(cluster, traitSet, input, collation, offset, fetch);
        assert getConvention() instanceof MycatConvention;
        assert getConvention() == input.getConvention();
    }

    public static MycatMemSort create(
            RelTraitSet traitSet,
            RelNode input,
            RelCollation collation,
            RexNode offset,
            RexNode fetch) {
        return new MycatMemSort(input.getCluster(), traitSet.replace(MycatConvention.INSTANCE), input, collation, offset, fetch);
    }

    @Override
    public MycatMemSort copy(RelTraitSet traitSet, RelNode newInput,
                             RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new MycatMemSort(getCluster(), traitSet, newInput, newCollation,
                offset, fetch);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.9);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatSort").item("offset", offset).item("limit", fetch).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        EnumerableSort enumerableSort = EnumerableSort.create(getInput(), getCollation(), offset, fetch);
        return enumerableSort.implement(implementor, pref);
    }
}
