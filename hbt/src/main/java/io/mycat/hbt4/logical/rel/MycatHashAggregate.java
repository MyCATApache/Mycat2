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
package io.mycat.hbt4.logical.rel;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;


/**
 * Aggregate operator implemented in Mycat convention.
 */
public class MycatHashAggregate extends Aggregate implements MycatRel {
    protected MycatHashAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
        assert getConvention() instanceof MycatConvention;
    }
    public static MycatHashAggregate create(
            RelTraitSet traitSet,
            RelNode input,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        traitSet = traitSet.replace(MycatConvention.INSTANCE);
        return new MycatHashAggregate(input.getCluster(),traitSet,input,groupSet,groupSets,aggCalls);

    }
    @Override
    public MycatHashAggregate copy(RelTraitSet traitSet, RelNode input,
                                   ImmutableBitSet groupSet,
                                   List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new MycatHashAggregate(getCluster(), traitSet, input,
                groupSet, groupSets, aggCalls);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatAggregate").item("groupSets", groupSets).item("aggCalls", aggCalls).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }


    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}