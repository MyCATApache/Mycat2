package io.mycat.hbt4.logical;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

;

/**
     * Aggregate operator implemented in Mycat convention.
     */
    public  class MycatAggregate extends Aggregate implements MycatRel {
        public MycatAggregate(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                ImmutableBitSet groupSet,
                List<ImmutableBitSet> groupSets,
                List<AggregateCall> aggCalls) {
            super(cluster, traitSet, ImmutableList.of(), input, groupSet, groupSets, aggCalls);
            assert getConvention() instanceof MycatConvention;
        }

        @Override
        public MycatAggregate copy(RelTraitSet traitSet, RelNode input,
                                   ImmutableBitSet groupSet,
                                   List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
            return new MycatAggregate(getCluster(), traitSet, input,
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