package io.mycat.hbt4.logical;

import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;

;

/**
     * Implementation of {@link Filter} in
     * {@link MycatConvention Mycat calling convention}.
     */
    public  class MycatFilter extends Filter implements MycatRel {
        public MycatFilter(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                RexNode condition) {
            super(cluster, traitSet, input, condition);
            assert getConvention() instanceof MycatConvention;
        }

        public MycatFilter copy(RelTraitSet traitSet, RelNode input,
                                RexNode condition) {
            return new MycatFilter(getCluster(), traitSet, input, condition);
        }

      @Override
      public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatFilter").item("condition", condition).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
      }


        @Override
        public Executor implement(ExecutorImplementor implementor) {
            return implementor.implement(this);
        }
    }