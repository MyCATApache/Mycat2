package io.mycat.optimizer.logical;

import com.google.common.collect.ImmutableList;
import io.mycat.optimizer.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
     * Implementation of {@link Project} in
     * {@link MycatConvention Mycat calling convention}.
     */
    public  class MycatProject
            extends Project
            implements MycatRel {
        public MycatProject(
                RelOptCluster cluster,
                RelTraitSet traitSet,
                RelNode input,
                List<? extends RexNode> projects,
                RelDataType rowType) {
            super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
            assert getConvention() instanceof MycatConvention;
        }


        @Override
        public MycatProject copy(RelTraitSet traitSet, RelNode input,
                                 List<RexNode> projects, RelDataType rowType) {
            return new MycatProject(getCluster(), traitSet, input, projects, rowType);
        }

        @Override
        public RelOptCost computeSelfCost(RelOptPlanner planner,
                                          RelMetadataQuery mq) {
            return super.computeSelfCost(planner, mq)
                    .multiplyBy(MycatConvention.COST_MULTIPLIER);
        }



        @Override
        public ExplainWriter explain(ExplainWriter writer) {
          writer.name("MycatProject").item("projects", this.exps).into();
          ((MycatRel) getInput()).explain(writer);
          return writer.ret();
      }


        @Override
        public Executor implement(ExecutorImplementor implementor) {
            return implementor.implement(this);
        }
    }