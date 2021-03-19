package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.logical.MycatView;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;

import java.util.Optional;

public  class MycatProjectJoinClusteringRule extends RelRule<MycatProjectJoinClusteringRule.Config> {

        public MycatProjectJoinClusteringRule(Config config) {
            super(config);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            final LogicalProject origProject = call.rel(0);
            final LogicalJoin origJoin = call.rel(1);
            final RelNode left = call.rel(2);
            final RelNode right = call.rel(3);
            Optional<RelNode> joinOptional = SQLRBORewriter.bottomJoin(left, right, origJoin);
            if (joinOptional.isPresent()) {
                RelNode relNode = joinOptional.get();
                Project newProject = (Project)origProject.copy(origProject.getTraitSet(), ImmutableList.of(relNode));
                if (relNode instanceof MycatView){
                    call.transformTo(newProject.accept(new SQLRBORewriter()));
                   return;
                }
                call.transformTo(newProject);
            }
        }

        public interface Config extends RelRule.Config {
            MycatProjectJoinClusteringRule.Config DEFAULT = EMPTY.as(MycatProjectJoinClusteringRule.Config.class)
                    .withOperandFor(LogicalJoin.class);

            @Override
            default MycatProjectJoinClusteringRule toRule() {
                return new MycatProjectJoinClusteringRule(this);
            }

            default MycatProjectJoinClusteringRule.Config withOperandFor(Class<? extends Join> joinClass) {
                return withOperandSupplier(b0 ->
                        b0.operand(LogicalProject.class)
                                .oneInput(j->j.operand(joinClass).inputs(
                                        b1 -> b1.operand(MycatView.class).anyInputs(),
                                        b2 -> b2.operand(MycatView.class).anyInputs())))
                        .as(MycatProjectJoinClusteringRule.Config.class);
            }
        }
    }
