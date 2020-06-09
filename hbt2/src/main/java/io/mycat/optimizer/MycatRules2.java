package io.mycat.optimizer;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.tools.RelBuilder;

public class MycatRules2 {

    public static class FilterView extends RelOptRule {
        public static final FilterView INSTACNE = new FilterView();

        public FilterView() {
            super(operand(MycatRules.MycatFilter.class,operand(BottomView.class,none())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Filter filter = call.rel(0);
            BottomView bottomView = call.rel(1);
            RelNode relNode = bottomView.getRelNode();

            RelBuilder builder = call.builder();
            if (relNode == null) builder.push(bottomView);
            else {
                builder.push(relNode);
            }
            RelNode res = builder.filter(filter.getVariablesSet(), filter.getChildExps()).build();
            BottomView newBottomView = BottomView.create(
                    bottomView.getCluster(),
                    bottomView.getTable(),
                    res
            );
            call.transformTo(newBottomView);
        }
    }
    public static class ProjectView extends RelOptRule {
        public static final ProjectView INSTACNE = new ProjectView();

        public ProjectView() {
            super(operand(MycatRules.MycatProject.class,operand(BottomView.class,none())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Project project = call.rel(0);
            BottomView bottomView = call.rel(1);
            RelNode relNode = bottomView.getRelNode();

            RelBuilder builder = call.builder();
            if (relNode == null) builder.push(bottomView);
            else {
                builder.push(relNode);
            }
            RelNode res = builder.project(project.getChildExps()).build();
            BottomView newBottomView = BottomView.create(
                    bottomView.getCluster(),
                    bottomView.getTable(),
                    res
            );
            call.transformTo(newBottomView);
        }
    }
}