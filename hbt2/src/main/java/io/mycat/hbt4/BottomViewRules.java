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
package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import io.mycat.DataNode;
import io.mycat.calcite.rules.PushDownLogicTableRule;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class BottomViewRules {

    public static class FilterView extends RelOptRule {
        public static final FilterView INSTACNE = new FilterView();

        public FilterView() {
            super(operand(Filter.class, operand(BottomView.class, none())));
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

            BottomView newBottomView;
            //不分片直接下推
            if (bottomView.getDataNodes().size() == 1) {
                newBottomView = BottomView.makeTransient(bottomView.getTable(), res);
            } else {
                HepProgramBuilder builder1 = new HepProgramBuilder();
                builder1.addRuleInstance(FilterTableScanRule.INSTANCE);
                HepPlanner planner = new HepPlanner(builder1.build());
                planner.setRoot(res);
                RelNode bestExp = planner.findBestExp();
                List<DataNode> dataNodes = new ArrayList<>();
                builder1 = new HepProgramBuilder();
                builder1.addRuleInstance(PushDownLogicTableRule.BindableTableScan);
                planner = new HepPlanner(builder1.build());

                builder1 = new HepProgramBuilder();
                builder1.addRuleInstance(PushDownLogicTableRule.BindableTableScan);
                planner = new HepPlanner(builder1.build());

                planner.setRoot(bestExp);
                bestExp = planner.findBestExp();
                newBottomView = BottomView.makeTransient(bottomView.getTable(), bestExp, dataNodes);
            }
            call.transformTo(newBottomView);
        }
    }

    public static class ProjectView extends RelOptRule {
        public static final ProjectView INSTANCE = new ProjectView();

        public ProjectView() {
            super(operand(Project.class, operand(BottomView.class, none())));
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
            BottomView newBottomView = BottomView.makeTransient(bottomView.getTable(),
                    res
            );
            call.transformTo(newBottomView);
        }
    }

    public static class AggregateView extends RelOptRule {
        public static final AggregateView INSTACNE = new AggregateView();

        public AggregateView() {
            super(operand(Aggregate.class, operand(BottomView.class, none())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Aggregate aggregate = call.rel(0);
            BottomView bottomView = call.rel(1);
            RelNode relNode = bottomView.getRelNode();

            RelBuilder builder = call.builder();
            if (relNode == null) builder.push(bottomView);
            else {
                builder.push(relNode);
            }
            RelNode res = builder.aggregate(builder.groupKey(aggregate.getGroupSet()), aggregate.getAggCallList()).build();
            BottomView newBottomView = BottomView.makeTransient(
                    bottomView.getTable(),
                    res
            );
            call.transformTo(newBottomView);
        }
    }

    public static class SortView extends RelOptRule {
        public static final SortView INSTACNE = new SortView();

        public SortView() {
            super(operand(Sort.class, operand(BottomView.class, none())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {

            Sort sort = call.rel(0);
            BottomView bottomView = call.rel(1);
            RelNode relNode = bottomView.getRelNode();

            RelBuilder builder = call.builder();
            if (relNode == null) builder.push(bottomView);
            else {
                builder.push(relNode);
            }
            Sort sort1 = sort.copy(relNode.getTraitSet(), ImmutableList.of(relNode));
            RelOptCluster cluster = builder.getCluster();

            BottomView newBottomView = BottomView.makeTransient(
                    bottomView.getTable(),
                    sort1);

            call.transformTo(newBottomView);
        }
    }

    public static class JoinView extends RelOptRule {
        public static final CorrelateView INSTANCE = new CorrelateView();

        public JoinView() {
            super(operand(Join.class,
                    operand(BottomView.class, none()),
                    operand(BottomView.class, none())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Join join = call.rel(0);
            BottomView left = call.rel(1);
            BottomView right = call.rel(1);

            JoinInfo joinInfo = join.analyzeCondition();
            if (joinInfo.isEqui()) {
                if (left.isSamePartition(right)) {
                    Join newJoin = join.copy(join.getTraitSet(), ImmutableList.of(left.getRelNode(), right.getRelNode()));
                    call.transformTo(BottomView.create(left.getCluster(), left.getTable()));
                }
            }
        }
    }

    public static class CorrelateView extends RelOptRule {
        public static final CorrelateView INSTANCE = new CorrelateView();

        public CorrelateView() {
            super(operand(Correlate.class,
                    operand(BottomView.class, none()),
                    operand(BottomView.class, none())));
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
            Correlate correlate = call.rel(0);
            BottomView left = call.rel(1);
            BottomView right = call.rel(1);
            if (left.isSamePartition(right)) {
                Correlate newCorrelate = correlate.copy(correlate.getTraitSet(), ImmutableList.of(left.getRelNode(), right.getRelNode()));
                call.transformTo(BottomView.create(left.getCluster(), left.getTable()));
            }
        }
    }

    public static class JoinRearrangeView extends RelOptRule {
        public static final JoinRearrangeView INSTANCE = new JoinRearrangeView(operandJ(Join.class,
                null,
                input -> Optional.ofNullable(input).map(i -> i.analyzeCondition()).map(i -> i.isEqui()).orElse(false),
                unordered(
                        operand(BottomView.class, none()),
                        operandJ(Join.class,
                                null,
                                input -> Optional.ofNullable(input).map(i -> i.analyzeCondition()).map(i -> i.isEqui()).orElse(false),
                                some(operand(BottomView.class, none())))))
        );

        public JoinRearrangeView(RelOptRuleOperand operand) {
            super(operand);
        }

        @Override
        public void onMatch(RelOptRuleCall call) {
//            Join join = call.rel(0);
//            ArrayList<BottomView> list = new ArrayList<>();
//            collectView(join, list);

        }

//        private void collectView(List<RelNode> inputs , ArrayList<BottomView> bottomViews,ArrayList<IntPair> columns) {
//            for (RelNode input : inputs) {
//                if (input instanceof BottomView) {
//                    bottomViews.add((BottomView)input);
//                }else if (input instanceof Join){
//                    Join input1 = (Join) input;
//                    List<IntPair> pairs = input1.analyzeCondition().pairs();
//                    columns.addAll(pairs);
//                    collectView(input1.getInputs(),bottomViews);
//                }
//            }
//        }
    }

}