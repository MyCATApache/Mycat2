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
package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.logical.rel.MycatMergeSort;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.rules.AggregateUnionTransposeRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

public class SQLRBORewriter extends RelShuttleImpl {
    final static NextConvertor nextConvertor = new NextConvertor();


    static {
        nextConvertor.put(TableScan.class,
                Project.class, Union.class, Aggregate.class, Sort.class, Filter.class, Join.class);
        nextConvertor.put(Filter.class,
                Project.class, Union.class, Aggregate.class, Sort.class);
        nextConvertor.put(Join.class,
                Project.class, Union.class, Aggregate.class, Filter.class, Join.class, Sort.class);
        nextConvertor.put(Project.class,
                Project.class, Union.class, Aggregate.class, Sort.class);
        nextConvertor.put(Aggregate.class,
                Project.class, Union.class, Filter.class, Sort.class);
    }



    public SQLRBORewriter() {
    }

    @Override
    public RelNode visit(TableScan scan) {
        AbstractMycatTable abstractMycatTable = scan.getTable().unwrap(AbstractMycatTable.class);
        if (abstractMycatTable != null) {
            return View.of(scan, abstractMycatTable.computeDataNode());
        }
        return scan;
    }

    @Override
    public RelNode visit(TableFunctionScan scan) {
        return scan;
    }

    @Override
    public RelNode visit(LogicalValues values) {
        return values;
    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        RelNode input = filter.getInput().accept(this);
        if (RelMdSqlViews.filter(input)) {
            return filter(input, filter);
        } else {
            return filter.copy(filter.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return null;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RelNode input = project.getInput().accept(this);
        if (RelMdSqlViews.project(input)) {
            return project(input, project);
        } else {
            return project.copy(project.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        List<RelNode> inputs = join.getInputs();
        RelNode left = inputs.get(0).accept(this);
        RelNode right = inputs.get(1).accept(this);
        if (RelMdSqlViews.join(left) && (RelMdSqlViews.join(right))) {
            return join(left, right, join);
        } else {
            return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
        }
    }

    @Override
    public RelNode visit(LogicalCorrelate correlate) {
        List<RelNode> inputs = correlate.getInputs();
        RelNode left = inputs.get(0).accept(this);
        RelNode right = inputs.get(1).accept(this);
        if (RelMdSqlViews.correlate(left) && (RelMdSqlViews.correlate(right))) {
            return correlate(left, right, correlate);
        } else {
            return correlate.copy(correlate.getTraitSet(), ImmutableList.of(left, right));
        }
    }

    @Override
    public RelNode visit(LogicalUnion union) {
        List<RelNode> inputs = new ArrayList<>();
        for (RelNode i : union.getInputs()) {
            RelNode accept = i.accept(this);
            inputs.add(accept);
        }
        return union(inputs, union);
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        return null;
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        return null;
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        RelNode input = aggregate.getInput().accept(this);
        if (RelMdSqlViews.aggregate(input)) {
            return aggregate(input, aggregate);
        } else {
            return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return null;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        RelNode input = sort.getInput().accept(this);
        if (RelMdSqlViews.sort(input)) {
            return sort(input, sort);
        } else {
            return sort.copy(sort.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return null;
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        return null;
    }

    @Override
    public RelNode visit(RelNode other) {
        return other;
    }

    public static RelNode sort(RelNode input, LogicalSort sort) {
        Distribution dataNodeInfo = null;
        if (input instanceof View) {
            dataNodeInfo = ((View) input).getDistribution();
            input = ((View) input).getRelNode();
        }
        if (dataNodeInfo == null) {
            return sort.copy(input.getTraitSet(), ImmutableList.of(input));
        }
        if (dataNodeInfo.isSingle()) {
            input = sort.copy(input.getTraitSet(), ImmutableList.of(input));
            return View.of(input, dataNodeInfo);
        } else {
            RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
            Number offset = sort.offset == null ? 0 : (Number) ((RexLiteral) sort.offset).getValue();
            Number fetch = sort.fetch == null ? Long.MAX_VALUE : (Number) ((RexLiteral) sort.fetch).getValue();
            BigDecimal offsetPlusFetch = BigDecimal.valueOf(offset.longValue() + fetch.longValue());
            input = LogicalSort.create(input, sort.getCollation()
                    , rexBuilder.makeExactLiteral(BigDecimal.ZERO)
                    , rexBuilder.makeExactLiteral(offsetPlusFetch));
            input = View.of(input, dataNodeInfo);
            return MycatMergeSort.create(
                    input.getTraitSet().replace(MycatConvention.INSTANCE),
                    input,
                    sort.collation, sort.offset, sort.fetch);
        }
    }


    public static RelNode aggregate(RelNode input, LogicalAggregate aggregate) {
        RelOptCluster cluster = input.getCluster();
        Distribution dataNodeInfo = null;
        if (input instanceof View) {
            dataNodeInfo = ((View) input).getDistribution();
            input = ((View) input).getRelNode();
        }
        if (dataNodeInfo == null) {
            input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
            return input;
        }
        if (dataNodeInfo.isSingle()) {
            input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
            return View.of(input, dataNodeInfo);
        } else {
            if (!(input instanceof Union)) {
                input = LogicalUnion.create(ImmutableList.of(input, input), true);
                input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
            }
            HepProgramBuilder hepProgram = new HepProgramBuilder();
            hepProgram.addMatchLimit(1);
            hepProgram.addRuleInstance(AggregateUnionTransposeRule.INSTANCE);
            HepPlanner planner = new HepPlanner(hepProgram.build());
            planner.setRoot(input);
            RelNode bestExp = planner.findBestExp();
            View multiView = View.of(
                    bestExp.getInput(0).getInput(0),
                    dataNodeInfo);
            return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(multiView));
        }
    }

    public static RelNode union(List<RelNode> inputs, LogicalUnion union) {
        if (union.all) {
            List<RelNode> children = new ArrayList<>();
            for (RelNode input : inputs) {
                if (input instanceof LogicalUnion) {
                    LogicalUnion bottomUnion = (LogicalUnion) input;
                    if (bottomUnion.all) {
                        children.addAll(bottomUnion.getInputs());
                    } else {
                        children.add(bottomUnion);
                    }
                } else {
                    children.add(input);
                }
            }
            return union.copy(union.getTraitSet(), children);
        }
        return union.copy(union.getTraitSet(), inputs);
    }

    public static RelNode correlate(RelNode left, RelNode right, LogicalCorrelate correlate) {
        return correlate.copy(correlate.getTraitSet(), ImmutableList.of(left, right));
    }

    public static RelNode join(RelNode left, RelNode right, LogicalJoin join) {
        if (left instanceof View && right instanceof View) {
            View leftView = (View) left;
            View rightView = (View) right;

            RelNode leftNode = leftView.getRelNode();
            RelNode rightNode = rightView.getRelNode();

            Distribution ldistribution = leftView.getDistribution();
            Distribution rdistribution = rightView.getDistribution();
            if (ldistribution.isBroadCast() || rdistribution.isBroadCast()) {
                if (!ldistribution.isBroadCast()) {
                    return View.of(join.copy(join.getTraitSet(), ImmutableList.of(leftView.getRelNode(), rightView.getRelNode())), ldistribution);
                }
                if (!rdistribution.isBroadCast()) {
                    return View.of(join.copy(join.getTraitSet(), ImmutableList.of(leftView.getRelNode(), rightView.getRelNode())), rdistribution);
                }
                if (ldistribution.isBroadCast() && rdistribution.isBroadCast()) {
                    return View.of(join.copy(join.getTraitSet(), ImmutableList.of(leftView.getRelNode(), rightView.getRelNode())), ldistribution);
                }
            }
            if (ldistribution.isPhy() && rdistribution.isPhy() && ldistribution.getDataNodes().equals(rdistribution.getDataNodes())) {
                return View.of(join.copy(join.getTraitSet(), ImmutableList.of(leftView.getRelNode(), rightView.getRelNode())), ldistribution);
            }
//            left = ((View) left).expandToPhyRelNode();
//            right = ((View) right).expandToPhyRelNode();
//            List<DataNode> one = ldistribution.getDataNodes();
//            List<DataNode> two = rdistribution.getDataNodes();
//            LogicalJoin copy = (LogicalJoin) join.copy(join.getTraitSet(), ImmutableList.of(left, right));
//            HepProgramBuilder builder = new HepProgramBuilder();
//            builder.addRuleInstance(CoreRules.JOIN_LEFT_UNION_TRANSPOSE);
//            builder.addRuleInstance(CoreRules.JOIN_RIGHT_UNION_TRANSPOSE);
//            HepPlanner planner = new HepPlanner(builder.build());
//            planner.setRoot(copy);
//            RelNode bestExp = planner.findBestExp();
//            if (bestExp instanceof LogicalUnion) {
//                SQLRBORewriter sqlrboRewriter = new SQLRBORewriter();
//                List<RelNode> inputs = bestExp.getInputs().stream().flatMap(i -> {
//                    if (i instanceof LogicalUnion) {
//                        return i.getInputs().stream();
//                    } else {
//                        return Stream.of(i);
//                    }
//                }).collect(Collectors.toList());
//                List<RelNode> res = new ArrayList<>();
//                for (RelNode input : inputs) {
//                    RelNode accept = input.accept(sqlrboRewriter);
//                    res.add(accept);
//                }
//                ArrayList<DataNode> dataNodes2 = new ArrayList<>(one);
//                dataNodes2.addAll(two);
//                return LogicalUnion.create(res, true);
//            }
        }
        return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
    }

    public static RelNode filter(RelNode input, LogicalFilter filter) {
        Distribution dataNodeInfo = null;
        if (input instanceof View) {
            dataNodeInfo = ((View) input).getDistribution();
            input = ((View) input).getRelNode();

        }

        if (input instanceof LogicalTableScan) {
            RexNode condition = filter.getCondition();
            RelOptTable table = input.getTable();
            AbstractMycatTable nodes = table.unwrap(AbstractMycatTable.class);
            Distribution distribution = nodes.computeDataNode(ImmutableList.of(condition));
            return View.of(filter.copy(filter.getTraitSet(), (input), condition), distribution);
        }
        filter = (LogicalFilter) filter.copy(filter.getTraitSet(), ImmutableList.of(input));
        if (dataNodeInfo != null) {
            return View.of(filter, dataNodeInfo);
        } else {
            return filter;
        }
    }

    public static RelNode project(RelNode input, LogicalProject project) {
        Distribution dataNodeInfo = null;
        if (input instanceof View) {
            dataNodeInfo = ((View) input).getDistribution();
            input = ((View) input).getRelNode();
        }
        input = project.copy(project.getTraitSet(), ImmutableList.of(input));
        if (dataNodeInfo == null) {
            return input;
        } else {
            return View.of(input, dataNodeInfo);
        }
    }

}