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
import io.mycat.BackendTableInfo;
import io.mycat.DataNode;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.hbt4.MycatConvention;
import io.mycat.hbt4.logical.rel.MycatMergeSort;
import io.mycat.hbt4.logical.rel.MycatQuery;
import io.mycat.metadata.CustomTableHandlerWrapper;
import io.mycat.metadata.QueryBuilder;
import io.mycat.util.NameMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;
import org.jetbrains.annotations.Nullable;

import java.math.BigDecimal;
import java.util.*;

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

    protected OptimizationContext optimizationContext;
    protected List<Object> params;


    public SQLRBORewriter(OptimizationContext optimizationContext, List<Object> params) {
        this.optimizationContext = optimizationContext;
        this.params = params;
    }

    @Override
    public RelNode visit(TableScan scan) {
        AbstractMycatTable abstractMycatTable = scan.getTable().unwrap(AbstractMycatTable.class);
        if (abstractMycatTable != null) {
            if (abstractMycatTable.isCustom()) {
                MycatLogicTable mycatLogicTable = (MycatLogicTable) abstractMycatTable;
                CustomTableHandlerWrapper customTableHandler = (CustomTableHandlerWrapper) mycatLogicTable.getTable();
                QueryBuilder queryBuilder = customTableHandler.createQueryBuilder(scan.getCluster());
                queryBuilder.setRowType(scan.getRowType());
                return queryBuilder;
            }
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
        if (!userDefinedFunctionInFilter(filter)) {
            if (RelMdSqlViews.filter(input)) {
                return filter(input, filter, optimizationContext);
            }
        }
        return filter.copy(filter.getTraitSet(), ImmutableList.of(input));
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        return null;
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RelNode input = project.getInput().accept(this);
        boolean canProject = !userDefinedFunctionInProject(project);
        if (canProject) {
            if (RelMdSqlViews.project(input)) {
                return project(input, project);
            }
        }
        return project.copy(project.getTraitSet(), ImmutableList.of(input));
    }

    private static boolean userDefinedFunctionInProject(Project project) {
        CheckingUserDefinedAndConvertFunctionVisitor visitor = new CheckingUserDefinedAndConvertFunctionVisitor();
        for (RexNode node : project.getProjects()) {
            node.accept(visitor);
            if (visitor.containsUserDefinedFunction()) {
                return true;
            }
        }
        return false;
    }

    private static boolean userDefinedFunctionInFilter(Filter filter) {
        CheckingUserDefinedAndConvertFunctionVisitor visitor = new CheckingUserDefinedAndConvertFunctionVisitor();
        RexNode condition = filter.getCondition();
        condition.accept(visitor);
        return filter.containsOver() || visitor.containsUserDefinedFunction();
    }

    /**
     * Visitor that checks whether part of a projection is a user-defined
     * function (UDF).
     */
    private static class CheckingUserDefinedAndConvertFunctionVisitor
            extends RexVisitorImpl<Void> {

        private boolean containsUsedDefinedFunction = false;

        CheckingUserDefinedAndConvertFunctionVisitor() {
            super(true);
        }

        public boolean containsUserDefinedFunction() {
            return containsUsedDefinedFunction;
        }

        @Override
        public Void visitCall(RexCall call) {
            SqlOperator operator = call.getOperator();
            String name = operator.getName();
            if (operator instanceof SqlFunction) {
                containsUsedDefinedFunction |= Information_Functions.containsKey(name, false);
            }
            return super.visitCall(call);
        }
    }

    public static NameMap<Object> Information_Functions = new NameMap<>();

    static {
        Information_Functions.put("BENCHMARK", null);
        Information_Functions.put("BINLOG_GTID_POS", null);
        Information_Functions.put("CHARSET", null);
        Information_Functions.put("COERCIBILITY", null);
        Information_Functions.put("COLLATION", null);
        Information_Functions.put("CONNECTION_ID", null);
        Information_Functions.put("CURRENT_ROLE", null);
        Information_Functions.put("CURRENT_USER", null);
        Information_Functions.put("DATABASE", null);
        Information_Functions.put("DEFAULT", null);
        Information_Functions.put("FOUND_ROWS", null);
        Information_Functions.put("LAST_INSERT_ID", null);
        Information_Functions.put("LAST_VALUE", null);
        Information_Functions.put("PROCEDURE_ANALYSE", null);
        Information_Functions.put("ROW_COUNT", null);
        Information_Functions.put("SCHEMA", null);
        Information_Functions.put("SESSION_USER", null);
        Information_Functions.put("SYSTEM_USER", null);
        Information_Functions.put("USER", null);
        Information_Functions.put("VERSION", null);
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        List<RelNode> inputs = join.getInputs();
        RelNode left = inputs.get(0).accept(this);
        RelNode right = inputs.get(1).accept(this);
        if (RelMdSqlViews.join(left) && (RelMdSqlViews.join(right))) {
            return join(params,left, right, join);
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
        for (RelNode input : intersect.getInputs()) {
            RelNode accept = input.accept(this);
        }

        return intersect;
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
        if (input instanceof QueryBuilder) {
            RexNode fetchRex = sort.fetch;
            RexNode offsetRex = sort.offset;
            Long fetchNumber = Long.MAX_VALUE;
            Long offsetNumber = 0L;
            RelCollation collation = sort.getCollation();
            QueryBuilder mycatCustomTable = (QueryBuilder) input;

            if (fetchRex instanceof RexLiteral) {
                Object fetchValue = ((RexLiteral) fetchRex).getValue();
                if (fetchValue != null && fetchValue instanceof Number) {
                    fetchNumber = ((Number) fetchValue).longValue();
                }
            }

            if (offsetRex instanceof RexLiteral) {
                Object offsetValue = ((RexLiteral) offsetRex).getValue();
                if (offsetValue != null && offsetValue instanceof Number) {
                    offsetNumber = ((Number) offsetValue).longValue();
                }
            }
            Optional<QueryBuilder> queryBuilder = mycatCustomTable
                    .sort(offsetNumber, fetchNumber, collation);
            if (queryBuilder.isPresent()) {
                return queryBuilder.get();
            }
            return
                    sort.copy(sort.getTraitSet()
                            .replace(MycatConvention.INSTANCE), mycatCustomTable, collation);
        }
        if (dataNodeInfo.isSingle()) {
            input = sort.copy(input.getTraitSet(), ImmutableList.of(input));
            return View.of(input, dataNodeInfo);
        } else {
            if (sort.offset == null && sort.fetch == null) {
                input = LogicalSort.create(input, sort.getCollation()
                        , null
                        , null);
                input = View.of(input, dataNodeInfo);
                return MycatMergeSort.create(
                        input.getTraitSet().replace(MycatConvention.INSTANCE),
                        input,
                        sort.collation, sort.offset, sort.fetch);
            }
            RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
            RexNode rexNode;
            if (sort.offset == null && sort.fetch != null) {
                rexNode = sort.fetch;
            } else if (sort.offset != null && sort.fetch == null) {
                rexNode = sort.offset;
            } else {
                if (sort.offset instanceof RexLiteral && sort.fetch instanceof RexLiteral) {
                    BigDecimal decimal = ((RexLiteral) sort.offset).getValueAs(BigDecimal.class).add(
                            ((RexLiteral) sort.fetch).getValueAs(BigDecimal.class));
                    rexNode = rexBuilder.makeExactLiteral(decimal);
                } else {
                    rexNode = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, sort.offset, sort.fetch);
                }
            }
            input = LogicalSort.create(input, sort.getCollation()
                    , rexBuilder.makeExactLiteral(BigDecimal.ZERO)
                    , rexNode);
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
            RelNode backup = input;
            if (!(input instanceof Union)) {
                input = LogicalUnion.create(ImmutableList.of(input, input), true);
                input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
            }
            HepProgramBuilder hepProgram = new HepProgramBuilder();
            hepProgram.addMatchLimit(1);
            hepProgram.addRuleInstance(CoreRules.AGGREGATE_UNION_TRANSPOSE);
            HepPlanner planner = new HepPlanner(hepProgram.build());
            planner.setRoot(input);
            RelNode bestExp = planner.findBestExp();
            if (bestExp instanceof Aggregate && ((Aggregate) bestExp).getInput() instanceof Union) {
                View multiView = View.of(
                        bestExp.getInput(0).getInput(0),
                        dataNodeInfo);
                return bestExp.copy(aggregate.getTraitSet(), ImmutableList.of(multiView));
            } else {
                return View.of(
                        backup,
                        dataNodeInfo);
            }

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
    public static RelNode join(List<Object> params,
                               RelNode left,
                               RelNode right,
                               LogicalJoin join) {
        if (left instanceof View && right instanceof View) {
            View leftView = (View) left;
            View rightView = (View) right;

            RelNode res = pushDownJoinByNormalTableOrGlobalTable(join, leftView, rightView);
            if (res != null) return res;

            if (params != null) {
                return pushDownJoinByDataNode(join,
                        leftView,
                        rightView,
                        leftView.getDistribution().getDataNodes(params),
                        rightView.getDistribution().getDataNodes(params));
            }
            return pushDownJoinByDataNode(join,
                    leftView,
                    rightView,
                    leftView.getDistribution().getDataNodes(),
                    rightView.getDistribution().getDataNodes());
        }
        return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
    }

    public static RelNode join(RelNode left,
                               RelNode right,
                               LogicalJoin join) {
        if (left instanceof View && right instanceof View) {
            View leftView = (View) left;
            View rightView = (View) right;

            RelNode res = pushDownJoinByNormalTableOrGlobalTable(join, leftView, rightView);

            if (res != null) return res;

            return pushDownJoinByDataNode(join,
                    leftView,
                    rightView,
                    leftView.getDistribution().getDataNodes(),
                    rightView.getDistribution().getDataNodes());
        }
        return join.copy(join.getTraitSet(), ImmutableList.of(left, right));
    }

    @Nullable
    private static RelNode pushDownJoinByNormalTableOrGlobalTable(LogicalJoin join, View leftView, View rightView) {
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
        return null;
    }


    private static RelNode pushDownJoinByDataNode(LogicalJoin join,
                                                  View leftView,
                                                  View rightView,
                                                  Iterable<DataNode> leftDataNodes,
                                                  Iterable<DataNode> rightDataNodes) {
        Map<String, List<RelNode>> phyViews = new HashMap<>();
        ArrayList<RelNode> list = new ArrayList<>();
        for (DataNode leftDataNode : leftDataNodes) {
            for (DataNode rightDataNode : rightDataNodes) {
                RelNode leftN = leftView.applyDataNode(leftDataNode);
                RelNode rightN = rightView.applyDataNode(rightDataNode);
                if (leftDataNode.getTargetName().equals(rightDataNode.getTargetName())) {
                    List<RelNode> relNodes = phyViews.computeIfAbsent(leftDataNode.getTargetName(), (k) -> new ArrayList<>());
                    relNodes.add(join.copy(join.getTraitSet(),
                            ImmutableList.of(
                                    leftN,
                                    rightN)));
                } else {
                    list.add(join.copy(join.getTraitSet(), ImmutableList.of(
                            View.of(leftN,
                                    Distribution.of(leftDataNode)),
                            View.of(rightN,
                                    Distribution.of(rightDataNode)))));
                }
            }

        }


        int unionLimit = 4;
        for (Map.Entry<String, List<RelNode>> e : phyViews.entrySet()) {
            String key = e.getKey();
            List<RelNode> value = new ArrayList<>(e.getValue());
            while (true) {
                List<RelNode> relNodes = value.subList(0, Math.min(value.size(), unionLimit));
                list.add(View.of(LogicalUnion.create(relNodes, true),
                        Distribution.of(new BackendTableInfo(key, "", ""))));
                if (unionLimit < value.size()) {
                    value = value.subList(relNodes.size(), value.size());
                } else {
                    break;
                }
            }
        }
        if (list.size() == 1) {
            return list.get(0);
        }
        return LogicalUnion.create(list, true);
    }

    public static RelNode filter(RelNode input, LogicalFilter filter, OptimizationContext optimizationContext) {
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
            if (optimizationContext != null && distribution.isPartial()) {
                optimizationContext.setPredicateOnView(true);
            }
            return View.of(filter.copy(filter.getTraitSet(), (input), condition), distribution);
        }
        if (input instanceof QueryBuilder) {
            QueryBuilder queryBuilder = (QueryBuilder) input;
            Optional<QueryBuilder> newFilter = queryBuilder.filter(filter.getCondition());
            if (newFilter.isPresent()) {
                return newFilter.get();
            } else {
                return filter.copy(filter.getTraitSet(),
                        ImmutableList.of(queryBuilder));
            }
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
        if (input instanceof QueryBuilder) {
            QueryBuilder queryBuilder = (QueryBuilder) input;
            Optional<QueryBuilder> builder = queryBuilder
                    .project(
                            ImmutableIntList.identity(
                                    project.getRowType().getFieldCount())
                                    .toIntArray());
            if (builder.isPresent()) {
                return builder.get();
            }
            return project.copy(project.getTraitSet(), ImmutableList.of(queryBuilder));
        }
        input = project.copy(project.getTraitSet(), ImmutableList.of(input));
        if (dataNodeInfo == null) {
            return input;
        } else {
            return View.of(input, dataNodeInfo);
        }
    }

}