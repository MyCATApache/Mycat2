/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.localrel.LocalRules;
import io.mycat.calcite.localrel.LocalSort;
import io.mycat.calcite.localrel.MycatAggregateUnionTransposeRule;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatHashAggregate;
import io.mycat.calcite.physical.MycatProject;
import io.mycat.calcite.physical.MycatSortMergeJoin;
import io.mycat.calcite.rules.MycatMergeJoinRule;
import io.mycat.calcite.table.*;
import io.mycat.config.ServerConfig;
import io.mycat.querycondition.QueryType;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.NameMap;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptListener;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.metadata.RelColumnOrigin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.*;
import java.util.stream.Collectors;


public class SQLRBORewriter extends RelShuttleImpl {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLRBORewriter.class);

    public static RelBuilder relbuilder(RelOptCluster cluster, RelOptSchema schema) {
        return LocalRules.LOCAL_BUILDER.create(cluster, schema);
    }

    public final static io.mycat.calcite.rewriter.RelMdSqlViews RelMdSqlViews = new RelMdSqlViews() {

        @Override
        public boolean filter(RelNode relNode) {
            return true;
        }


        @Override
        public boolean join(RelNode relNode) {
            return true;
        }


        @Override
        public boolean project(RelNode relNode) {
            return true;
        }


        @Override
        public boolean aggregate(RelNode relNode) {
            return true;
        }

        @Override
        public boolean correlate(RelNode left) {
            return true;
        }

        @Override
        public boolean sort(RelNode input) {
            return true;
        }
    };

    public SQLRBORewriter() {

    }

    public static Optional<RelNode> on(RelNode bottom, RelNode up) {
        if (bottom instanceof MycatView) {
            MycatView view = (MycatView) bottom;
            if (view.isMergeAgg() || view.isMergeSort()) {
                return Optional.empty();
            }
        }
        if (up instanceof Calc) {
            return view(bottom, (Calc) up);
        }
        if (up instanceof Filter) {
            return view(bottom, (Filter) up);
        }
        if (up instanceof Project) {
            return view(bottom, (Project) up);
        }
        if (up instanceof Aggregate) {
            return view(bottom, (Aggregate) up);
        }
        if (up instanceof Sort) {
            return view(bottom, (Sort) up);
        }
        return Optional.empty();
    }

    public static Optional<RelNode> on(RelNode left, RelNode right, RelNode up) {
        if (up instanceof Join) {
            return view(left, right, (Join) up);
        }
        return Optional.empty();
    }

    @Override
    public RelNode visit(TableScan scan) {
        return view(scan).orElse(scan);
    }

    @NotNull
    public static final Optional<RelNode> view(TableScan scan) {
        AbstractMycatTable abstractMycatTable = scan.getTable().unwrap(AbstractMycatTable.class);
        MycatLogicTable mycatLogicTable = (MycatLogicTable) abstractMycatTable;
        if (abstractMycatTable != null) {
            if (abstractMycatTable.isVisual()) {
                TableHandler table = ((MycatLogicTable) abstractMycatTable).getTable();
                MycatTableScan mycatTableScan = MycatTableScan.ofTable(scan);
                return Optional.of(mycatTableScan);
            }
            if (abstractMycatTable.isCustom()) {
                CustomTableHandlerWrapper customTableHandler = (CustomTableHandlerWrapper) mycatLogicTable.getTable();
                QueryBuilder queryBuilder = customTableHandler.createQueryBuilder(scan.getCluster());
                queryBuilder.setRowType(scan.getRowType());
                return Optional.of(queryBuilder);
            }
            return Optional.of(MycatView.ofBottom(scan, abstractMycatTable.createDistribution()));
        }
        return Optional.empty();
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
        return view(input, filter)
                .orElse(filter.copy(filter.getTraitSet(), ImmutableList.of(filter)));
    }

    @NotNull
    public static final Optional<RelNode> view(RelNode input, Filter filter) {
        if (!userDefinedFunctionInFilter(filter)) {
            if (RelMdSqlViews.filter(input)) {
                return Optional.of(filter(input, filter));
            }
        }
        return Optional.empty();
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        RelNode input = calc.getInput().accept(this);
        return view(input, calc).orElse(calc.copy(calc.getTraitSet(), ImmutableList.of(input)));
    }

    public static final Optional<RelNode> view(RelNode input, Calc calc) {
        final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter =
                calc.getProgram().split();
        RelBuilder relBuilder = relbuilder(calc.getCluster(), null);
        RelNode relNode = relBuilder.push(input).filter(projectFilter.right).build();
        if (relNode instanceof Filter) {
            return view(input, (Filter) relNode)
                    .flatMap(u -> {
                        RelNode node = relBuilder.push(u).project(projectFilter.left).build();
                        if (node instanceof Project) {
                            return view(u, (Project) node);
                        } else {
                            return Optional.empty();
                        }
                    });
        } else {
            return Optional.empty();
        }
    }

    @Override
    public RelNode visit(LogicalProject project) {
        RelNode input = project.getInput().accept(this);
        return view(input, project).orElse(project.copy(project.getTraitSet(), ImmutableList.of(input)));
    }

    public static final Optional<RelNode> view(RelNode input, Project project) {
        boolean canProject = !userDefinedFunctionInProject(project);
        if (canProject) {
            if (RelMdSqlViews.project(input)) {
                return Optional.of(project(input, project));
            }
        }
        return Optional.empty();
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

    public static boolean userDefinedFunctionInFilter(Filter filter) {
        CheckingUserDefinedAndConvertFunctionVisitor visitor = new CheckingUserDefinedAndConvertFunctionVisitor();
        RexNode condition = filter.getCondition();

        condition.accept(visitor);
        return filter.containsOver() || visitor.containsUserDefinedFunction();
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

        //window function
        Information_Functions.put("ROW_NUMBER", null);
        Information_Functions.put("RANK", null);
        Information_Functions.put("DENSE_RANK", null);
        Information_Functions.put("PERCENT_RANK", null);
        Information_Functions.put("CUME_DIST", null);
        Information_Functions.put("FIRST_VALUE", null);
        Information_Functions.put("LAST_VALUE", null);
        Information_Functions.put("LAG", null);
        Information_Functions.put("LEAD", null);
        Information_Functions.put("NTH_VALUE", null);
    }

    @Override
    public RelNode visit(LogicalJoin join) {
        List<RelNode> inputs = join.getInputs();
        RelNode left = inputs.get(0).accept(this);
        RelNode right = inputs.get(1).accept(this);
        return view(left, right, join).orElse(join.copy(join.getTraitSet(), ImmutableList.of(left, right)));
    }

    public static final Optional<RelNode> view(RelNode left, RelNode right, Join join) {
        boolean lr = RelMdSqlViews.join(left);
        boolean rr = RelMdSqlViews.join(right);
        if (lr && rr) {
            return rboJoinRewrite(left, right, join);
        }
        return Optional.empty();
    }

    @NotNull
    public static Optional<RelNode> tryMergeJoin(RelNode left, RelNode right, Join join) {
        if (!DrdsSqlCompiler.RBO_MERGE_JOIN) {
            return Optional.empty();
        }
        boolean isleftView = left instanceof MycatView;
        boolean isRightView = right instanceof MycatView;
        if (isleftView && isRightView) {
            MycatSortMergeJoin mycatSortMergeJoin = MycatMergeJoinRule.INSTANCE.tryMycatSortMergeJoin(join.copy(join.getTraitSet(), ImmutableList.of(left, right)), true);
            return Optional.ofNullable(mycatSortMergeJoin);
        } else {
            return Optional.empty();
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
        return view(inputs, union).orElse(union.copy(union.getTraitSet(), inputs));
    }

    @Override
    public RelNode visit(LogicalIntersect intersect) {
        ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
        for (RelNode input : intersect.getInputs()) {
            builder.add(input.accept(this));
        }
        return intersect.copy(intersect.getTraitSet(), builder.build());
    }

    @Override
    public RelNode visit(LogicalMinus minus) {
        ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
        for (RelNode input : minus.getInputs()) {
            builder.add(input.accept(this));
        }
        return minus.copy(minus.getTraitSet(), builder.build());
    }

    @Override
    public RelNode visit(LogicalAggregate aggregate) {
        RelNode input = aggregate.getInput().accept(this);
        return view(input, aggregate).orElse(aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input)));
    }

    public static Optional<RelNode> view(RelNode input, Aggregate aggregate) {
        if (RelMdSqlViews.aggregate(input)) {
            return (aggregate(input, aggregate));
        }
        return Optional.empty();
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return match;
    }

    @Override
    public RelNode visit(LogicalSort sort) {
        RelNode input = sort.getInput().accept(this);
        return view(input, sort).orElse(sort.copy(sort.getTraitSet(), ImmutableList.of(input)));
    }

    public static Optional<RelNode> view(RelNode input, Sort sort) {
        if (RelMdSqlViews.sort(input)) {
            return (sort(input, sort));
        }
        return Optional.empty();
    }

    @Override
    public RelNode visit(LogicalExchange exchange) {
        return exchange.copy(exchange.getTraitSet(), ImmutableList.of(exchange.getInput().accept(this)));
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        RelNode relNode = modify.getInput().accept(this);
        return modify.copy(modify.getTraitSet(), ImmutableList.of(relNode));
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof LogicalCalc) {
            return visit((LogicalCalc) other);
        }
        if (other instanceof LogicalRepeatUnion) {
            return visit((LogicalRepeatUnion) other);
        }
        if (other instanceof LogicalTableSpool) {
            return visit((LogicalTableSpool) other);
        }
        if (other instanceof LogicalWindow) {
            return visit((LogicalWindow) other);
        }
        return other;
    }

    public RelNode visit(LogicalRepeatUnion logicalRepeatUnion) {
        ImmutableList.Builder<RelNode> builder = ImmutableList.builder();
        for (RelNode input : logicalRepeatUnion.getInputs()) {
            builder.add(input.accept(this));
        }
        return logicalRepeatUnion.copy(logicalRepeatUnion.getTraitSet(), builder.build());
    }

    public RelNode visit(LogicalTableSpool logicalTableSpool) {
        return logicalTableSpool.copy(logicalTableSpool.getTraitSet(), ImmutableList.of(logicalTableSpool.getInput().accept(this)));
    }

    public RelNode visit(LogicalWindow logicalWindow) {
        return logicalWindow.copy(logicalWindow.getTraitSet(), ImmutableList.of(logicalWindow.getInput().accept(this)));
    }

    public static Optional<RelNode> sort(RelNode original, Sort sort) {
        RelNode input = original;
        MycatView view = null;
        Distribution dataNodeInfo = null;
        if (input instanceof MycatView) {
            dataNodeInfo = ((MycatView) input).getDistribution();
            view = (MycatView) original;
            input = ((MycatView) input).getRelNode();
        }
        if (dataNodeInfo == null) {
            return Optional.empty();
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
                return Optional.of(queryBuilder.get());
            }
            return
                    Optional.of(sort.copy(sort.getTraitSet()
                            .replace(MycatConvention.INSTANCE), mycatCustomTable, collation));
        }
        if (dataNodeInfo.type() == Distribution.Type.PHY || dataNodeInfo.type() == Distribution.Type.BROADCAST) {
            input = sort.copy(input.getTraitSet(), ImmutableList.of(input));
            return Optional.of(view.changeTo(input, dataNodeInfo));
        } else {
            if (sort.offset == null && sort.fetch == null) {
                input = LogicalSort.create(input, sort.getCollation()
                        , null
                        , null);
                input = view.changeTo(LocalSort.create((LogicalSort) input, ((LogicalSort) input).getInput()), dataNodeInfo);
                return Optional.of(input);
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
            input = view.changeTo(LocalSort.create((LogicalSort) input, ((LogicalSort) input).getInput()), dataNodeInfo);
            return Optional.of(input);
        }
    }


    public static Optional<RelNode> aggregate(RelNode original, Aggregate aggregate) {
        RelNode input = original;
        Distribution dataNodeInfo = null;
        MycatView view = null;
        ImmutableList<RelHint> hints = aggregate.getHints();
        if (input instanceof MycatView) {
            dataNodeInfo = ((MycatView) input).getDistribution();
            view = (MycatView) original;
            input = ((MycatView) input).getRelNode();
        }
        if (dataNodeInfo == null) {
            return Optional.empty();
        }
        if (dataNodeInfo.type() == Distribution.Type.PHY || dataNodeInfo.type() == Distribution.Type.BROADCAST) {
            input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
            return Optional.of(view.changeTo(input, dataNodeInfo));
        } else {
            ImmutableBitSet groupSet = aggregate.getGroupSet();
            RelMetadataQuery metadataQuery = aggregate.getCluster().getMetadataQuery();
            IdentityHashMap<MycatLogicTable, Set<String>> shardingKeysMap = new IdentityHashMap<>();
            for (Integer integer : groupSet) {
                RelColumnOrigin columnOrigin = metadataQuery.getColumnOrigin(input, integer);
                if (columnOrigin == null || columnOrigin.isDerived()) {
                    continue;
                }
                MycatLogicTable mycatLogicTable = columnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
                if (mycatLogicTable == null || !mycatLogicTable.isSharding()) {
                    continue;
                }
                ShardingTableHandler tableHandler = (ShardingTableHandler) mycatLogicTable.getTable();
                SimpleColumnInfo simpleColumnInfo = tableHandler.getColumns().get(columnOrigin.getOriginColumnOrdinal());
                if (simpleColumnInfo.isShardingKey()) {
                    Set<String> shardingKeySet = shardingKeysMap.computeIfAbsent(mycatLogicTable, s -> new HashSet<>());
                    shardingKeySet.add(simpleColumnInfo.getColumnName());
                    if (tableHandler.function().requireShardingKeys(shardingKeySet)) {
                        input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
                        return Optional.of(view.changeTo(input, dataNodeInfo));
                    }
                }
            }

            RelNode backup = input;
            if (!(input instanceof Union)) {
                input = LogicalUnion.create(ImmutableList.of(input, input), true);
                input = LogicalAggregate.create(input, aggregate.getHints(), aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
            }
            HepProgramBuilder hepProgram = new HepProgramBuilder();
            hepProgram.addMatchLimit(512);
            hepProgram.addRuleInstance(MycatAggregateUnionTransposeRule.Config.DEFAULT.toRule());
            HepPlanner planner = new HepPlanner(hepProgram.build());
            planner.setRoot(input);
            RelNode bestExp = planner.findBestExp();

            if (bestExp instanceof Aggregate) {
                Aggregate mergeAgg = (Aggregate) bestExp;
                if (mergeAgg.getInput() instanceof Union && mergeAgg.getInput(0).getInput(0) instanceof Aggregate) {
                    MycatView multiView = view.changeTo(
                            mergeAgg.getInput(0).getInput(0),
                            dataNodeInfo);
                    MycatHashAggregate mycatHashAggregate =
                            MycatHashAggregate
                                    .create(mergeAgg.getTraitSet(),
                                            hints,
                                            multiView,
                                            mergeAgg.getGroupSet(),
                                            mergeAgg.getGroupSets(),
                                            mergeAgg.getAggCallList());
                    return Optional.of(mycatHashAggregate);
                }
            }
            {
                return splitAggregate(view, aggregate);
            }
        }
    }

    private static Optional<RelNode> splitAggregate(MycatView viewNode, Aggregate aggregate) {

        AggregatePushContext aggregateContext = AggregatePushContext.split(aggregate);

        MycatView newView = viewNode.changeTo(
                LogicalAggregate.create(viewNode.getRelNode(),
                        aggregate.getHints(),
                        aggregate.getGroupSet(),
                        aggregate.getGroupSets(),
                        aggregateContext.getPartialAggregateCallList()));

        Aggregate globalAggregateRelNode = aggregate.copy(aggregate.getTraitSet(), newView,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregateContext.getGlobalAggregateCallList());

        MycatProject projectRelNode = MycatProject.create(globalAggregateRelNode,
                aggregateContext.getProjectExprList(),
                aggregate.getRowType());

        return RexUtil.isIdentity(projectRelNode.getProjects(), projectRelNode.getInput().getRowType()) ?
                Optional.of(globalAggregateRelNode) : Optional.of(projectRelNode);
    }


    public static Optional<RelNode> view(List<RelNode> inputs, Union union) {
        List<RelNode> backup = new ArrayList<>(inputs);
        List<RelNode> children = new ArrayList<>();
        for (RelNode input : inputs) {
            if (input instanceof LogicalUnion) {
                Union bottomUnion = (Union) input;
                children.addAll(bottomUnion.getInputs());
            } else {
                children.add(input);
            }
        }
        inputs = children;

        List<MycatView> inputViews = new LinkedList<>();
        List<RelNode> newViews = new ArrayList<>();

        List<RelNode> others = new ArrayList<>();

        for (RelNode input : inputs) {
            if (input instanceof MycatView) {
                MycatView curView = (MycatView) input;
                if (curView.getDistribution().type() == Distribution.Type.SHARDING) {
                    if (!unionAllInPartitonKey(curView)) {
                        return Optional.empty();
                    }
                }
                inputViews.add((MycatView) input);
            } else {
                others.add(input);
            }
        }
        if (inputViews.isEmpty()) {
            return Optional.empty();
        }
        if (inputViews.size() > 1) {
            MycatView left = inputViews.get(0);
            List<MycatView> matchViews = new ArrayList<>();
            matchViews.add(left);
            List<MycatView> failViews = new ArrayList<>();
            Distribution distribution = null;
            for (MycatView right : inputViews.subList(1, inputViews.size())) {
                Optional<Distribution> distributionOptional = left.getDistribution().join(right.getDistribution());
                if (distributionOptional.isPresent()) {
                    distribution = distributionOptional.get();
                    Distribution.Type type = distribution.type();
                    if (type == Distribution.Type.PHY || type == Distribution.Type.BROADCAST) {
                        matchViews.add(right);
                        continue;
                    } else if (type == Distribution.Type.SHARDING &&
                            unionAllInPartitonKey(left) &&
                            unionAllInPartitonKey(right)) {
                        matchViews.add(right);
                        continue;
                    }
                }
                failViews.add(right);
            }
            if (distribution != null) {
                newViews.add(left.changeTo(union.copy(union.getTraitSet(),
                                (List) matchViews.stream().map(i -> i.getRelNode()).collect(Collectors.toList())),
                        distribution)
                );
            } else {
                newViews.addAll(matchViews);
            }
            newViews.addAll(failViews);
        } else {
            newViews.addAll(inputViews);
        }

        inputs = (List) ImmutableList.builder().addAll(newViews).addAll(others).build();
        if (inputs.size() == 1) {
            return Optional.of(inputs.get(0));
        }
        if (backup.equals(inputs)) {
            return Optional.empty();
        }
        return Optional.of(union.copy(union.getTraitSet(), inputs));
    }

    private static boolean unionAllInPartitonKey(MycatView mycatView) {
        boolean isInPartitionKey;
        switch (mycatView.getDistribution().type()) {
            case BROADCAST:
            case PHY:
                isInPartitionKey = true;
                break;
            case SHARDING:
                ShardingTable shardingTable = mycatView.getDistribution().getShardingTables().get(0);
                isInPartitionKey = isInPartitionKey(mycatView, shardingTable.getShardingFuntion());
                break;
            default:
                isInPartitionKey = false;
                break;
        }
        return isInPartitionKey;
    }

    public static RelNode correlate(RelNode left, RelNode right, LogicalCorrelate correlate) {
        return correlate.copy(correlate.getTraitSet(), ImmutableList.of(left, right));
    }


//    public static RelNode join(RelNode left,
//                        RelNode right,
//                        LogicalJoin join) {
//        Optional<RelNode> relNodeOptional = bottomJoin(left, right, join);
//        if (relNodeOptional.isPresent()) return relNodeOptional.get();
//        Join newJoin = join.copy(join.getTraitSet(), ImmutableList.of(left, right));
////        if (this.joinClustering) {
////            int orgJoinCount = RelOptUtil.countJoins(newJoin);
////            if (!(newJoin instanceof MycatRel) && newJoin.getJoinType() == JoinRelType.INNER && orgJoinCount > 1 && orgJoinCount < 12) {
////                RelOptCluster cluster = newJoin.getCluster();
////                RelOptPlanner planner = cluster.getPlanner();
////                planner.clear();
////
////                planner.setRoot(planner.changeTraits(newJoin, cluster.traitSetOf(MycatConvention.INSTANCE)));
////
////                RelNode bestExp = planner.findBestExp();
////                if (RelOptUtil.countJoins(bestExp) < orgJoinCount) {
////                    return bestExp;
////                }
////            }
////        }
//        return newJoin;
//    }

    public static Optional<RelNode> rboJoinRewrite(RelNode left, RelNode right, Join join) {
        Optional<RelNode> relNodeOptional = bottomJoin(left, right, join);
        if (relNodeOptional.isPresent()) {
            return relNodeOptional;
        }
        return tryMergeJoin(left, right, join);
    }

    public static Optional<RelNode> bottomJoin(RelNode left, RelNode right, Join join) {
        if (left instanceof MycatView && right instanceof MycatView) {
            Optional<RelNode> relNodeOptional = pushDownJoinByNormalTableOrGlobalTable(join,
                    (MycatView) left,
                    (MycatView) right);
            if (relNodeOptional.isPresent()) {
                return relNodeOptional;
            }
            relNodeOptional = pushDownERTable(join, (MycatView) left, (MycatView) right);
            if (relNodeOptional.isPresent()) {
                return relNodeOptional;
            }
        }
        return Optional.empty();
    }

    private static class RuleAttemptsListener implements RelOptListener {
        private long beforeTimestamp;
        private Map<String, Pair<Long, Long>> ruleAttempts;

        RuleAttemptsListener() {
            ruleAttempts = new HashMap<>();
        }

        @Override
        public void relEquivalenceFound(RelEquivalenceEvent event) {
        }

        @Override
        public void ruleAttempted(RuleAttemptedEvent event) {
            if (event.isBefore()) {
                this.beforeTimestamp = System.nanoTime();
            } else {
                long elapsed = (System.nanoTime() - this.beforeTimestamp) / 1000;
                String rule = event.getRuleCall().getRule().toString();
                if (ruleAttempts.containsKey(rule)) {
                    Pair<Long, Long> p = ruleAttempts.get(rule);
                    ruleAttempts.put(rule, Pair.of(p.left + 1, p.right + elapsed));
                } else {
                    ruleAttempts.put(rule, Pair.of(1L, elapsed));
                }
            }
        }

        @Override
        public void ruleProductionSucceeded(RuleProductionEvent event) {
        }

        @Override
        public void relDiscarded(RelDiscardedEvent event) {
        }

        @Override
        public void relChosen(RelChosenEvent event) {
        }

        public String dump() {
            // Sort rules by number of attempts descending, then by rule elapsed time descending,
            // then by rule name ascending.
            List<Map.Entry<String, Pair<Long, Long>>> list =
                    new ArrayList<>(this.ruleAttempts.entrySet());
            Collections.sort(list,
                    (left, right) -> {
                        int res = right.getValue().left.compareTo(left.getValue().left);
                        if (res == 0) {
                            res = right.getValue().right.compareTo(left.getValue().right);
                        }
                        if (res == 0) {
                            res = left.getKey().compareTo(right.getKey());
                        }
                        return res;
                    });

            // Print out rule attempts and time
            StringBuilder sb = new StringBuilder();
            sb.append(String
                    .format(Locale.ROOT, "%n%-60s%20s%20s%n", "Rules", "Attempts", "Time (us)"));
            NumberFormat usFormat = NumberFormat.getNumberInstance(Locale.US);
            long totalAttempts = 0;
            long totalTime = 0;
            for (Map.Entry<String, Pair<Long, Long>> entry : list) {
                sb.append(
                        String.format(Locale.ROOT, "%-60s%20s%20s%n",
                                entry.getKey(),
                                usFormat.format(entry.getValue().left),
                                usFormat.format(entry.getValue().right)));
                totalAttempts += entry.getValue().left;
                totalTime += entry.getValue().right;
            }
            sb.append(
                    String.format(Locale.ROOT, "%-60s%20s%20s%n",
                            "* Total",
                            usFormat.format(totalAttempts),
                            usFormat.format(totalTime)));

            return sb.toString();
        }

    }


    private static Optional<RelNode> pushDownERTable(Join join,
                                                     MycatView left,
                                                     MycatView right) {
        switch (join.getJoinType()) {
            case INNER:
            case LEFT:
            case SEMI:
            case ANTI:
            case RIGHT:
                break;
            case FULL:
                return Optional.empty();
            default:
                throw new IllegalStateException("Unexpected value: " + join.getJoinType());
        }
        JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.isEqui()) {
            List<IntPair> pairs = joinInfo.pairs();
            if (pairs.isEmpty()) return Optional.empty();

            RexNode conditions = left.getCondition().orElse(null);
            RelMetadataQuery metadataQuery = join.getCluster().getMetadataQuery();


            IdentityHashMap<TableHandler, Set<String>> keysMap = new IdentityHashMap<>();
            IdentityHashMap<RelColumnOrigin, RelColumnOrigin> equals = new IdentityHashMap<>();

            for (IntPair pair : pairs) {

                RelColumnOrigin leftColumnOrigin = metadataQuery.getColumnOrigin(left.getRelNode(), pair.source);
                RelColumnOrigin rightColumnOrigin = metadataQuery.getColumnOrigin(right.getRelNode(), pair.target);

                if (leftColumnOrigin == null || rightColumnOrigin == null) {
                    continue;
                }
                MycatLogicTable leftLogicTable = leftColumnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
                MycatLogicTable rightLogicTable = rightColumnOrigin.getOriginTable().unwrap(MycatLogicTable.class);

                if (!leftColumnOrigin.isDerived()
                        && !rightColumnOrigin.isDerived()) {

                    TableHandler leftTableHandler = leftLogicTable.getTable();

                    keysMap.computeIfAbsent(leftTableHandler, mycatLogicTable -> new HashSet<>())
                            .add(leftTableHandler.getColumns().get(leftColumnOrigin.getOriginColumnOrdinal()).getColumnName());

                    TableHandler rightTableHandler = rightLogicTable.getTable();

                    keysMap.computeIfAbsent(rightTableHandler, mycatLogicTable -> new HashSet<>())
                            .add(rightTableHandler.getColumns().get(rightColumnOrigin.getOriginColumnOrdinal()).getColumnName());

                    equals.put(leftColumnOrigin, rightColumnOrigin);
                }
            }
            boolean allHitPartition = keysMap.entrySet().stream().allMatch(shardingTableHandlerSetEntry -> {
                TableHandler tableHandler = shardingTableHandlerSetEntry.getKey();
                switch (tableHandler.getType()) {
                    case SHARDING:
                        CustomRuleFunction function = ((ShardingTableHandler) tableHandler).function();
                        Set<String> columns = shardingTableHandlerSetEntry.getValue();
                        return function.requireShardingKeys(shardingTableHandlerSetEntry.getValue()) ||
                                columns.stream().anyMatch(function::isShardingPartitionKey);//保证命中分区
                    case GLOBAL:
                    case NORMAL:
                        return true;
                    case CUSTOM:
                    case VISUAL:
                    case VIEW:
                    default:
                        return false;
                }
            });
            if (allHitPartition) {

                //erjoin
                boolean pushDown = isErJoin(equals);
                if (!pushDown) {
                    pushDown = isSameTargetPartitionJoin(left, equals);
                }

                if (!pushDown) {
                    pushDown = isSameTargetPartitionGlobalOrNormalJoin(left, right, equals);
                }
                if (pushDown) {
                    return left.getDistribution().join(right.getDistribution())
                            .map(distribution -> MycatView.ofCondition(join.copy(join.getTraitSet(), ImmutableList.of(left.getRelNode(), right.getRelNode())), distribution,
                                    conditions));
                }
            }
        }
        return Optional.empty();
    }

    private static boolean isSameTargetPartitionGlobalOrNormalJoin(MycatView left, MycatView right, IdentityHashMap<RelColumnOrigin, RelColumnOrigin> equals) {
        return equals.entrySet().stream().anyMatch(entry -> {
            RelColumnOrigin leftColumnOrigin = entry.getKey();
            RelColumnOrigin rightColumnOrigin = entry.getValue();

            MycatLogicTable leftRelNode = leftColumnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
            MycatLogicTable rightRelNode = rightColumnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
            LogicTableType leftTableType = leftRelNode.getTable().getType();
            LogicTableType rightTableType = rightRelNode.getTable().getType();

            if (leftTableType == LogicTableType.SHARDING) {
                if (rightTableType == LogicTableType.GLOBAL || rightTableType == LogicTableType.NORMAL) {
                    return isSameTargetPartitionJoin(left, leftColumnOrigin, leftRelNode, rightRelNode);
                }
            } else if (leftTableType == LogicTableType.GLOBAL || leftTableType == LogicTableType.NORMAL) {
                if (rightTableType == LogicTableType.SHARDING) {
                    return isSameTargetPartitionJoin(right, leftColumnOrigin, rightRelNode, rightRelNode);
                }
            }
            return false;
        });
    }

    private static boolean isSameTargetPartitionJoin(MycatView leftView, IdentityHashMap<RelColumnOrigin, RelColumnOrigin> equals) {
        return equals.entrySet().stream().anyMatch(entry -> {
            RelColumnOrigin leftColumnOrigin = entry.getKey();
            RelColumnOrigin rightColumnOrigin = entry.getValue();

            MycatLogicTable leftRelNode = leftColumnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
            MycatLogicTable rightRelNode = rightColumnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
            LogicTableType leftTableType = leftRelNode.getTable().getType();
            LogicTableType rightTableType = rightRelNode.getTable().getType();

            if (leftTableType == LogicTableType.SHARDING) {
                ShardingTable leftTableHandler = (ShardingTable) leftRelNode.logicTable();
                SimpleColumnInfo lColumn = leftTableHandler.getColumns().get(leftColumnOrigin.getOriginColumnOrdinal());
                CustomRuleFunction lFunction = leftTableHandler.getShardingFuntion();
                if (isInPartitionKey(leftView, lFunction)) {
                    if (rightTableType == LogicTableType.SHARDING) {
                        ShardingTable rightTableHandler = (ShardingTable) rightRelNode.logicTable();
                        SimpleColumnInfo rColumn = rightTableHandler.getColumns().get(rightColumnOrigin.getOriginColumnOrdinal());
                        CustomRuleFunction rFunction = rightTableHandler.getShardingFuntion();
                        return Distribution.isTargetPartitionJoin(lFunction, rFunction);
                    }
                    return false;
                } else {
                    return false;
                }
            }
            return false;
        });
    }

    private static boolean isErJoin(IdentityHashMap<RelColumnOrigin, RelColumnOrigin> equals) {
        return equals.entrySet().stream().allMatch(entry -> {
            RelColumnOrigin leftColumnOrigin = entry.getKey();
            RelColumnOrigin rightColumnOrigin = entry.getValue();

            MycatLogicTable leftRelNode = leftColumnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
            MycatLogicTable rightRelNode = rightColumnOrigin.getOriginTable().unwrap(MycatLogicTable.class);
            LogicTableType leftTableType = leftRelNode.getTable().getType();
            LogicTableType rightTableType = rightRelNode.getTable().getType();

            if (leftTableType == LogicTableType.SHARDING) {
                ShardingTable leftTableHandler = (ShardingTable) leftRelNode.logicTable();
                SimpleColumnInfo lColumn = leftTableHandler.getColumns().get(leftColumnOrigin.getOriginColumnOrdinal());
                CustomRuleFunction lFunction = leftTableHandler.getShardingFuntion();

                if (rightTableType == LogicTableType.SHARDING) {
                    ShardingTable rightTableHandler = (ShardingTable) rightRelNode.logicTable();
                    SimpleColumnInfo rColumn = rightTableHandler.getColumns().get(rightColumnOrigin.getOriginColumnOrdinal());
                    CustomRuleFunction rFunction = rightTableHandler.getShardingFuntion();
                    if (lFunction.isSameDistribution(rFunction)) {
                        return lFunction.getColumnInfo(lColumn.getColumnName()).equals(rFunction.getColumnInfo(rColumn.getColumnName()));
                    }
                }
            }
            return false;
        });
    }

    private static boolean isSameTargetPartitionJoin(MycatView left, RelColumnOrigin leftColumnOrigin, MycatLogicTable leftRelNode, MycatLogicTable rightRelNode) {
        ShardingTable leftTableHandler = (ShardingTable) leftRelNode.logicTable();
        SimpleColumnInfo lColumn = leftTableHandler.getColumns().get(leftColumnOrigin.getOriginColumnOrdinal());
        CustomRuleFunction lFunction = leftTableHandler.getShardingFuntion();
        LogicTableType rightTableType = rightRelNode.getTable().getType();
        boolean inPartitionKey = isInPartitionKey(left, lFunction);
        boolean sameTargetPartitionJoin = false;
        if (rightTableType == LogicTableType.NORMAL) {
            NormalTable rightTableHandler = (NormalTable) rightRelNode.logicTable();
            {//partition key
                if (DrdsSqlCompiler.RBO_PARTITION_KEY_JOIN && inPartitionKey
                        && lFunction.isShardingPartitionKey(lColumn.getColumnName()) &&
                        lFunction.isAllPartitionInTargetName(rightTableHandler.getDataNode().getTargetName())) {
                    sameTargetPartitionJoin = true;
                }
            }
        } else if (rightTableType == LogicTableType.GLOBAL) {
            {//partition key
                if (DrdsSqlCompiler.RBO_PARTITION_KEY_JOIN && inPartitionKey
                        && lFunction.isShardingPartitionKey(lColumn.getColumnName())) {
                    sameTargetPartitionJoin = true;
                }
            }
        }
        return sameTargetPartitionJoin;
    }

    private static boolean isErJoinEqualColumn(SimpleColumnInfo lColumn, CustomRuleFunction lFunction, SimpleColumnInfo rColumn, CustomRuleFunction rFunction) {
        boolean erJoin;
        erJoin = lFunction.isSameDistribution(rFunction)
                &&
                lFunction.isShardingDbKey(lColumn.getColumnName())
                        ==
                        rFunction.isShardingDbKey(rColumn.getColumnName())
                &&
                lFunction.isShardingTableKey(lColumn.getColumnName())
                        ==
                        rFunction.isShardingTableKey(rColumn.getColumnName());
        return erJoin;
    }

    private static boolean isInPartitionKey(MycatView right, CustomRuleFunction rFunction) {
        boolean hasPartition = right.getPredicateIndexCondition().stream().filter(i -> i.getQueryType() == QueryType.PK_POINT_QUERY)
                .anyMatch(i -> i.getIndexColumnNames().stream().anyMatch(c -> rFunction.isShardingPartitionKey(c)));
        if (hasPartition) {
            return true;
        }
        return rFunction.requireShardingKeys(right.getPredicateIndexCondition().stream()
                .filter(i -> i.getQueryType() == QueryType.PK_POINT_QUERY).flatMap(i -> i.getIndexColumnNames().stream()).collect(Collectors.toSet()));

    }


    private static Optional<RelNode> pushDownJoinByNormalTableOrGlobalTable(Join join,
                                                                            MycatView left,
                                                                            MycatView right) {
        Distribution ldistribution = left.getDistribution();
        Distribution rdistribution = right.getDistribution();
        Distribution.Type lType = ldistribution.type();
        Distribution.Type rType = rdistribution.type();

        boolean asBroadcast = ldistribution.canAsBroadcast(rdistribution);

        if (lType == Distribution.Type.SHARDING &&
                (rType == Distribution.Type.BROADCAST || rType == Distribution.Type.PHY && asBroadcast)
        ) {
            switch (join.getJoinType()) {
                case INNER:
                case LEFT:
                case SEMI:
                case ANTI:
                    break;
                case RIGHT:
                    RelHint lastPushJoinHint = HintTools.getLastPushJoinHint(join.getHints());
                    if (lastPushJoinHint != null) {
                        if ("push_down_join_broadcast".equalsIgnoreCase(lastPushJoinHint.hintName)) {
                            break;
                        }
                    }
                    ServerConfig serverConfig = getServerConfig();
                    if (serverConfig.isForcedPushDownBroadcast()) {
                        break;
                    }
                    return Optional.empty();
                case FULL:
                    return Optional.empty();
            }
        } else if (
                (lType == Distribution.Type.BROADCAST || lType == Distribution.Type.PHY && asBroadcast)
                        && rType == Distribution.Type.SHARDING) {
            switch (join.getJoinType()) {
                case INNER:
                case RIGHT:
                    break;
                case SEMI:
                case ANTI:
                case LEFT:
                    RelHint lastPushJoinHint = HintTools.getLastPushJoinHint(join.getHints());
                    if (lastPushJoinHint != null) {
                        if ("push_down_join_broadcast".equalsIgnoreCase(lastPushJoinHint.hintName)) {
                            break;
                        }
                    }
                    ServerConfig serverConfig = getServerConfig();
                    if (serverConfig.isForcedPushDownBroadcast()) {
                        break;
                    }
                    return Optional.empty();
                case FULL:
                    return Optional.empty();
            }
        } else if (lType == Distribution.Type.PHY && rType == Distribution.Type.BROADCAST ||
                (lType == Distribution.Type.BROADCAST && rType == Distribution.Type.PHY)) {
            switch (join.getJoinType()) {
                case INNER:
                case LEFT:
                case SEMI:
                case ANTI:
                case RIGHT:
                    break;
                case FULL:
                    return Optional.empty();
            }
        } else if (lType == Distribution.Type.PHY && rType == Distribution.Type.PHY) {

        } else if (lType == Distribution.Type.BROADCAST && rType == Distribution.Type.BROADCAST) {

        } else {
            return Optional.empty();
        }
        return ldistribution.join(rdistribution).map(distribution -> MycatView.ofBottom(
                join.copy(join.getTraitSet(), ImmutableList.of(left.getRelNode(), right.getRelNode())),
                distribution));
    }

    private static ServerConfig getServerConfig() {
        return MetaClusterCurrent.exist(ServerConfig.class) ? MetaClusterCurrent.wrapper(ServerConfig.class) : new ServerConfig();
    }


    public static RelNode filter(RelNode original, Filter filter) {
        RelNode input = original;
        Distribution dataNodeInfo = null;
        MycatView view = null;
        if (input instanceof MycatView) {
            dataNodeInfo = ((MycatView) input).getDistribution();
            view = (MycatView) original;
            input = ((MycatView) input).getRelNode();
        }

        if (input instanceof TableScan) {
            RexNode condition = filter.getCondition();
            RelOptTable table = input.getTable();
            MycatLogicTable mycatTable = table.unwrap(MycatLogicTable.class);
            return MycatView.ofCondition(filter.copy(filter.getTraitSet(), input, condition), mycatTable.createDistribution(), (condition));
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
        filter = (Filter) filter.copy(filter.getTraitSet(), ImmutableList.of(input));
        if (view != null) {
            return view.changeTo(filter, dataNodeInfo);
        } else {
            return filter;
        }
    }

    public static RelNode project(RelNode original, Project project) {
        Distribution dataNodeInfo = null;
        RelNode input = original;
        MycatView mycatView = null;
        if (RexUtil.isIdentity(project.getProjects(), original.getRowType())) {
            return original;//ProjectRemoveRule
        }
        if (input instanceof MycatView) {
            dataNodeInfo = ((MycatView) input).getDistribution();
            mycatView = (MycatView) original;
            input = ((MycatView) input).getRelNode();

            if (project.containsOver()) {
                if (dataNodeInfo.type() == Distribution.Type.PHY || (dataNodeInfo.type() == Distribution.Type.BROADCAST)) {
                    input = project.copy(project.getTraitSet(), ImmutableList.of(input));
                    return mycatView.changeTo(input, dataNodeInfo);
                }
            }
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
        if (mycatView != null) {
            return mycatView.changeTo(input, dataNodeInfo);
        } else {
            return input;
        }
    }

}