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
package io.mycat.calcite.rewriter;

import com.google.common.collect.ImmutableList;
import io.mycat.LogicTableType;
import io.mycat.SimpleColumnInfo;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatConvention;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatMergeSort;
import io.mycat.calcite.sqlfunction.infofunction.MycatSessionValueFunction;
import io.mycat.calcite.table.*;
import io.mycat.router.CustomRuleFunction;
import io.mycat.util.NameMap;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.*;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.mapping.IntPair;

import java.math.BigDecimal;
import java.text.NumberFormat;
import java.util.*;

import static org.apache.calcite.rel.rules.CoreRules.*;


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
            if (abstractMycatTable.isCustom()) {
                MycatLogicTable mycatLogicTable = (MycatLogicTable) abstractMycatTable;
                CustomTableHandlerWrapper customTableHandler = (CustomTableHandlerWrapper) mycatLogicTable.getTable();
                QueryBuilder queryBuilder = customTableHandler.createQueryBuilder(scan.getCluster());
                queryBuilder.setRowType(scan.getRowType());
                return queryBuilder;
            }
            return MycatView.ofBottom(scan, abstractMycatTable.createDistribution());
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
                return filter(input, filter);
            }
        }
        return filter.copy(filter.getTraitSet(), ImmutableList.of(input));
    }

    @Override
    public RelNode visit(LogicalCalc calc) {
        final Pair<ImmutableList<RexNode>, ImmutableList<RexNode>> projectFilter =
                calc.getProgram().split();
        RelBuilder relBuilder = MycatCalciteSupport.relBuilderFactory.create(calc.getCluster(), null);
        relBuilder.push(calc.getInput());
        relBuilder.filter(projectFilter.right);
        relBuilder.project(projectFilter.left, calc.getRowType().getFieldNames());
        RelNode relNode = relBuilder.build();
        return relNode.accept(this);
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
            if (operator == MycatSessionValueFunction.INSTANCE) {
                containsUsedDefinedFunction = true;
            }
            return super.visitCall(call);
        }

        @Override
        public Void visitCorrelVariable(RexCorrelVariable correlVariable) {
            containsUsedDefinedFunction = true;
            return super.visitCorrelVariable(correlVariable);
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
        boolean lr = RelMdSqlViews.join(left);
        boolean rr = RelMdSqlViews.join(right);
        if (lr && rr) {
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
        if (RelMdSqlViews.aggregate(input)) {
            return aggregate(input, aggregate);
        } else {
            return aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
        }
    }

    @Override
    public RelNode visit(LogicalMatch match) {
        return match;
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
        return exchange.copy(exchange.getTraitSet(), ImmutableList.of(exchange.getInput().accept(this)));
    }

    @Override
    public RelNode visit(LogicalTableModify modify) {
        RelNode relNode = modify.getInput().accept(this);
        return modify.copy(modify.getTraitSet(), ImmutableList.of(relNode));
    }

    @Override
    public RelNode visit(RelNode other) {
        if (other instanceof LogicalCalc){
           return visit((LogicalCalc)other);
        }
        return other;
    }

    public static RelNode sort(RelNode original, LogicalSort sort) {
        RelNode input = original;
        MycatView view = null;
        Distribution dataNodeInfo = null;
        if (input instanceof MycatView) {
            dataNodeInfo = ((MycatView) input).getDistribution();
            view = (MycatView) original;
            input = ((MycatView) input).getRelNode();
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
        if (dataNodeInfo.type() == Distribution.Type.PHY || dataNodeInfo.type() == Distribution.Type.BroadCast) {
            input = sort.copy(input.getTraitSet(), ImmutableList.of(input));
            return view.changeTo(input, dataNodeInfo);
        } else {
            if (sort.offset == null && sort.fetch == null) {
                input = LogicalSort.create(input, sort.getCollation()
                        , null
                        , null);
                input = view.changeTo(input, dataNodeInfo);
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
            input = view.changeTo(input, dataNodeInfo);
            return MycatMergeSort.create(
                    input.getTraitSet().replace(MycatConvention.INSTANCE),
                    input,
                    sort.collation, sort.offset, sort.fetch);
        }
    }


    public static RelNode aggregate(RelNode original, LogicalAggregate aggregate) {
        RelNode input = original;
        Distribution dataNodeInfo = null;
        MycatView view = null;
        if (input instanceof MycatView) {
            dataNodeInfo = ((MycatView) input).getDistribution();
            view = (MycatView) original;
            input = ((MycatView) input).getRelNode();
        }
        if (dataNodeInfo == null) {
            input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
            return input;
        }
        if (dataNodeInfo.type() == Distribution.Type.PHY || dataNodeInfo.type() == Distribution.Type.BroadCast) {
            input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
            return view.changeTo(input, dataNodeInfo);
        } else {
            ColumnRefResolver columnMapping = new ColumnRefResolver();
            input.accept(columnMapping);
            ImmutableBitSet groupSet = aggregate.getGroupSet();
            boolean canPushDown = false;
            for (Integer integer : groupSet) {
                ColumnInfo bottomColumnInfo = columnMapping.getBottomColumnInfoList(integer).stream()
                        .filter(i -> i.getTableScan().getTable().unwrap(MycatLogicTable.class).isSharding())
                        .findFirst().orElse(null);
                if (bottomColumnInfo == null) {
                    continue;
                }
                MycatLogicTable shardingTable = bottomColumnInfo.getTableScan().getTable().unwrap(MycatLogicTable.class);
                List<SimpleColumnInfo> columns = shardingTable.getTable().getColumns();
                SimpleColumnInfo simpleColumnInfo = columns.get(bottomColumnInfo.getIndex());
                if (simpleColumnInfo.isShardingKey()) {
                    canPushDown = true;
                    break;
                }
            }
            if (canPushDown) {
                input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
                return view.changeTo(input, dataNodeInfo);
            }


            RelNode backup = input;
            if (!(input instanceof Union)) {
                input = LogicalUnion.create(ImmutableList.of(input, input), true);
                input = aggregate.copy(aggregate.getTraitSet(), ImmutableList.of(input));
            }
            HepProgramBuilder hepProgram = new HepProgramBuilder();
            hepProgram.addMatchLimit(1024);
            hepProgram.addRuleInstance(CoreRules.AGGREGATE_UNION_TRANSPOSE);
            HepPlanner planner = new HepPlanner(hepProgram.build());
            planner.setRoot(input);
            RelNode bestExp = planner.findBestExp();
            if (bestExp instanceof Aggregate && ((Aggregate) bestExp).getInput() instanceof Union) {
                MycatView multiView = view.changeTo(
                        bestExp.getInput(0).getInput(0),
                        dataNodeInfo);
                return bestExp.copy(aggregate.getTraitSet(), ImmutableList.of(multiView));
            } else {
                return view.changeTo(
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


    public static RelNode join(RelNode left,
                               RelNode right,
                               LogicalJoin join) {
        Optional<RelNode> relNodeOptional = bottomJoin(left, right, join);
        if (relNodeOptional.isPresent()) return relNodeOptional.get();
        Join newJoin = join.copy(join.getTraitSet(), ImmutableList.of(left, right));
        if (!(newJoin instanceof MycatRel) && newJoin.getJoinType() == JoinRelType.INNER) {
            int orgJoinCount = RelOptUtil.countJoins(newJoin);
            RelOptCluster cluster = newJoin.getCluster();
            RelOptPlanner planner = cluster.getPlanner();
            planner.clear();
            MycatConvention.INSTANCE.register(planner);
            planner.addRule(CoreRules.JOIN_COMMUTE);
            planner.addRule(CoreRules.JOIN_COMMUTE_OUTER);
            planner.addRule(CoreRules.JOIN_ASSOCIATE);
            planner.addRule(CoreRules.FILTER_INTO_JOIN);
            planner.addRule(CoreRules.JOIN_PUSH_EXPRESSIONS);
            planner.addRule(CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES);
            planner.addRule(MycatJoinClusteringRule.Config.DEFAULT.toRule());
            planner.addRule(MycatProjectJoinClusteringRule.Config.DEFAULT.toRule());
            planner.addRule(MycatJoinPushThroughJoinRule.LEFT);
            planner.addRule(MycatJoinPushThroughJoinRule.RIGHT);
            planner.addRule(MycatFilterJoinRule.JoinConditionPushRule.Config.DEFAULT.withPredicate((join1, joinType, exp) -> false).toRule());
            planner.setRoot(planner.changeTraits(newJoin, cluster.traitSetOf(MycatConvention.INSTANCE)));

            RelNode bestExp = planner.findBestExp();
            if (RelOptUtil.countJoins(bestExp) < orgJoinCount) {
                return bestExp;
            }
        }
        return newJoin;
    }


    public static Optional<RelNode> bottomJoin(RelNode left, RelNode right, LogicalJoin join) {
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


    private static Optional<RelNode> pushDownERTable(LogicalJoin join,
                                                     MycatView left,
                                                     MycatView right) {
        JoinInfo joinInfo = join.analyzeCondition();
        if (joinInfo.isEqui()) {
            List<IntPair> pairs = joinInfo.pairs();
            if (pairs.isEmpty()) return Optional.empty();

            RexNode conditions = left.getConditions().orElse(right.getConditions().orElse(null));

            ColumnRefResolver leftColumnMapping = new ColumnRefResolver();
            ColumnRefResolver rightColumnMapping = new ColumnRefResolver();
            left.getRelNode().accept(leftColumnMapping);
            right.getRelNode().accept(rightColumnMapping);

            for (IntPair pair : pairs) {

                Optional<ColumnInfo> leftBottomColumnInfoOptional = leftColumnMapping.getBottomColumnInfoList(pair.source).stream()
                        .filter(i -> i.getTableScan().getTable().unwrap(MycatLogicTable.class).isSharding()).findFirst();
                Optional<ColumnInfo> rightBottomColumnInfoOptional = rightColumnMapping.getBottomColumnInfoList(pair.target).stream()
                        .filter(i -> i.getTableScan().getTable().unwrap(MycatLogicTable.class).isSharding()).findFirst();

                if (leftBottomColumnInfoOptional.isPresent() && rightBottomColumnInfoOptional.isPresent()) {
                    ColumnInfo leftBottomColumnInfo = leftBottomColumnInfoOptional.get();
                    ColumnInfo rightBottomColumnInfo = rightBottomColumnInfoOptional.get();
                    MycatLogicTable leftRelNode = leftBottomColumnInfo.getTableScan().getTable().unwrap(MycatLogicTable.class);
                    MycatLogicTable rightRelNode = rightBottomColumnInfo.getTableScan().getTable().unwrap(MycatLogicTable.class);
                    LogicTableType leftTableType = leftRelNode.getTable().getType();
                    LogicTableType rightTableType = rightRelNode.getTable().getType();

                    if (leftTableType == LogicTableType.SHARDING && leftTableType == rightTableType) {
                        ShardingTable leftTableHandler = (ShardingTable) leftRelNode.logicTable();
                        ShardingTable rightTableHandler = (ShardingTable) rightRelNode.logicTable();

                        SimpleColumnInfo lColumn = leftTableHandler.getColumns().get(leftBottomColumnInfo.getIndex());
                        SimpleColumnInfo rColumn = rightTableHandler.getColumns().get(rightBottomColumnInfo.getIndex());

                        CustomRuleFunction lFunction = leftTableHandler.getShardingFuntion();
                        CustomRuleFunction rFunction = rightTableHandler.getShardingFuntion();
                        if (lFunction.isShardingDbKey(lColumn.getColumnName())
                                ==
                                rFunction.isShardingDbKey(rColumn.getColumnName())
                                &&
                                lFunction.isShardingTableKey(lColumn.getColumnName())
                                        ==
                                        rFunction.isShardingTableKey(rColumn.getColumnName())) {
                            return left.getDistribution().join(right.getDistribution())
                                    .map(distribution -> MycatView.ofCondition(join.copy(join.getTraitSet(), ImmutableList.of(left.getRelNode(), right.getRelNode())), distribution,
                                            conditions));
                        }
                    }

                }
            }
        }
        return Optional.empty();
    }


    private static Optional<RelNode> pushDownJoinByNormalTableOrGlobalTable(LogicalJoin join,
                                                                            MycatView left,
                                                                            MycatView right) {
        Distribution ldistribution = left.getDistribution();
        Distribution rdistribution = right.getDistribution();
        Distribution.Type lType = ldistribution.type();
        Distribution.Type rType = rdistribution.type();
        if (lType != Distribution.Type.Sharding && rType != Distribution.Type.Sharding) {
            return ldistribution.join(rdistribution).map(distribution -> MycatView.ofBottom(
                    join.copy(join.getTraitSet(), ImmutableList.of(left.getRelNode(), right.getRelNode())),
                    distribution));
        }
        if (lType == Distribution.Type.BroadCast || rType == Distribution.Type.BroadCast) {
            return ldistribution.join(rdistribution).map(distribution -> MycatView.ofBottom(
                    join.copy(join.getTraitSet(), ImmutableList.of(left.getRelNode(), right.getRelNode())),
                    distribution));
        }
        return Optional.empty();
    }

    public static RelNode filter(RelNode original, LogicalFilter filter) {
        RelNode input = original;
        Distribution dataNodeInfo = null;
        MycatView view = null;
        if (input instanceof MycatView) {
            dataNodeInfo = ((MycatView) input).getDistribution();
            view = (MycatView) original;
            input = ((MycatView) input).getRelNode();
        }

        if (input instanceof LogicalTableScan) {
            RexNode condition = filter.getCondition();
            RelOptTable table = input.getTable();
            AbstractMycatTable nodes = table.unwrap(AbstractMycatTable.class);
            Distribution distribution = nodes.createDistribution();
            return MycatView.ofCondition(filter.copy(filter.getTraitSet(), input, condition), distribution, (condition));
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
        if (view != null) {
            return view.changeTo(filter, dataNodeInfo);
        } else {
            return filter;
        }
    }

    public static RelNode project(RelNode original, LogicalProject project) {
        Distribution dataNodeInfo = null;
        RelNode input = original;
        MycatView mycatView = null;
        if (input instanceof MycatView) {
            dataNodeInfo = ((MycatView) input).getDistribution();
            mycatView = (MycatView) original;
            input = ((MycatView) input).getRelNode();
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