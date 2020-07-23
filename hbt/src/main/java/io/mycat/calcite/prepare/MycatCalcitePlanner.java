/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite.prepare;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatRelBuilder;
import io.mycat.calcite.rules.LimitPushRemoveRule;
import io.mycat.calcite.rules.MycatProjectTransportRule;
import io.mycat.calcite.rules.PushDownLogicTableRule;
import io.mycat.calcite.table.MycatPhysicalTable;
import io.mycat.calcite.table.MycatSQLTableScan;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import io.mycat.calcite.table.SingeTargetSQLTable;
import io.mycat.upondb.MycatDBContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;

/**
 * @author Junwen Chen
 **/
public class MycatCalcitePlanner extends PlannerImpl implements RelOptTable.ViewExpander {
    private final static Logger LOGGER = LoggerFactory.getLogger(MycatCalcitePlanner.class);
    private final SchemaPlus rootSchema;

    private MycatCalciteDataContext dataContext;

    public MycatCalcitePlanner(MycatCalciteDataContext dataContext) {
        super(dataContext);
        this.dataContext = dataContext;
        this.rootSchema = dataContext.getRootSchema();
    }


    public MycatRelBuilder createRelBuilder(RelOptCluster cluster) {
        return (MycatRelBuilder) MycatCalciteSupport.INSTANCE.relBuilderFactory.create(cluster, createCalciteCatalogReader());
    }

    public MycatRelBuilder createRelBuilder() {
        return (MycatRelBuilder) MycatCalciteSupport.INSTANCE.relBuilderFactory.create(newCluster(), createCalciteCatalogReader());
    }

    public SqlValidatorImpl getSqlValidator() {
        SqlOperatorTable opTab = MycatCalciteSupport.INSTANCE.config.getOperatorTable();
        SqlValidatorCatalogReader catalogReader = createCalciteCatalogReader();
        RelDataTypeFactory typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
        return (SqlValidatorImpl) SqlValidatorUtil.newValidator(opTab, catalogReader, typeFactory, MycatCalciteSupport.INSTANCE.getValidatorConfig());
    }

    public SqlToRelConverter createSqlToRelConverter() {
        return new SqlToRelConverter(this, getSqlValidator(),
                createCalciteCatalogReader(), newCluster(), MycatCalciteSupport.MycatStandardConvertletTable.INSTANCE, MycatCalciteSupport.INSTANCE.sqlToRelConverterConfig);
    }

    //    List<RelOptRule> relOptRules = Arrays.asList(
//            FilterTableScanRule.INSTANCE,
//            ProjectTableScanRule.INSTANCE,
//            FilterSetOpTransposeRule.INSTANCE,
//            ProjectRemoveRule.INSTANCE,
//            JoinUnionTransposeRule.LEFT_UNION,
//            JoinUnionTransposeRule.RIGHT_UNION,
//            JoinExtractFilterRule.INSTANCE,
//            JoinPushTransitivePredicatesRule.INSTANCE,
//            AggregateUnionTransposeRule.INSTANCE,
//            AggregateUnionAggregateRule.AGG_ON_FIRST_INPUT,
//            AggregateUnionAggregateRule.AGG_ON_SECOND_INPUT,
//            AggregateUnionAggregateRule.INSTANCE,
//            AggregateProjectMergeRule.INSTANCE,//下推聚合
//            AggregateProjectPullUpConstantsRule.INSTANCE,
//            PushDownLogicTable.INSTANCE_FOR_PushDownFilterLogicTable
//    );
//目的是下推谓词,所以添加 Filter 相关
    static final ImmutableSet<RelOptRule> FILTER = ImmutableSet.of(
            FilterAggregateTransposeRule.INSTANCE,
            FilterCorrelateRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            FilterJoinRule.DUMB_FILTER_ON_JOIN,
            FilterMergeRule.INSTANCE,
            FilterMultiJoinMergeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            FilterRemoveIsNotDistinctFromRule.INSTANCE,
            FilterTableScanRule.INSTANCE,
            FilterSetOpTransposeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            SemiJoinFilterTransposeRule.INSTANCE,
            ProjectJoinTransposeRule.INSTANCE,
//        ReduceExpressionsRule.FILTER_INSTANCE,
//        ReduceExpressionsRule.JOIN_INSTANCE,
            //https://issues.apache.org/jira/browse/CALCITE-4045
//        ReduceExpressionsRule.PROJECT_INSTANCE,
//            ProjectFilterTransposeRule.INSTANCE,//产生子查询
            FilterTableScanRule.INSTANCE,
            JoinPushTransitivePredicatesRule.INSTANCE,
            JoinPushTransitivePredicatesRule.INSTANCE,
            ProjectTableScanRule.INSTANCE,
            JoinExtractFilterRule.INSTANCE,
            Bindables.BINDABLE_TABLE_SCAN_RULE

    );

    public RelNode eliminateLogicTable(final RelNode originExpr) {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder()
                .addMatchLimit(FILTER.size())
                .addRuleCollection(FILTER);
        HepPlanner planner = new HepPlanner(hepProgramBuilder.build());
        planner.setRoot(originExpr);
        RelNode pushRes = Objects.requireNonNull(planner.findBestExp());

        hepProgramBuilder = new HepProgramBuilder()
                .addMatchLimit(FILTER.size())
                .addRuleCollection(ImmutableList.of(
                        PushDownLogicTableRule.LogicalTable,
                        PushDownLogicTableRule.BindableTableScan
                ));

        planner = new HepPlanner(hepProgramBuilder.build());
        planner.setRoot(pushRes);
        return Objects.requireNonNull(planner.findBestExp());
    }

    public RelNode pushDownBySQL(RelNode bestExp, boolean forUpdate) {
        return pushDownBySQL(createRelBuilder(bestExp.getCluster()), bestExp, forUpdate);
    }

    public RelNode pushDownBySQL(MycatRelBuilder relBuilder, final RelNode bestExp0, boolean forUpdate) {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addMatchLimit(1);
        hepProgramBuilder.addMatchOrder(HepMatchOrder.TOP_DOWN);
        hepProgramBuilder.addRuleInstance(LimitPushRemoveRule.INSTANCE);
        HepProgram build = hepProgramBuilder.build();

        RelOptPlanner planner = new HepPlanner(build);

        planner.setRoot(bestExp0);
        final RelNode bestExp1 = planner.findBestExp();
        ComputePushDownInfo computePushDownInfo = new ComputePushDownInfo(bestExp1).invoke();
        IdentityHashMap<RelNode, Boolean> cache = computePushDownInfo.getCache();
        IdentityHashMap<RelNode, List<String>> margeList = computePushDownInfo.getMargeList();

        //从根节点开始把变成SQL下推
        RelHomogeneousShuttle relHomogeneousShuttle1 = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                /**
                 * 跳过集合操作,不能下推union
                 */
                if (other instanceof SetOp) {
                    return super.visit(other);
                }
                if (cache.get(other) == Boolean.TRUE) {
                    List<String> strings = margeList.get(other);
                    if (strings == null || strings != null && strings.isEmpty()) {
                        //不翻译无表sql
                    } else {
                        String targetName = strings.get(0);
                        targetName = dataContext.getUponDBContext().resolveFinalTargetName(targetName);
                        return relBuilder.makeTransientSQLScan(targetName, other, forUpdate);
                    }
                }
                return super.visit(other);
            }
        };
        return Objects.requireNonNull(bestExp1.accept(relHomogeneousShuttle1));
    }

    /**
     * 测试单库分表与不同分片两个情况
     *
     * @param relBuilder
     * @param cache
     * @param margeList
     * @param bestExp2
     * @return
     */
    private RelNode simplyAggreate(MycatRelBuilder relBuilder, IdentityHashMap<RelNode, Boolean> cache, IdentityHashMap<RelNode, List<String>> margeList, RelNode bestExp2) {
        RelNode parent = bestExp2;
        RelNode child = bestExp2 instanceof Aggregate ? bestExp2.getInput(0) : null;
        RelNode bestExp3 = parent;
        if (parent instanceof Aggregate && child instanceof Union) {
            Aggregate aggregate = (Aggregate) parent;
            if (aggregate.getAggCallList() != null && !aggregate.getAggCallList().isEmpty()) {//distinct会没有参数
                List<AggregateCall> aggCallList = aggregate.getAggCallList();
                boolean allMatch = aggregate.getRowType().getFieldCount() == 1 && aggCallList.stream().allMatch(new Predicate<AggregateCall>() {
                    @Override
                    public boolean test(AggregateCall aggregateCall) {
                        return SUPPORTED_AGGREGATES.getOrDefault(aggregateCall.getAggregation().getKind(), false)
                                &&
                                aggregate.getRowType().getFieldList().stream().allMatch(i -> i.getType().getSqlTypeName().getFamily() == SqlTypeFamily.NUMERIC);
                    }
                });
                if (allMatch) {
                    List<RelNode> inputs = child.getInputs();
                    List<RelNode> resList = new ArrayList<>(inputs.size());
                    boolean allCanPush = true;//是否聚合节点涉及不同分片
                    String target = null;
                    for (RelNode input : inputs) {
                        RelNode res;
                        if (cache.get(input)) {
                            res = LogicalAggregate.create(input, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
                            cache.put(res, Boolean.TRUE);
                            List<String> strings = margeList.getOrDefault(input, Collections.emptyList());
                            Objects.requireNonNull(strings);
                            if (target == null && strings.size() > 0) {
                                target = strings.get(0);
                            } else if (target != null && strings.size() > 0) {
                                if (!target.equals(strings.get(0))) {
                                    allCanPush = false;
                                }
                            }
                            margeList.put(res, strings);
                        } else {
                            res = input;
                            allCanPush = false;
                        }
                        resList.add(res);
                    }

                    LogicalUnion logicalUnion = LogicalUnion.create(resList, ((Union) child).all);

                    //构造sum
                    relBuilder.clear();
                    relBuilder.push(logicalUnion);
                    List<RexNode> fields = relBuilder.fields();
                    if (fields == null) {
                        fields = Collections.emptyList();
                    }

                    RelBuilder.GroupKey groupKey = relBuilder.groupKey();
                    List<RelBuilder.AggCall> aggCalls = fields.stream().map(i -> relBuilder.sum(i)).collect(Collectors.toList());
                    relBuilder.aggregate(groupKey, aggCalls);
                    bestExp3 = relBuilder.build();

                    cache.put(logicalUnion, allCanPush);
                    cache.put(bestExp3, allCanPush);
                    if (target != null) {//是否聚合节点涉及不同分片
                        List<String> targetSingelList = Collections.singletonList(target);
                        margeList.put(logicalUnion, targetSingelList);
                        margeList.put(bestExp3, targetSingelList);
                    }
                }
            }
        }
        return bestExp3;
    }


    public static RelNode toPhysical(RelNode rel, Consumer<RelOptPlanner> setting) {
        final RelOptPlanner planner = rel.getCluster().getPlanner();
        planner.clear();
        setting.accept(planner);
        planner.addRule(new RelOptRule(operand(MycatTransientSQLTableScan.class, none()), RelFactories.LOGICAL_BUILDER, "MycatTransientSQLTableScan") {

            @Override
            public void onMatch(RelOptRuleCall call) {
                final MycatTransientSQLTableScan scan = call.rel(0);
                final RelOptTable table = scan.getTable();
                if (Bindables.BindableTableScan.canHandle(table)) {
                    call.transformTo(
                            Bindables.BindableTableScan.create(scan.getCluster(), table));
                }
            }
        });
        final Program program = Programs.of(RuleSets.ofList(planner.getRules()));
        return program.run(planner, rel, rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
                ImmutableList.of(), ImmutableList.of());
    }

    public RelOptCluster newCluster() {
        RelOptPlanner planner = new VolcanoPlanner();
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        return RelOptCluster.create(planner, MycatCalciteSupport.INSTANCE.RexBuilder);
    }

    public static final EnumMap<SqlKind, Boolean> SUPPORTED_AGGREGATES = new EnumMap<>(SqlKind.class);

    static {
        SUPPORTED_AGGREGATES.put(SqlKind.MIN, true);
        SUPPORTED_AGGREGATES.put(SqlKind.MAX, true);
        SUPPORTED_AGGREGATES.put(SqlKind.COUNT, true);
        SUPPORTED_AGGREGATES.put(SqlKind.SUM, true);
        SUPPORTED_AGGREGATES.put(SqlKind.SUM0, true);
        SUPPORTED_AGGREGATES.put(SqlKind.ANY_VALUE, true);
        SUPPORTED_AGGREGATES.put(SqlKind.BIT_AND, true);
        SUPPORTED_AGGREGATES.put(SqlKind.BIT_OR, true);
    }


    public void convert(String sql) {

    }

    public DatasourceInfo preComputeSeq(RelNode relNode) {
        MycatDBContext uponDBContext = dataContext.getUponDBContext();
        Map<String, List<SingeTargetSQLTable>> map = new HashMap<>();
        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                SingeTargetSQLTable unwrap = scan.getTable().unwrap(SingeTargetSQLTable.class);
                if (unwrap != null) {
                    List<SingeTargetSQLTable> preComputationSQLTables = map.computeIfAbsent(uponDBContext.resolveFinalTargetName(unwrap.getTargetName()), s -> new ArrayList<>(1));
                    preComputationSQLTables.add(unwrap);
                }
                return super.visit(scan);
            }
        });
        List<SingeTargetSQLTable> preSeq = new ArrayList<>();
        for (Map.Entry<String, List<SingeTargetSQLTable>> stringListEntry : map.entrySet()) {
            List<SingeTargetSQLTable> value = stringListEntry.getValue();
            int size = value.size() - 1;
            for (int i = 0; i < size; i++) {
                preSeq.add(value.get(i));
            }
        }
        return new DatasourceInfo(preSeq, map);
    }


    public RelNode convertToMycatRel(RelNode relNode) {
        return relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatSQLTableScan unwrap = scan.getTable().unwrap(MycatSQLTableScan.class);
                if (unwrap != null) {
                    return unwrap.toRel(ViewExpanders.toRelContext(MycatCalcitePlanner.this, MycatCalcitePlanner.this.newCluster()), scan.getTable());
                }
                return super.visit(scan);
            }
        });
    }

    //目的是上拉union,所以添加 union 相关
    //目的是消除project,减少产生的子查询 所以添加 project 相关
    static final ImmutableSet<RelOptRule> PULL_RULES = ImmutableSet.of(
            UnionEliminatorRule.INSTANCE,
//            UnionMergeRule.INTERSECT_INSTANCE,
            UnionMergeRule.INSTANCE,
//            UnionMergeRule.MINUS_INSTANCE,
//            UnionPullUpConstantsRule.INSTANCE,
            UnionToDistinctRule.INSTANCE,
//            ProjectCorrelateTransposeRule.INSTANCE,
//            ProjectFilterTransposeRule.INSTANCE,
            ProjectJoinJoinRemoveRule.INSTANCE,
            ProjectJoinRemoveRule.INSTANCE,
//            ProjectJoinTransposeRule.INSTANCE,
            ProjectMergeRule.INSTANCE,
//            ProjectMultiJoinMergeRule.INSTANCE,
            ProjectRemoveRule.INSTANCE,
            JoinUnionTransposeRule.LEFT_UNION,
            JoinUnionTransposeRule.RIGHT_UNION,
            AggregateProjectMergeRule.INSTANCE,//
            AggregateUnionTransposeRule.INSTANCE,//该实现可能有问题
            AggregateUnionAggregateRule.INSTANCE,
            AggregateProjectMergeRule.INSTANCE,
            AggregateRemoveRule.INSTANCE,
            AggregateProjectPullUpConstantsRule.INSTANCE2,
            MycatProjectTransportRule.INSTANCE,//org.apache.calcite.rel.rules.ProjectSetOpTransposeRule 该实现可能有问题
            ProjectSortTransposeRule.INSTANCE,
//            ProjectSetOpTransposeRule.INSTANCE,
//            AggregateCaseToFilterRule.INSTANCE,丢失条件，错误下推
//            AggregateFilterTransposeRule.INSTANCE,#该改造产生有问题的group by字段
            AggregateValuesRule.INSTANCE,
            //sort
//            SortJoinCopyRule.INSTANCE,#产生多余的sort
//            SortJoinTransposeRule.INSTANCE,
//            SortProjectTransposeRule.INSTANCE,
//            SortRemoveConstantKeysRule.INSTANCE,
            SortRemoveRule.INSTANCE,
//            SortUnionTransposeRule.INSTANCE,
//            SortUnionTransposeRule.MATCH_NULL_FETCH,
            SubQueryRemoveRule.FILTER,
            SubQueryRemoveRule.JOIN,
            SubQueryRemoveRule.PROJECT);

    public RelNode pullUpUnion(RelNode relNode1) {
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addMatchLimit(1024);
        hepProgramBuilder.addRuleCollection(PULL_RULES);
        final HepPlanner planner2 = new HepPlanner(hepProgramBuilder.build());
        planner2.setRoot(relNode1);
        return planner2.findBestExp();
    }


    private class ComputePushDownInfo {
        private RelNode bestExp1;
        private IdentityHashMap<RelNode, Boolean> cache;
        private IdentityHashMap<RelNode, List<String>> margeList;

        public ComputePushDownInfo(RelNode bestExp1) {
            this.bestExp1 = bestExp1;
        }

        public IdentityHashMap<RelNode, Boolean> getCache() {
            return cache;
        }

        public IdentityHashMap<RelNode, List<String>> getMargeList() {
            return margeList;
        }


        public ComputePushDownInfo invoke() {
            RelNode root = bestExp1;
            //子节点运算的节点是同一个目标的,就把它们的父节点标记为可以变成SQL
            cache = new IdentityHashMap<>();
            margeList = new IdentityHashMap<>();
            RelHomogeneousShuttle relHomogeneousShuttle = new RelHomogeneousShuttle() {
                @Override
                public RelNode visit(RelNode other) {
                    RelNode res = super.visit(other);//后续遍历


                    List<RelNode> inputs = other.getInputs();
                    boolean isLeftNode = inputs == null || other.getInputs() != null && other.getInputs().isEmpty();


                    if (!isLeftNode) {

                        ArrayList<String> targetList = new ArrayList<>();
                        for (RelNode input : inputs) {
                            targetList.addAll(margeList.getOrDefault(input, Collections.emptyList()));
                        }
                        Set<String> distinct = new HashSet<>(targetList);
                        margeList.put(other, targetList);
                        boolean b = other instanceof Aggregate && other != root;//控制深度为2的关系表达式节点是否是Aggregate

//                        if (other instanceof Correlate) {
//                            cache.put(other, false);//关联子查询(mycat不支持)和
//                        }

                        {
                            boolean distinctValue = distinct.isEmpty() || distinct.size() == 1;
                            cache.put(other, distinctValue);
                        }

                    } else {
                        MycatPhysicalTable mycatPhysicalTable = Optional.ofNullable(other.getTable()).map(i -> i.unwrap(MycatPhysicalTable.class)).orElse(null);
                        if (mycatPhysicalTable != null) {
                            margeList.put(other, Collections.singletonList(mycatPhysicalTable.getTargetName()));
                        } else {
                            margeList.put(other, Collections.emptyList());
                        }
                        cache.put(other, Boolean.TRUE);
                    }
                    //修正,不能影响上面流程
                    if (other instanceof SetOp || other instanceof Correlate) {
                        cache.put(other, false);//没有事务并行查询->总是并行查询
                        margeList.put(other, ImmutableList.of("a", "b"));//强制使union的数据源不一致,这样就不会下推union
                        return other;
                    }
                    return other;
                }
            };
            relHomogeneousShuttle.visit(bestExp1);
            return this;
        }
    }
}