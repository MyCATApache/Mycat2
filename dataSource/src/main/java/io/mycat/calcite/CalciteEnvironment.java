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
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.logic.MycatPhysicalTable;
import io.mycat.calcite.relBuilder.MycatTransientSQLTable;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.*;
import java.util.function.Consumer;

public enum CalciteEnvironment {
    INSTANCE;
    final Logger LOGGER = LoggerFactory.getLogger(CalciteEnvironment.class);

    private final EnumMap<SqlKind, Boolean> SUPPORTED_AGGREGATES = new EnumMap<>(SqlKind.class);


    private CalciteEnvironment() {
        SUPPORTED_AGGREGATES.put(SqlKind.MIN, true);
        SUPPORTED_AGGREGATES.put(SqlKind.MAX, true);
        SUPPORTED_AGGREGATES.put(SqlKind.COUNT, true);
        SUPPORTED_AGGREGATES.put(SqlKind.SUM, true);
        SUPPORTED_AGGREGATES.put(SqlKind.SUM0, true);
        SUPPORTED_AGGREGATES.put(SqlKind.ANY_VALUE, true);
        SUPPORTED_AGGREGATES.put(SqlKind.BIT_AND, true);
        SUPPORTED_AGGREGATES.put(SqlKind.BIT_OR, true);
    }

    public TextResultSetResponse getConnection(String defaultSchema, String sql) throws Exception {
        FrameworkConfig config = MycatCalciteContext.INSTANCE.create(defaultSchema);
        PlannerImpl planner = new PlannerImpl(config);
        SqlNode parse = planner.parse(sql);
        SqlNode validate = planner.validate(parse);
        RelNode convert = planner.convert(validate);
        HepProgram program =  new HepProgramBuilder().build();
        HepPlanner hepPlanner = new HepPlanner(program);
//        Arrays.asList(
//                FilterTableScanRule.INSTANCE,
//                ProjectTableScanRule.INSTANCE,
////                FilterSetOpTransposeRule.INSTANCE,
//// ProjectRemoveRule.INSTANCE,
//////        planner2.addRule(JoinUnionTransposeRule.LEFT_UNION,
//////        planner2.addRule(JoinUnionTransposeRule.RIGHT_UNION.
////    JoinExtractFilterRule.INSTANCE,
////        planner2.addRule(JoinPushTransitivePredicatesRule.INSTANCE,
////                AggregateUnionTransposeRule.INSTANCE,
////                AggregateUnionAggregateRule.AGG_ON_FIRST_INPUT,
////                AggregateUnionAggregateRule.AGG_ON_SECOND_INPUT,
////                AggregateUnionAggregateRule.INSTANCE,
////                AggregateProjectMergeRule.INSTANCE,
////                AggregateProjectPullUpConstantsRule.INSTANCE,
//                PushDownLogicTable.INSTANCE_FOR_PushDownFilterLogicTable
////                AggregateValuesRule.INSTANCE
//        ).forEach(i -> hepProgramBuilder.addRuleInstance(i));
////        hepProgramBuilder.addRuleInstance(PushDownLogicTable.INSTANCE_FOR_PushDownLogicTable);

//             RelShuttleImpl relShuttle = new RelShuttleImpl() {
//            @Override
//            public RelNode visit(TableScan scan) {
//                MycatLogicTable unwrap = scan.getTable().unwrap(MycatLogicTable.class);
//                if (unwrap != null) {
//                    return PushDownLogicTable.toPhyTable(relBuilder, scan);
//                }
//                return super.visit(scan);
//            }
//        };
//        bestExp = relShuttle.visit(bestExp);
//        bestExp = pushDownBySQL(relBuilder, bestExp);
//        collectMycatTransientSQLTable(bestExp);
        System.out.println(convert);
        ResultSet resultSet = RelRunners.run(convert).executeQuery();
        while (resultSet.next()){

        };
        CalciteDataContext dataContext = new CalciteDataContext(config.getDefaultSchema(), planner);
        System.out.println("---------------------------------------------");
        RelNode phy = toPhysical(convert, relOptPlanner -> RelOptUtil.registerDefaultRules(relOptPlanner, true, true));
        //修复变成物理表达式后无法运行,所以重新编译成逻辑表达式
        RelNode fixLogic = new ToLogicalConverter(RelBuilder.proto( RelFactories.LOGICAL_BUILDER).create(convert.getCluster(),null)) {
            @Override
            public RelNode visit(RelNode relNode) {
                if (relNode instanceof MycatTransientSQLTable) {
                    return relNode;
                }
                return super.visit(relNode);
            }
        }.visit(phy);
        final Map<String, Object> map = new HashMap<>(1);
        ArrayBindable bindable1 = Interpreters.bindable(fixLogic);
        EnumeratorRowIterator enumeratorRowIterator = new EnumeratorRowIterator(CalciteConvertors.getMycatRowMetaData(convert.getRowType()), bindable1.bind(dataContext).enumerator());

//        RelShuttleImpl relHomogeneousShuttle = new RelShuttleImpl() {
//            @Override
//            public RelNode visit(LogicalAggregate aggregate) {
//                List<AggregateCall> aggCallList = aggregate.getAggCallList();
//                List<Integer> a = new ArrayList<>();
//                for (AggregateCall aggregateCall : aggCallList) {
//                    if(aggregateCall.getAggregation().getKind() == SqlKind.AVG){
//                        a.add(aggregateCall.getArgList().get(0);
//                    }
//                }
//
//                aggregate.copy(aggregate.getTraitSet(),)
//                return super.visit(aggregate);
//            }
//        };
//
     return new TextResultSetResponse(enumeratorRowIterator);
    }

    private RelNode pushDownBySQL(MycatRelBuilder relBuilder, RelNode bestExp) {
        //子节点运算的节点是同一个目标的,就把它们的父节点标记为可以变成SQL
        IdentityHashMap<RelNode, Boolean> cache = new IdentityHashMap<>();
        IdentityHashMap<RelNode, List<String>> margeList = new IdentityHashMap<>();
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
                    cache.put(other, distinct.isEmpty() || distinct.size() == 1);
                } else {
                    MycatPhysicalTable mycatPhysicalTable = other.getTable().unwrap(MycatPhysicalTable.class);
                    if (mycatPhysicalTable != null) {
                        margeList.put(other, Collections.singletonList(mycatPhysicalTable.getTargetName()));
                    } else {
                        margeList.put(other, Collections.emptyList());
                    }
                    cache.put(other, Boolean.TRUE);
                }
                return res;
            }
        };
        relHomogeneousShuttle.visit(bestExp);


        bestExp = new RelShuttleImpl() {
            @Override
            protected RelNode visitChild(RelNode parent, int i, RelNode child) {
                if (parent instanceof Aggregate && child instanceof Union) {
                    Aggregate aggregate = (Aggregate) parent;
                    List<AggregateCall> aggCallList = aggregate.getAggCallList();
                    boolean allMatch = aggCallList.stream().allMatch(aggregateCall -> SUPPORTED_AGGREGATES.getOrDefault(aggregateCall.getAggregation().getKind(), false));
                    if (allMatch) {
                        List<RelNode> inputs = child.getInputs();
                        List<RelNode> resList = new ArrayList<>(inputs.size());
                        for (RelNode input : inputs) {
                            RelNode res;
                            if (cache.get(input)) {
                                res = LogicalAggregate.create(input, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
                                cache.put(res, Boolean.TRUE);
                                margeList.put(res, margeList.get(input));
                            } else {
                                res = input;
                            }
                            resList.add(res);
                        }
                        LogicalUnion logicalUnion = LogicalUnion.create(resList, ((Union) child).all);
                        return LogicalAggregate.create(logicalUnion, aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList());
                    }
                }
                return super.visitChild(parent, i, child);
            }
        }.visit(bestExp);


        //从根节点开始把变成SQL下推
        RelHomogeneousShuttle relHomogeneousShuttle1 = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                if (cache.get(other) == Boolean.TRUE) {
                    List<String> strings = margeList.get(other);
                    String targetName = strings.get(0);
                    return relBuilder.makeTransientSQLScan( targetName, other);
                }
                return super.visit(other);
            }
        };
        bestExp = bestExp.accept(relHomogeneousShuttle1);
        return bestExp;
    }

    private void collectMycatTransientSQLTable(RelNode bestExp) {
        List<MycatTransientSQLTable> list = new ArrayList<>();
        bestExp.accept(new RelShuttleImpl() {

            @Override
            public RelNode visit(TableScan scan) {
                MycatTransientSQLTable unwrap = scan.getTable().unwrap(MycatTransientSQLTable.class);
                if (unwrap != null) {
                    list.add(unwrap);
                } else {
                    throw new IllegalArgumentException();
                }
                return super.visit(scan);
            }
        });
    }

    public CalciteConnection getRawConnection() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL;fun=mysql;conformance=MYSQL_5");
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            return calciteConnection;
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new RuntimeException(e);
        }
    }

    private static RelNode toPhysical(RelNode rel, Consumer<RelOptPlanner> setting) {
        final RelOptPlanner planner = rel.getCluster().getPlanner();
        planner.clear();
        setting.accept(planner);
        final Program program = Programs.of(RuleSets.ofList(planner.getRules()));
        return program.run(planner, rel, rel.getTraitSet().replace(EnumerableConvention.INSTANCE),
                ImmutableList.of(), ImmutableList.of());
    }
}