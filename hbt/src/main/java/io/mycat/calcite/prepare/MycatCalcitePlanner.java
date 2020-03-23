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
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatRelBuilder;
import io.mycat.calcite.rules.PushDownLogicTable;
import io.mycat.calcite.table.*;
import io.mycat.upondb.MycatDBContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Pair;

import java.io.Reader;
import java.util.*;
import java.util.function.Consumer;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;

/**
 * @author Junwen Chen
 **/
public class MycatCalcitePlanner implements Planner, RelOptTable.ViewExpander {

    private final SchemaPlus rootSchema;
    private final PlannerImpl planner;
    CalciteCatalogReader reader = null;
    private MycatCalciteDataContext dataContext;

    public MycatCalcitePlanner(MycatCalciteDataContext dataContext) {
        this.dataContext = dataContext;
        this.rootSchema = dataContext.getRootSchema();
        this.planner = new PlannerImpl(dataContext);
    }

    public CalciteCatalogReader createCalciteCatalogReader() {
        if (reader == null) {
            List<String> path = Collections.emptyList();
            SchemaPlus defaultSchema = dataContext.getDefaultSchema();
            if (defaultSchema!=null){
                String name = defaultSchema.getName();
                path = Collections.singletonList(name);
            }
            CalciteCatalogReader calciteCatalogReader = new CalciteCatalogReader(
                    CalciteSchema.from(rootSchema),
                    path,
                    MycatCalciteSupport.INSTANCE.TypeFactory, MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());
            reader = calciteCatalogReader;
        }
        return reader;
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
        SqlConformance conformance = MycatCalciteSupport.INSTANCE.calciteConnectionConfig.conformance();
        return (SqlValidatorImpl) SqlValidatorUtil.newValidator(opTab, catalogReader, typeFactory, conformance);
    }

    public SqlToRelConverter createSqlToRelConverter() {
        return new SqlToRelConverter(planner, getSqlValidator(),
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

    public RelNode eliminateLogicTable(final RelNode bestExp) {
        RelOptCluster cluster = bestExp.getCluster();
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        PushDownLogicTable pushDownLogicTable = new PushDownLogicTable();
        Arrays.asList(
                FilterProjectTransposeRule.INSTANCE,
                Bindables.BINDABLE_TABLE_SCAN_RULE,
                FilterTableScanRule.INSTANCE,
                ProjectTableScanRule.INSTANCE,
                FilterSetOpTransposeRule.INSTANCE,
                JoinUnionTransposeRule.LEFT_UNION,
                JoinUnionTransposeRule.RIGHT_UNION,
                JoinExtractFilterRule.INSTANCE,
                JoinPushTransitivePredicatesRule.INSTANCE,
                AggregateUnionTransposeRule.INSTANCE,
                AggregateUnionAggregateRule.AGG_ON_FIRST_INPUT,
                AggregateUnionAggregateRule.AGG_ON_SECOND_INPUT,
                AggregateUnionAggregateRule.INSTANCE,
                AggregateProjectMergeRule.INSTANCE,
                AggregateProjectPullUpConstantsRule.INSTANCE,
                pushDownLogicTable,
//                PushDownLogicTable.INSTANCE_FOR_PushDownFilterLogicTable,
                AggregateValuesRule.INSTANCE
        ).forEach(i -> hepProgramBuilder.addRuleInstance(i));
//        hepProgramBuilder.addRuleInstance(PushDownLogicTable.INSTANCE_FOR_PushDownLogicTable);

        final HepPlanner planner2 = new HepPlanner(hepProgramBuilder.build());
        planner2.setRoot(bestExp);
        final RelNode bestExp1 = planner2.findBestExp();

        RelShuttleImpl relShuttle = new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatLogicTable unwrap = scan.getTable().unwrap(MycatLogicTable.class);
                if (unwrap != null) {
                    return pushDownLogicTable.toPhyTable(createRelBuilder(cluster), scan);
                }
                return super.visit(scan);
            }
        };
        final RelNode bestExp2 = bestExp1.accept(relShuttle);
        return relShuttle.visit(bestExp2);
    }

    public RelNode pushDownBySQL(RelNode bestExp,boolean forUpdate) {
        return pushDownBySQL(createRelBuilder(bestExp.getCluster()), bestExp,forUpdate);
    }

    public RelNode pushDownBySQL(MycatRelBuilder relBuilder, final RelNode bestExp0,boolean forUpdate) {
        HepProgram build = new HepProgramBuilder().build();

        RelOptPlanner planner = new HepPlanner(build);
        RelOptUtil.registerDefaultRules(planner, true, true);

        planner.setRoot(bestExp0);
        final RelNode bestExp1 = planner.findBestExp();

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
        final RelNode bestExp2 = relHomogeneousShuttle.visit(bestExp1);

        final RelNode bestExp3 = new RelShuttleImpl() {
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
        }.visit(bestExp2);


        //从根节点开始把变成SQL下推
        RelHomogeneousShuttle relHomogeneousShuttle1 = new RelHomogeneousShuttle() {
            @Override
            public RelNode visit(RelNode other) {
                if (cache.get(other) == Boolean.TRUE) {
                    List<String> strings = margeList.get(other);
                    String targetName = strings.get(0);
                    return relBuilder.makeTransientSQLScan(targetName, other,forUpdate);
                }
                return super.visit(other);
            }
        };
        final RelNode bestExp4 = relHomogeneousShuttle1.visit(bestExp3);


        return bestExp4;
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

    private static final EnumMap<SqlKind, Boolean> SUPPORTED_AGGREGATES = new EnumMap<>(SqlKind.class);

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


    @Override
    public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        return planner.expandView(rowType, queryString, schemaPath, viewPath);
    }

    @Override
    public SqlNode parse(Reader source) throws SqlParseException {
        return planner.parse(source);
    }

    @Override
    public SqlNode validate(SqlNode sqlNode) throws ValidationException {
        return planner.validate(sqlNode);
    }

    @Override
    public Pair<SqlNode, RelDataType> validateAndGetType(SqlNode sqlNode) throws ValidationException {
        return planner.validateAndGetType(sqlNode);
    }

    @Override
    public RelRoot rel(SqlNode sql) throws RelConversionException {
        return planner.rel(sql);
    }

    @Override
    public RelNode convert(SqlNode sql) throws RelConversionException {
        return planner.convert(sql);
    }

    @Override
    public RelDataTypeFactory getTypeFactory() {
        return planner.getTypeFactory();
    }

    @Override
    public RelNode transform(int ruleSetIndex, RelTraitSet requiredOutputTraits, RelNode rel) throws RelConversionException {
        return planner.transform(ruleSetIndex, requiredOutputTraits, rel);
    }

    @Override
    public void reset() {
        planner.reset();
    }

    @Override
    public void close() {
        planner.close();
    }

    @Override
    public RelTraitSet getEmptyTraitSet() {
        return planner.getEmptyTraitSet();
    }

    public void convert(String sql) {

    }

    public DatasourceInfo preComputeSeq(RelNode relNode) {
        MycatDBContext uponDBContext = dataContext.getUponDBContext();
        Map<String, List<PreComputationSQLTable>> map = new HashMap<>();
        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                PreComputationSQLTable unwrap = scan.getTable().unwrap(PreComputationSQLTable.class);
                if (unwrap != null) {
                    List<PreComputationSQLTable> preComputationSQLTables = map.computeIfAbsent(uponDBContext.resolveFinalTargetName(unwrap.getTargetName()), s -> new ArrayList<>(1));
                    preComputationSQLTables.add(unwrap);
                }
                return super.visit(scan);
            }
        });
        List<PreComputationSQLTable> preSeq = new ArrayList<>();
        for (Map.Entry<String, List<PreComputationSQLTable>> stringListEntry : map.entrySet()) {
            List<PreComputationSQLTable> value = stringListEntry.getValue();
            int size = value.size() - 1;
            for (int i = 0; i < size; i++) {
                preSeq.add(value.get(i));
            }
        }
        return new DatasourceInfo(preSeq,map);
    }


    public RelNode convertToMycatRel(RelNode relNode) {
        return relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatTransientSQLTable unwrap = scan.getTable().unwrap(MycatTransientSQLTable.class);
                if (unwrap != null) {
                    return unwrap.toRel(ViewExpanders.toRelContext(planner, MycatCalcitePlanner.this.newCluster()), scan.getTable());
                }
                return super.visit(scan);
            }
        });
    }
}