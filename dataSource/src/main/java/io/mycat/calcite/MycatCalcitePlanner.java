package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.logic.MycatLogicTable;
import io.mycat.calcite.logic.MycatPhysicalTable;
import io.mycat.calcite.relBuilder.MycatTransientSQLTable;
import io.mycat.calcite.relBuilder.MycatTransientSQLTableScan;
import io.mycat.datasource.jdbc.resultset.JdbcRowBaseIteratorImpl;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.interpreter.Bindables;
import org.apache.calcite.interpreter.Interpreters;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.PlannerImpl;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.ToLogicalConverter;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.*;
import org.apache.calcite.util.Pair;

import java.io.Reader;
import java.sql.PreparedStatement;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.apache.calcite.plan.RelOptRule.none;
import static org.apache.calcite.plan.RelOptRule.operand;

public class MycatCalcitePlanner implements Planner, RelOptTable.ViewExpander {
    SchemaPlus rootSchema;
    PlannerImpl planner;

    public MycatCalcitePlanner(SchemaPlus rootSchema, String defaultSchemaName) {
        this.rootSchema = rootSchema;
        this.planner = new PlannerImpl(MycatCalciteContext.INSTANCE.createFrameworkConfig(defaultSchemaName));
        ;
    }

    public MycatRelBuilder createRelBuilder(RelOptCluster cluster) {
        return (MycatRelBuilder) MycatCalciteContext.INSTANCE.relBuilderFactory.create(cluster, null);
    }

    List<RelOptRule> relOptRules = Arrays.asList(
            FilterTableScanRule.INSTANCE,
            ProjectTableScanRule.INSTANCE,
            FilterSetOpTransposeRule.INSTANCE,
            ProjectRemoveRule.INSTANCE,
            JoinUnionTransposeRule.LEFT_UNION,
            JoinUnionTransposeRule.RIGHT_UNION,
            JoinExtractFilterRule.INSTANCE,
            JoinPushTransitivePredicatesRule.INSTANCE,
            AggregateUnionTransposeRule.INSTANCE,
            AggregateUnionAggregateRule.AGG_ON_FIRST_INPUT,
            AggregateUnionAggregateRule.AGG_ON_SECOND_INPUT,
            AggregateUnionAggregateRule.INSTANCE,
            AggregateProjectMergeRule.INSTANCE,//下推聚合
            AggregateProjectPullUpConstantsRule.INSTANCE,
            PushDownLogicTable.INSTANCE_FOR_PushDownFilterLogicTable
    );

    public RelNode eliminateLogicTable(RelNode bestExp) {
        RelOptCluster cluster = bestExp.getCluster();
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();

        Arrays.asList(
                FilterProjectTransposeRule.INSTANCE,
                Bindables.BINDABLE_TABLE_SCAN_RULE,
                FilterTableScanRule.INSTANCE,
                ProjectTableScanRule.INSTANCE,
                FilterSetOpTransposeRule.INSTANCE,
                ProjectRemoveRule.INSTANCE,
//        planner2.addRule(JoinUnionTransposeRule.LEFT_UNION,
//        planner2.addRule(JoinUnionTransposeRule.RIGHT_UNION.
                JoinExtractFilterRule.INSTANCE,
//        planner2.addRule(JoinPushTransitivePredicatesRule.INSTANCE,
//                AggregateUnionTransposeRule.INSTANCE,
//                AggregateUnionAggregateRule.AGG_ON_FIRST_INPUT,
//                AggregateUnionAggregateRule.AGG_ON_SECOND_INPUT,
                AggregateUnionAggregateRule.INSTANCE,
                AggregateProjectMergeRule.INSTANCE,
                AggregateProjectPullUpConstantsRule.INSTANCE,
                PushDownLogicTable.INSTANCE_FOR_PushDownFilterLogicTable,
                AggregateValuesRule.INSTANCE
        ).forEach(i -> hepProgramBuilder.addRuleInstance(i));
//        hepProgramBuilder.addRuleInstance(PushDownLogicTable.INSTANCE_FOR_PushDownLogicTable);

        final HepPlanner planner2 = new HepPlanner(hepProgramBuilder.build());
        planner2.setRoot(bestExp);
        bestExp = planner2.findBestExp();

        RelShuttleImpl relShuttle = new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                MycatLogicTable unwrap = scan.getTable().unwrap(MycatLogicTable.class);
                if (unwrap != null) {
                    return PushDownLogicTable.toPhyTable(createRelBuilder(cluster), scan);
                }
                return super.visit(scan);
            }
        };
        bestExp = bestExp.accept(relShuttle);
        return relShuttle.visit(bestExp);
    }

    public RelNode pushDownBySQL(RelNode bestExp) {
        return pushDownBySQL(createRelBuilder(bestExp.getCluster()), bestExp);
    }

    public RelNode pushDownBySQL(MycatRelBuilder relBuilder, RelNode bestExp) {
        HepProgram build = new HepProgramBuilder().build();

        RelOptPlanner planner = new HepPlanner(build);
        RelOptUtil.registerDefaultRules(planner, true, true);

        planner.setRoot(bestExp);
        bestExp = planner.findBestExp();

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
        bestExp = relHomogeneousShuttle.visit(bestExp);

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
                    return relBuilder.makeTransientSQLScan(targetName, other);
                }
                return super.visit(other);
            }
        };
        bestExp = relHomogeneousShuttle1.visit(bestExp);


        return bestExp;
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
        return RelOptCluster.create(planner, MycatCalciteContext.INSTANCE.RexBuilder);
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

    @SneakyThrows
    public Supplier<RowBaseIterator> run(RelNode o) {
        try {
            MycatRowMetaData mycatRowMetaData = CalciteConvertors.getMycatRowMetaData(o.getRowType());
            RelNode phy = toPhysical(o, relOptPlanner -> {
                RelOptUtil.registerDefaultRules(relOptPlanner, false, false);
            });
            //修复变成物理表达式后无法运行,所以重新编译成逻辑表达式
            RelNode fixLogic = new ToLogicalConverter(createRelBuilder(o.getCluster())) {
                @Override
                public RelNode visit(RelNode relNode) {
                    if (relNode instanceof MycatTransientSQLTable) {
                        return relNode;
                    }
                    return super.visit(relNode);
                }
            }.visit(phy);

            System.out.println(RelOptUtil.toString(fixLogic));

            ArrayBindable bindable1 = Interpreters.bindable(fixLogic);
            Enumerator<Object[]> enumerator = bindable1.bind(new MycatCalciteDataContext(rootSchema, null)).enumerator();
            return () -> new EnumeratorRowIterator(mycatRowMetaData, enumerator);
        } catch (java.lang.AssertionError | Exception e) {//实在运行不了使用原来的方法运行
            System.err.println(e);
            PreparedStatement run = RelRunners.run(o);
            return new Supplier<RowBaseIterator>() {
                @SneakyThrows
                @Override
                public RowBaseIterator get() {
                    return new JdbcRowBaseIteratorImpl(run, run.executeQuery());
                }
            };
        }
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
}