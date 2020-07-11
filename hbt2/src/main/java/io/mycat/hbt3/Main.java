package io.mycat.hbt3;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.RootHelper;
import io.mycat.TableHandler;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.hbt4.*;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import io.mycat.mpp.Row;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;

import java.util.*;

public class Main {
    public static void main(String[] args) throws Exception {
        String defaultSchema = "db1";
        String s = "(select count(1) from travelrecord) union all (select count(1) from travelrecord where id = 1 limit 2)";
        SqlParser sqlParser = SqlParser.create(s, MycatCalciteSupport.INSTANCE.SQL_PARSER_CONFIG);

        SqlNode sqlNode = sqlParser.parseQuery();

        SchemaPlus plus = CalciteSchema.createRootSchema(true).plus();
        MetadataManager.INSTANCE.load(RootHelper.INSTANCE.bootConfig(Main.class).currentConfig());
        for (Map.Entry<String, SchemaHandler> entry : MetadataManager.INSTANCE.getSchemaMap().entrySet()) {
            Collection<TableHandler> tables = entry.getValue().logicTables().values();
            String schemaName = entry.getKey();
            MycatSchema schema = new MycatSchema(tables);
            plus.add(schemaName,schema);
            Map<String, MycatTable> mycatTables = schema.getMycatTables();
        }

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema
                .from(plus),
                ImmutableList.of(defaultSchema),
                MycatCalciteSupport.INSTANCE.TypeFactory,
                MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());

        SqlValidator validator = SqlValidatorUtil.newValidator(MycatCalciteSupport.INSTANCE.config.getOperatorTable(),
                catalogReader, MycatCalciteSupport.INSTANCE.TypeFactory, MycatCalciteSupport.INSTANCE.getValidatorConfig());

        SqlNode validated = validator.validate(sqlNode);
        RelOptCluster cluster = newCluster();

        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                MycatCalciteSupport.INSTANCE.config.getConvertletTable(),
                MycatCalciteSupport.INSTANCE.sqlToRelConverterConfig);

        RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);
        final RelRoot root2 =
                root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));
        RelBuilder relBuilder = MycatCalciteSupport.INSTANCE.relBuilderFactory.create(cluster, null);
        RelRoot finalRoot = root2.withRel(
                RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
        RelNode logPlan = finalRoot.rel;
        logPlan = optimizeWithRBO(logPlan);
        RBO rbo = new RBO();
        RelNode relNode = logPlan.accept(rbo);
        if (relNode instanceof View) {
            String sql = ((View) relNode).getSql();
            System.out.println(sql);
        }
        MycatRel relNode1 = (MycatRel) optimizeWithCBO(relNode, cluster);
        Map<String, Object> context = new HashMap<>();
        context.put("?0", 0);
        context.put("?1", 1);
        try (DatasourceFactoryImpl datasourceFactory = new DatasourceFactoryImpl()) {
            RelDataType rowType = relNode1.getRowType();
            CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(rowType.getFieldList());
            ExecutorImplementorImpl executorImplementor = new ExecutorImplementorImpl(context, datasourceFactory);
            Executor implement = relNode1.implement(executorImplementor);
            implement.open();
            Iterator<Row> iterator = implement.iterator();
            while (iterator.hasNext()) {
                Row next = iterator.next();
                System.out.println(next);
            }
        }

    }


    public static boolean is(Object a, Class k) {
        return k.isInstance(a);
    }

    //    private static RelNode hep(RelNode parent, RelNode cur) {
//        List<RelNode> inputs = cur.getInputs();
//        if (inputs != null) {
//            for (RelNode input : inputs) {
//                RelNode res = hep(cur, input);
//                if (res != input) {
//                    return
//                }
//            }
//        }
//
//
//        RelNode input;
//        if (logPlan instanceof TableScan) {
//            return BottomView.create((LogicalTableScan) logPlan);
//        }
//        if (logPlan instanceof LogicalFilter) {
//            LogicalFilter filter = (LogicalFilter) logPlan;
//            input = filter.getInput();
//            if (input instanceof LogicalTableScan) {
//                return BottomView.create(filter, (LogicalTableScan) input);
//            }
//        }
//        if (logPlan instanceof LogicalValues) {
//
//        }
//        if (logPlan instanceof LogicalProject) {
//
//        }
//        if (logPlan instanceof LogicalJoin) {
//
//        }
//        if (logPlan instanceof LogicalCorrelate) {
//
//        }
//        if (logPlan instanceof LogicalCorrelate) {
//
//        }
//        if (logPlan instanceof LogicalUnion) {
//
//        }
//        if (logPlan instanceof LogicalAggregate) {
//
//        }
//        if (logPlan instanceof LogicalSort) {
//
//        }
//        return null;
//    }
    private static RelNode optimizeWithCBO(RelNode logPlan, RelOptCluster cluster) {
        RelOptPlanner planner = cluster.getPlanner();
        planner.clear();
        MycatConvention.INSTANCE.register(planner);
        logPlan = planner.changeTraits(logPlan, cluster.traitSetOf(MycatConvention.INSTANCE));
        planner.setRoot(logPlan);
        return planner.findBestExp();
    }

    static final ImmutableSet<RelOptRule> FILTER = ImmutableSet.of(
            JoinPushTransitivePredicatesRule.INSTANCE,
            JoinPushTransitivePredicatesRule.INSTANCE,
            JoinExtractFilterRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            FilterJoinRule.DUMB_FILTER_ON_JOIN,
            FilterJoinRule.JOIN,
            FilterCorrelateRule.INSTANCE,
            FilterAggregateTransposeRule.INSTANCE,
            FilterMultiJoinMergeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            FilterRemoveIsNotDistinctFromRule.INSTANCE,
            FilterSetOpTransposeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE,
            SemiJoinFilterTransposeRule.INSTANCE,
            ProjectFilterTransposeRule.INSTANCE,
            ReduceExpressionsRule.FILTER_INSTANCE,
            ReduceExpressionsRule.JOIN_INSTANCE,
            ReduceExpressionsRule.PROJECT_INSTANCE,
            FilterMergeRule.INSTANCE
    );

    private static RelNode optimizeWithRBO(RelNode logPlan) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchLimit(1024);
        builder.addRuleCollection(FILTER);
        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(logPlan);
        return planner.findBestExp();
    }

    private static RelOptCluster newCluster() {
        RelOptPlanner planner = new VolcanoPlanner();
        ImmutableList<RelTraitDef> TRAITS = ImmutableList
                .of(ConventionTraitDef.INSTANCE
//                        , RelCollationTraitDef.INSTANCE
                );
        for (RelTraitDef i : TRAITS) {
            planner.addRelTraitDef(i);
        }
        return RelOptCluster.create(planner, MycatCalciteSupport.INSTANCE.RexBuilder);
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = new RelOptTable.ViewExpander() {
        @Override
        public RelRoot expandView(final RelDataType rowType, final String queryString,
                                  final List<String> schemaPath,
                                  final List<String> viewPath) {
            return null;
        }
    };
}