package io.mycat.optimizer;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;
import com.google.common.collect.ImmutableList;
import io.mycat.RootHelper;
import io.mycat.calcite.MycatCalciteMySqlNodeVisitor;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Main {
    private static final ImmutableList<RelTraitDef> TRAITS = ImmutableList
            .of(
                    ConventionTraitDef.INSTANCE

//                    ,
//            RelCollationTraitDef.INSTANCE
            );

    public static void main(String[] args) throws Exception {
        String defaultSchema = "db1";
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement("select * from travelrecord t2");
        List<Object> parameters = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        MySqlExportParameterVisitor mySqlExportParameterVisitor = new MySqlExportParameterVisitor(parameters, sb, true);
        sqlStatement.accept(mySqlExportParameterVisitor);
        Plan plan;


        if (parameters.isEmpty()) {
            String parameterSql = sb.toString();
            plan = PlanCache.INSTANCE.get(parameterSql, () -> compile(defaultSchema, SQLUtils.parseSingleMysqlStatement(parameterSql)));
        } else {
            plan = compile(defaultSchema, sqlStatement);
        }

    }

    private static Plan compile(String defaultSchema, SQLStatement sqlStatement) throws Exception {
        MycatCalciteMySqlNodeVisitor mycatCalciteMySqlNodeVisitor = new MycatCalciteMySqlNodeVisitor();
        sqlStatement.accept(mycatCalciteMySqlNodeVisitor);
        SqlNode sqlNode = mycatCalciteMySqlNodeVisitor.getSqlNode();

        SchemaPlus plus = CalciteSchema.createRootSchema(true).plus();
        MetadataManager.INSTANCE.load(RootHelper.INSTANCE.bootConfig(Main.class).currentConfig());
        for (Map.Entry<String, SchemaHandler> entry : MetadataManager.INSTANCE.getSchemaMap().entrySet()) {
            plus.add(entry.getKey(), new MycatSchema(entry.getValue().logicTables().values()));
        }
        CalciteSchema calciteSchema = CalciteSchema
                .from(plus);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(calciteSchema,
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
        RelNode logPlan = root2.withRel(
                RelDecorrelator.decorrelateQuery(root.rel, relBuilder)).project();
        String s = MycatCalciteSupport.INSTANCE.convertToSql(logPlan, MysqlSqlDialect.DEFAULT, false);

        RelBuilder push = relBuilder.push(logPlan);

        RexInputRef field = push.field(0);
        RelBuilder.AggCall sum = relBuilder.sum(field);
        ;
        relBuilder.push(logPlan.copy(logPlan.getTraitSet(), logPlan.getInputs()));
        relBuilder.join(JoinRelType.INNER);//                .union(true)
//                .limit(1,2)
        RelNode project = relBuilder
//                .project(relBuilder.call(PLUS,field,relBuilder.literal(1L)))
//                .filter(relBuilder.equals(field,relBuilder.literal(false)))
//                .aggregate( relBuilder.groupKey(0),sum)
                .build();

//        RelNode correlate = test(relBuilder);
        // String s2 = MycatCalciteSupport.INSTANCE.convertToSql((RelNode) correlate, MysqlSqlDialect.DEFAULT, false);

//        correlate = optimizeWithRBO(correlate);
//        EnumerableRel phyPlan = (EnumerableRel) optimizeWithCBO(correlate, cluster);
        ExplainWriter explainWriter = new ExplainWriter();
//        phyPlan.explain(explainWriter);
//        StringBuilder text = explainWriter.getText();
//        System.out.println(text);
//        Executor executor = phyPlan.implement(new ExecutorImplementorImpl());
//        executor.open();
//        Row row = null;
//        while ((row = executor.next())!=null){
//            System.out.println(row);
//        }
   //     Bindable bindable = EnumerableInterpretable.toBindable(Collections.EMPTY_MAP, null, phyPlan, EnumerableRel.Prefer.ARRAY);
      //  Enumerable bind = bindable.bind(new SchemaOnlyDataContext(calciteSchema));
        //  String s1 = MycatCalciteSupport.INSTANCE.convertToSql((RelNode) phyPlan, MysqlSqlDialect.DEFAULT, false);

//        for (Object o : bind) {
//            System.out.println(o);
//        }

        return null;
    }

    private static void test(RelBuilder relBuilder) {
        RelNode left = relBuilder
                .values(new String[]{"f", "f2"}, "1", "2").build();

        CorrelationId correlationId = new CorrelationId(0);
        RexNode rexCorrel =
                relBuilder.getRexBuilder().makeCorrel(
                        left.getRowType(),
                        correlationId);

        RelNode right = relBuilder
                .values(new String[]{"f3", "f4"}, "1", "2")
                .project(relBuilder.field(0),
                        relBuilder.getRexBuilder()
                                .makeFieldAccess(rexCorrel, 0))
                .build();
        RelNode correlate = new LogicalCorrelate(left.getCluster(),
                left.getTraitSet(), left, right, correlationId,
                ImmutableBitSet.of(0), JoinRelType.SEMI);

        class RelToSqlConverter2 extends RelToSqlConverter {
            public RelToSqlConverter2() {
                super(MysqlSqlDialect.DEFAULT);
            }

            @Override
            public Result dispatch(RelNode e) {
                return super.dispatch(e);
            }
        }
        ;
        correlate = RelDecorrelator.decorrelateQuery(correlate, relBuilder);
        RelToSqlConverter2 relToSqlConverter2 = new RelToSqlConverter2();
        SqlString sqlString = relToSqlConverter2.dispatch(correlate).asStatement().toSqlString(MysqlSqlDialect.DEFAULT);
    }

    private static RelNode optimizeWithCBO(RelNode logPlan, RelOptCluster cluster) {
        RelOptPlanner planner = cluster.getPlanner();

        EnumerableRules.rules().forEach(i -> planner.addRule(i));
        RelOptRules.CALC_RULES.forEach(i -> planner.addRule(i));
        planner.removeRule(EnumerableRules.ENUMERABLE_PROJECT_RULE);
        planner.removeRule(EnumerableRules.ENUMERABLE_FILTER_RULE);
        logPlan = planner.changeTraits(logPlan, cluster.traitSetOf(EnumerableConvention.INSTANCE));
        planner.setRoot(logPlan);
        return planner.findBestExp();
    }

    private static RelNode optimizeWithRBO(RelNode logPlan) {
        ImmutableList<RelOptRule> rules = ImmutableList.of(
//                BottomViewRules.ProjectView.INSTANCE,
//                BottomViewRules.FilterView.INSTACNE,
//                BottomViewRules.ProjectView.INSTANCE,
//                BottomViewRules.AggregateView.INSTACNE,
//                BottomViewRules.ProjectView.INSTANCE,
//                BottomViewRules.JoinView.INSTANCE,
//                BottomViewRules.ProjectView.INSTANCE,
//                BottomViewRules.CorrelateView.INSTANCE,
//                BottomViewRules.ProjectView.INSTANCE,
//                BottomViewRules.SortView.INSTACNE,
//                BottomViewRules.ProjectView.INSTANCE
        );
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addRuleCollection(rules);
        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(logPlan);
        return planner.findBestExp();
    }

    private static RelOptCluster newCluster() {
        RelOptPlanner planner = new VolcanoPlanner();
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

    /**
     * A simple data context only with schema information.
     */
    private static final class SchemaOnlyDataContext implements DataContext {
        private final SchemaPlus schema;

        SchemaOnlyDataContext(CalciteSchema calciteSchema) {
            this.schema = calciteSchema.plus();
        }

        @Override
        public SchemaPlus getRootSchema() {
            return schema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return new JavaTypeFactoryImpl();
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(final String name) {
            return null;
        }
    }
}