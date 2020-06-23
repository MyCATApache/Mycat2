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
import org.apache.calcite.avatica.util.Unsafe;
import org.apache.calcite.interpreter.Context;
import org.apache.calcite.interpreter.JaninoRexCompiler;
import org.apache.calcite.interpreter.Scalar;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.objenesis.instantiator.util.UnsafeUtils;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
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
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement("select count(1) from travelrecord where id = 1 limit 2");
        List<Object> parameters = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        MySqlExportParameterVisitor mySqlExportParameterVisitor = new MySqlExportParameterVisitor(parameters, sb, true);
        sqlStatement.accept(mySqlExportParameterVisitor);
        Plan plan;

        RexBuilder rexBuilder = MycatCalciteSupport.INSTANCE.RexBuilder;
        RelDataType javaType = MycatCalciteSupport.INSTANCE.TypeFactory.createJavaType(Integer.class);
        final RelDataTypeFactory typeFactory = MycatCalciteSupport.INSTANCE.TypeFactory;
        final RelDataTypeFactory.Builder builder = typeFactory.builder();

        builder.add("1", SqlTypeName.INTEGER);
        RelDataType build = builder.build();
        JaninoRexCompiler janinoRexCompiler = new JaninoRexCompiler(rexBuilder);
        RexNode rexNode = rexBuilder.makeCall(build, SqlStdOperatorTable.PLUS,
                Arrays.asList(
                        rexBuilder.makeExactLiteral(BigDecimal.valueOf(1)),
                        rexBuilder.makeInputRef(build, 0)));

        Scalar compile = janinoRexCompiler.compile(Arrays.asList(rexNode)


                , build
        );

        Context context =  (Context)UnsafeUtils.getUnsafe().allocateInstance( Context.class);

        Object[] objects = new Object[]{3};
        context.values = objects;
        Object[] res = new Object[2];
        compile.execute(context, res);
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
        MycatRel phyPlan = (MycatRel) optimizeWithCBO(logPlan, cluster);
        ExplainWriter explainWriter = new ExplainWriter();
        phyPlan.explain(explainWriter);
        StringBuilder text = explainWriter.getText();
        System.out.println(text);
        Executor executor = phyPlan.implement(new ExecutorImplementorImpl());
        return null;
    }

    private static RelNode optimizeWithCBO(RelNode logPlan, RelOptCluster cluster) {
        RelOptPlanner planner = cluster.getPlanner();
        MycatConvention.INSTANCE.register(planner);
        logPlan = planner.changeTraits(logPlan, cluster.traitSetOf(MycatConvention.INSTANCE));
        planner.setRoot(logPlan);
        return planner.findBestExp();
    }

    private static RelNode optimizeWithRBO(RelNode logPlan) {
        ImmutableList<RelOptRule> rules = ImmutableList.of(
                BottomViewRules.ProjectView.INSTANCE,
                BottomViewRules.FilterView.INSTACNE,
                BottomViewRules.ProjectView.INSTANCE,
                BottomViewRules.AggregateView.INSTACNE,
                BottomViewRules.ProjectView.INSTANCE,
                BottomViewRules.JoinView.INSTANCE,
                BottomViewRules.ProjectView.INSTANCE,
                BottomViewRules.CorrelateView.INSTANCE,
                BottomViewRules.ProjectView.INSTANCE,
                BottomViewRules.SortView.INSTACNE,
                BottomViewRules.ProjectView.INSTANCE
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
}