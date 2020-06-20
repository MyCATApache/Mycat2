package io.mycat.optimizer;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.google.common.collect.ImmutableList;
import io.mycat.RootHelper;
import io.mycat.calcite.MycatCalciteMySqlNodeVisitor;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;

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
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement("select count(1) from travelrecord where id = 1 limit 1");
        String parameterizedString = sqlStatement.toParameterizedString();

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
        MycatExecutor executor = phyPlan.implement(new MycatExecutorImplementor() {
        });
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
                MycatRules2.ProjectView.INSTANCE,
                MycatRules2.FilterView.INSTACNE,
                MycatRules2.ProjectView.INSTANCE,
                MycatRules2.AggregateView.INSTACNE,
                MycatRules2.ProjectView.INSTANCE,
                MycatRules2.JoinView.INSTANCE,
                MycatRules2.ProjectView.INSTANCE,
                MycatRules2.CorrelateView.INSTANCE,
                MycatRules2.ProjectView.INSTANCE,
                MycatRules2.SortView.INSTACNE,
                MycatRules2.ProjectView.INSTANCE
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