package io.mycat.hbt3;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.hbt4.*;
import io.mycat.mpp.Row;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
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

public class DrdsRunner {

    public void doAction(DrdsConfig config,
                         String defaultSchemaName,
                         String sql,
                         ResultSetHanlder resultSetHanlder) {
        try {
            SchemaPlus plus = CalciteSchema.createRootSchema(true).plus();
            List<MycatSchema> schemas = new ArrayList<>();
            config.getSchemas().forEach((schemaName, value) -> {
                MycatSchema schema = new MycatSchema();
                schema.setDrdsConst(config);
                schema.setSchemaName(schemaName);
                schema.setCreateTableSqls(value);
                schema.init();
                plus.add(schemaName, schema);
                schemas.add(schema);
            });

            if (config.isAutoCreateTable()) {
                autoCreateTable(schemas);
            }
            ///////////////////////////////////////////////////////////////////////////////////
            CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema
                    .from(plus),
                    ImmutableList.of(defaultSchemaName),
                    MycatCalciteSupport.INSTANCE.TypeFactory,
                    MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());

            SqlValidator validator = SqlValidatorUtil.newValidator(MycatCalciteSupport.INSTANCE.config.getOperatorTable(),
                    catalogReader, MycatCalciteSupport.INSTANCE.TypeFactory, MycatCalciteSupport.INSTANCE.getValidatorConfig());
            ;
            SqlParser sqlParser = SqlParser.create(sql, MycatCalciteSupport.INSTANCE.SQL_PARSER_CONFIG);
            SqlNode sqlNode = sqlParser.parseQuery();

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
            RelNode logPlan = finalRoot.project();
            logPlan = optimizeWithRBO(logPlan);
            RBO rbo = new RBO();
            RelNode relNode = logPlan.accept(rbo);
            MycatRel relNode1 = (MycatRel) optimizeWithCBO(relNode, cluster);
            Map<Object, Object> context = new HashMap<>();
            try (DatasourceFactoryImpl datasourceFactory = new DatasourceFactoryImpl()) {
                RelDataType rowType = relNode1.getRowType();
                CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(rowType.getFieldList());
                resultSetHanlder.onMetadata(calciteRowMetaData);
                ExecutorImplementorImpl executorImplementor = new ExecutorImplementorImpl(context, datasourceFactory);
                Executor implement = relNode1.implement(executorImplementor);
                implement.open();
                Iterator<Row> iterator = implement.iterator();
                while (iterator.hasNext()) {
                    resultSetHanlder.onRow(iterator.next());
                }
                resultSetHanlder.onOk();
            }
        } catch (Throwable e) {
            resultSetHanlder.onError(e);
        }
    }

    public Prepare prepare(DrdsConfig config, String schema, String sql) {
        return null;
    }

    public void execute(DrdsConfig config, long id, List<Object> params, ResultSetHanlder hanlder) {

    }

    public static class Prepare {
        long id;
        MycatRowMetaData columns;
        MycatRowMetaData params;
    }

    public static void autoCreateTable(List<MycatSchema> schemas) {
        DatasourceFactoryImpl datasourceFactory1 = new DatasourceFactoryImpl();
        for (MycatSchema mycatSchema : schemas) {
            for (MycatTable table : mycatSchema.getMycatTableMap().values()) {
                String schemaName = table.getSchemaName();
                String createTableSql = table.getCreateTableSql();
                MySqlCreateTableStatement proto = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSql);
                proto.setDbPartitionBy(null);
                proto.setDbPartitions(null);
                proto.setTablePartitionBy(null);
                proto.setTablePartitions(null);
                proto.setExPartition(null);
                proto.setStoredBy(null);
                proto.setDistributeByType(null);

                MySqlCreateTableStatement cur = proto.clone();
                cur.setTableName(table.getTableName());
                cur.setSchema(schemaName);
                datasourceFactory1.createTableIfNotExisted(0, cur.toString());
                PartInfo partInfo = table.computeDataNode();
                for (Part part : partInfo.toPartArray()) {
                    String backendSchemaName = part.getBackendSchemaName(table);
                    String backendTableName = part.getBackendTableName(table);
                    cur.setTableName(backendTableName);
                    cur.setSchema(backendSchemaName);
                    datasourceFactory1.createTableIfNotExisted(part.getMysqlIndex(), cur.toString());
                }
            }
        }
    }

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
        ImmutableList<RelTraitDef> TRAITS = ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
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