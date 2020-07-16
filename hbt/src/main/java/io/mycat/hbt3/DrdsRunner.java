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
package io.mycat.hbt3;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.calcite.MycatCalciteMySqlNodeVisitor;
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
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DrdsRunner {

    public void doAction(DrdsConfig config,
                         PlanCache planCache,
                         DatasourceFactory factory,
                         String defaultSchemaName,
                         String originalSql,
                         ResultSetHanlder resultSetHanlder) throws Throwable {
        SchemaPlus plus = CalciteSchema.createRootSchema(false).plus();
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
            autoCreateTable(factory, schemas);
        }
        List<Object> parameters;
        String parameterizedString;
        if (config.isPlanCache()) {
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(originalSql);
            StringBuilder sb = new StringBuilder(originalSql.length());
            MySqlOutputVisitor mySqlOutputVisitor = new MySqlOutputVisitor(sb, true);
            sqlStatement.accept(mySqlOutputVisitor);
            parameters = mySqlOutputVisitor.getParameters();
            parameterizedString = sb.toString();
        } else {
            parameters = ImmutableList.of();
            parameterizedString = originalSql;
        }
        MycatRel relNode1 = createRelNode(config, planCache, defaultSchemaName, parameterizedString, plus);

        execute(resultSetHanlder, factory, parameters, relNode1);
    }

    public void execute(ResultSetHanlder resultSetHanlder,
                        DatasourceFactory datasourceFactory,
                        List<Object> parameters,
                        MycatRel relNode1) {
        RelDataType rowType = relNode1.getRowType();
        CalciteRowMetaData calciteRowMetaData = new CalciteRowMetaData(rowType.getFieldList());
        resultSetHanlder.onMetadata(calciteRowMetaData);
        ExecutorImplementorImpl executorImplementor = new ExecutorImplementorImpl(parameters, datasourceFactory);
        Executor implement = relNode1.implement(executorImplementor);
        implement.open();
        Iterator<Row> iterator = implement.iterator();
        while (iterator.hasNext()) {
            resultSetHanlder.onRow(iterator.next());
        }
        resultSetHanlder.onOk();
    }

    public MycatRel createRelNode(DrdsConfig config, PlanCache planCache, String defaultSchemaName, String sql, SchemaPlus plus) throws SqlParseException {
        MycatRel relNode1;
        if (config.isPlanCache()) {
            Plan plan = planCache.getMinCostPlan(sql);
            if (plan != null) {
                relNode1 = plan.getRelNode();
            } else {
                relNode1 = compile(defaultSchemaName, sql, plus);
                RelOptCluster cluster = relNode1.getCluster();
                RelOptPlanner planner = cluster.getPlanner();
                RelOptCost relOptCost = relNode1.computeSelfCost(planner, cluster.getMetadataQuery());
                plan = new PlanImpl(relOptCost, relNode1);
                planCache.put(sql, plan);
            }
        } else {
            relNode1 = compile(defaultSchemaName, sql, plus);
        }
        return relNode1;
    }

    public MycatRel compile(String defaultSchemaName, String sql, SchemaPlus plus) throws SqlParseException {
        ///////////////////////////////////////////////////////////////////////////////////
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema
                .from(plus),
                ImmutableList.of(defaultSchemaName),
                MycatCalciteSupport.INSTANCE.TypeFactory,
                MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());

        SqlValidator validator = SqlValidatorUtil.newValidator(MycatCalciteSupport.INSTANCE.config.getOperatorTable(),
                catalogReader, MycatCalciteSupport.INSTANCE.TypeFactory, MycatCalciteSupport.INSTANCE.getValidatorConfig());

        SqlNode sqlNode = parseSql(sql);

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
        return (MycatRel) optimizeWithCBO(relNode);
    }

    public SqlNode parseSql(String sql) throws SqlParseException {
        boolean fast = true;
        SqlNode sqlNode;
        if (fast) {
            MycatCalciteMySqlNodeVisitor mycatCalciteMySqlNodeVisitor = new MycatCalciteMySqlNodeVisitor();
            SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
            sqlStatement.accept(mycatCalciteMySqlNodeVisitor);
            sqlNode = mycatCalciteMySqlNodeVisitor.getSqlNode();
        } else {
            SqlParser sqlParser = SqlParser.create(sql, MycatCalciteSupport.INSTANCE.SQL_PARSER_CONFIG);
            sqlNode = sqlParser.parseQuery();
        }
        return sqlNode;
    }

    public static void autoCreateTable(DatasourceFactory datasourceFactory, List<MycatSchema> schemas) {
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
                datasourceFactory.createTableIfNotExisted(0, cur.toString());
                PartInfo partInfo = table.computeDataNode();
                for (Part part : partInfo.toPartArray()) {
                    String backendSchemaName = part.getBackendSchemaName(table);
                    String backendTableName = part.getBackendTableName(table);
                    cur.setTableName(backendTableName);
                    cur.setSchema(backendSchemaName);
                    datasourceFactory.createTableIfNotExisted(part.getMysqlIndex(), cur.toString());
                }
            }
        }
    }

    public static RelNode optimizeWithCBO(RelNode logPlan) {
        RelOptCluster cluster = logPlan.getCluster();
        RelOptPlanner planner =cluster.getPlanner();
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

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;

}