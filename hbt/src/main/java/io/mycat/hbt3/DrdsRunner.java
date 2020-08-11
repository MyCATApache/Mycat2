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
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlOutputVisitor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.TableHandler;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.MycatCalciteMySqlNodeVisitor;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.hbt4.*;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.metadata.SchemaHandler;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.JDBCType;
import java.util.*;

public class DrdsRunner {
    public List<String> explainSql(DrdsConst config,
                                   PlanCache planCache,
                                   String defaultSchemaName,
                                   String originalSql) {
        List<String> lines = new ArrayList<>();
        try (RowBaseIterator rowBaseIterator = doAction(config, planCache, null, defaultSchemaName, originalSql, true)) {
            while (rowBaseIterator.next()) {
                lines.add(rowBaseIterator.getString(1));
            }
        }
        return lines;
    }
    @SneakyThrows
    public RowBaseIterator doHbt(DrdsConst config,
                                    PlanCache planCache,
                                    DatasourceFactory factory,
                                    String hbtText){

    }

    @SneakyThrows
    public RowBaseIterator doAction(DrdsConst config,
                                    PlanCache planCache,
                                    DatasourceFactory factory,
                                    String defaultSchemaName,
                                    String originalSql,
                                    boolean explain) {
        SchemaPlus plus = CalciteSchema.createRootSchema(false).plus();
        List<MycatSchema> schemas = new ArrayList<>();
        for (Map.Entry<String, SchemaHandler> entry : config.schemas().entrySet()) {
            String schemaName = entry.getKey();
            SchemaHandler schemaHandler = entry.getValue();
            Map<String, Table> logicTableMap = new HashMap<>();
            for (TableHandler tableHandler : schemaHandler.logicTables().values()) {
                MycatLogicTable logicTable = new MycatLogicTable(tableHandler);
                logicTableMap.put(logicTable.getTable().getTableName(), logicTable);
            }
            MycatSchema schema = MycatSchema.create(config, schemaName, logicTableMap);
            plus.add(schemaName, schema);
            schemas.add(schema);
        }
        if (config.isAutoCreateTable()) {
            autoCreateTable(factory, schemas);
        }
        List<Object> parameters;
        String parameterizedString;
        if (false) {
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
        if (explain) {
            final StringWriter sw = new StringWriter();
            final RelWriter planWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, false);
            relNode1.explain(planWriter);
            String[] split = sw.getBuffer().toString().split("\n");
            ResultSetBuilder builder = ResultSetBuilder.create();
            builder.addColumnInfo("explain", JDBCType.VARCHAR);
            for (String s : split) {
                builder.addObjectRowPayload(Arrays.asList(s));
            }
            return builder.build();
        }
        return execute(factory, parameters, relNode1);
    }

    public RowBaseIterator execute(
            DatasourceFactory datasourceFactory,
            List<Object> parameters,
            MycatRel relNode1) {
        RelDataType rowType = relNode1.getRowType();
        MycatContext context = new MycatContext();
        ExecutorImplementorImpl executorImplementor = new ExecutorImplementorImpl(context, datasourceFactory, new TempResultSetFactoryImpl());
        Executor implement = relNode1.implement(executorImplementor);
        implement.open();
        return new EnumeratorRowIterator(new CalciteRowMetaData(rowType.getFieldList()), Linq4j.asEnumerable(() -> implement.outputObjectIterator()).enumerator(),
                () -> {
                });
    }

    public MycatRel createRelNode(DrdsConst config, PlanCache planCache, String defaultSchemaName, String sql, SchemaPlus plus) throws SqlParseException {
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
                defaultSchemaName != null ? ImmutableList.of(defaultSchemaName) : ImmutableList.of(),
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
        return (MycatRel) optimizeWithCBO(logPlan);
    }

    static boolean isComplex(RelNode logPlan) {
        ComplexJudged complexJudged = new ComplexJudged();
        logPlan.accept(complexJudged);
        return complexJudged.c;

    }

    static class ComplexJudged extends RelShuttleImpl {
        boolean c = false;

        @Override
        public RelNode visit(LogicalJoin join) {
            RelNode left = join.getLeft();
            RelNode right = join.getRight();
            RelOptTable leftTable = left.getTable();
            RelOptTable rightTable = right.getTable();
            if (leftTable != null && rightTable != null) {
                AbstractMycatTable leftT = leftTable.unwrap(AbstractMycatTable.class);
                AbstractMycatTable rightT = rightTable.unwrap(AbstractMycatTable.class);
                if (leftT != null && rightT != null) {
                    if (!leftT.computeDataNode().isSingle() && !rightT.computeDataNode().isSingle()) {
                        c = true;
                    }
                } else {
                    c = true;
                }
            } else {
                c = true;
            }
            return super.visit(join);
        }
    }

    public SqlNode parseSql(String sql) throws SqlParseException {
        boolean fast = true;
        SqlNode sqlNode;
        if (true) {
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
            for (Table table : mycatSchema.getMycatTableMap().values()) {
//                String schemaName = table.getSchemaName();
//                String createTableSql = table.getCreateTableSql();
//                MySqlCreateTableStatement proto = (MySqlCreateTableStatement) SQLUtils.parseSingleMysqlStatement(createTableSql);
//                proto.setDbPartitionBy(null);
//                proto.setDbPartitions(null);
//                proto.setTablePartitionBy(null);
//                proto.setTablePartitions(null);
//                proto.setExPartition(null);
//                proto.setStoredBy(null);
//                proto.setDistributeByType(null);
//
//                MySqlCreateTableStatement cur = proto.clone();
//
//                Distribution partInfo = table.computeDataNode();
//                cur.setTableName(table.getTableName());
//                cur.setSchema(schemaName);
//                for (DataNode dataNode : partInfo.dataNodes()) {
//                    String backendSchemaName = dataNode.getSchema();
//                    String backendTableName = dataNode.getTable();
//                    cur.setTableName(backendTableName);
//                    cur.setSchema(backendSchemaName);
//                    datasourceFactory.createTableIfNotExisted(dataNode.getTargetName(), cur.toString());
//                }
            }
        }
    }

    public static MycatRel optimizeWithCBO(RelNode logPlan) {
        if (logPlan instanceof MycatRel) {
            return (MycatRel) logPlan;
        } else {
            RelOptCluster cluster = logPlan.getCluster();
            RelOptPlanner planner = cluster.getPlanner();
            planner.clear();
            MycatConvention.INSTANCE.register(planner);
            logPlan = planner.changeTraits(logPlan, cluster.traitSetOf(MycatConvention.INSTANCE));
            planner.setRoot(logPlan);
            return (MycatRel) planner.findBestExp();
        }
    }

    static final ImmutableSet<RelOptRule> FILTER = ImmutableSet.of(
            CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.JOIN_EXTRACT_FILTER,
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.FILTER_INTO_JOIN_DUMB,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.FILTER_CORRELATE,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
            CoreRules.FILTER_MULTI_JOIN_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_EXPAND_IS_NOT_DISTINCT_FROM,
            CoreRules.FILTER_SET_OP_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.SEMI_JOIN_FILTER_TRANSPOSE,
            CoreRules.PROJECT_FILTER_TRANSPOSE,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_MERGE
    );

    private static RelNode optimizeWithRBO(RelNode logPlan) {
        boolean complex = (isComplex(logPlan));
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchLimit(1024);
        builder.addRuleCollection(FILTER);
        if (complex) {
            ImmutableList<RelOptRule> relOptRules = ImmutableList.of(MycatFilterPhyViewRule.INSTANCE,
                    MycatTablePhyViewRule.INSTANCE,
                    CoreRules.PROJECT_SET_OP_TRANSPOSE,
                    CoreRules.FILTER_SET_OP_TRANSPOSE,
                    CoreRules.JOIN_LEFT_UNION_TRANSPOSE,
                    CoreRules.JOIN_RIGHT_UNION_TRANSPOSE,
                    CoreRules.AGGREGATE_UNION_TRANSPOSE,
                    CoreRules.PROJECT_SET_OP_TRANSPOSE,
                    CoreRules.FILTER_SET_OP_TRANSPOSE,
                    CoreRules.AGGREGATE_UNION_TRANSPOSE,
                    CoreRules.SORT_UNION_TRANSPOSE,
                    CoreRules.SORT_UNION_TRANSPOSE_MATCH_NULL_FETCH,
                    CoreRules.UNION_MERGE,
                    CoreRules.UNION_REMOVE,
                    CoreRules.PROJECT_MERGE,
                    CoreRules.FILTER_MERGE
            );
            builder.addRuleCollection(relOptRules);
        }
//        builder.addRuleInstance(MycatTableViewRule.INSTANCE);
        builder.addRuleCollection(MycatSQLViewRules.RULES);
//        builder.addRuleCollection(MycatGatherRules.RULES);
        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(logPlan);
        RelNode bestExp = planner.findBestExp();
        return bestExp.accept(new SQLRBORewriter());
    }

    private static RelOptCluster newCluster() {
        RelOptPlanner planner = new VolcanoPlanner();
        ImmutableList<RelTraitDef> TRAITS = ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
        for (RelTraitDef i : TRAITS) {
            planner.addRelTraitDef(i);
        }
        FILTER.forEach(f -> planner.addRule(f));
        return RelOptCluster.create(planner, MycatCalciteSupport.INSTANCE.RexBuilder);
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;

}