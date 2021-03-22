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
package io.mycat;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.calcite.*;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.*;
import io.mycat.calcite.plan.ObservablePlanImplementorImpl;
import io.mycat.calcite.plan.PlanImplementor;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.rewriter.MatierialRewriter;
import io.mycat.calcite.rewriter.OptimizationContext;
import io.mycat.calcite.rewriter.SQLRBORewriter;
import io.mycat.calcite.rules.MycatViewToIndexViewRule;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanCache;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.calcite.table.*;
import io.mycat.gsi.GSIService;
import io.mycat.hbt.HBTQueryConvertor;
import io.mycat.hbt.SchemaConvertor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.parser.HBTParser;
import io.mycat.hbt.parser.ParseNode;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.tree.ClassDeclaration;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.JDBCType;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.calcite.rel.rules.CoreRules.*;
import static org.apache.calcite.tools.Programs.ofRules;

public class DrdsRunner {
    final static Logger log = LoggerFactory.getLogger(DrdsRunner.class);
    private final SchemaPlus schemas;
    DrdsConst config;
    PlanCache planCache;

    public DrdsRunner(DrdsConst config, PlanCache planCache) {
        this.config = config;
        this.planCache = planCache;
        this.schemas = convertRoSchemaPlus(config);
    }

    @SneakyThrows
    public MycatRel doHbt(String hbtText) {
        log.debug("reveice hbt");
        log.debug(hbtText);
        HBTParser hbtParser = new HBTParser(hbtText);
        ParseNode statement = hbtParser.statement();
        SchemaConvertor schemaConvertor = new SchemaConvertor();
        Schema originSchema = schemaConvertor.transforSchema(statement);
        SchemaPlus plus = this.schemas;
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema
                .from(plus),
                ImmutableList.of(),
                MycatCalciteSupport.TypeFactory,
                MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());
        RelOptCluster cluster = newCluster();
        RelBuilder relBuilder = MycatCalciteSupport.relBuilderFactory.create(cluster, catalogReader);
        HBTQueryConvertor hbtQueryConvertor = new HBTQueryConvertor(Collections.emptyList(), relBuilder);
        RelNode relNode = hbtQueryConvertor.complie(originSchema);
        HepProgramBuilder hepProgramBuilder = new HepProgramBuilder();
        hepProgramBuilder.addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
        hepProgramBuilder.addMatchLimit(512);
        HepProgram hepProgram = hepProgramBuilder.build();
        HepPlanner hepPlanner = new HepPlanner(hepProgram);
        hepPlanner.setRoot(relNode);
        RelNode bestExp = hepPlanner.findBestExp();
        bestExp = bestExp.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                AbstractMycatTable table = scan.getTable().unwrap(AbstractMycatTable.class);
                if (table != null) {
                    if (table instanceof MycatPhysicalTable) {
                        DataNode dataNode = ((MycatPhysicalTable) table).getDataNode();
                        MycatPhysicalTable mycatPhysicalTable = (MycatPhysicalTable) table;
                        return new MycatTransientSQLTableScan(cluster,
                                mycatPhysicalTable.getRowType(),
                                dataNode.getTargetName(),
                                MycatCalciteSupport.INSTANCE.convertToSql(
                                        scan,
                                        MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(dataNode.getTargetName()),
                                        false
                                ).getSql());
                    }
                }
                return super.visit(scan);
            }
        });
        return optimizeWithCBO(bestExp, Collections.emptyList());
    }

    public DrdsSql preParse(String sqlStatement) {
        return preParse(SQLUtils.parseSingleMysqlStatement(sqlStatement));
    }

    public DrdsSql preParse(SQLStatement sqlStatement) {
        List<Object> params = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        MycatPreparedStatementUtil.outputToParameterized(sqlStatement, sb, Collections.emptyList(), params);
        String string = sb.toString();
        sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
        return DrdsSql.of(sqlStatement, string, params);
    }


    @SneakyThrows
    public DrdsSql convertToMycatRel(DrdsSql stmt, MycatDataContext dataContext, OptimizationContext optimizationContext) {
        SchemaPlus plus = this.schemas;
        return convertToMycatRel(planCache, stmt, plus, dataContext, optimizationContext);
    }

    public SchemaPlus convertRoSchemaPlus(DrdsConst config) {
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
        return plus;
    }

    private RowBaseIterator doExplain(MycatRel relNode1) {
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

    public DrdsSql convertToMycatRel(PlanCache planCache,
                                     DrdsSql drdsSql,
                                     SchemaPlus plus,
                                     MycatDataContext dataContext,
                                     OptimizationContext optimizationContext) {
        MycatRel rel;
        Plan minCostPlan = planCache.getMinCostPlan(drdsSql.getParameterizedString(), drdsSql.getTypes());
        if (minCostPlan != null) {
            switch (minCostPlan.getType()) {
                case PHYSICAL:
                    drdsSql.setRelNode((MycatRel) minCostPlan.getPhysical());
                    break;
                case UPDATE:
                case INSERT:
                    drdsSql.setRelNode(minCostPlan.getPhysical());
            }
        } else {
            rel = dispatch(optimizationContext, drdsSql, plus, dataContext);
            drdsSql.setRelNode(rel);
        }
        Objects.requireNonNull(drdsSql.getRelNode());
        return drdsSql;
    }

    public MycatRel dispatch(OptimizationContext optimizationContext,
                             DrdsSql drdsSql,
                             SchemaPlus plus, MycatDataContext dataContext) {
        SQLStatement sqlStatement = drdsSql.getSqlStatement();
        if (sqlStatement instanceof SQLSelectStatement) {
            return compileQuery(dataContext.getDefaultSchema(), optimizationContext, plus, drdsSql);
        }
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        metadataManager.resolveMetadata(sqlStatement);
        String defaultSchema = dataContext.getDefaultSchema();
        if (sqlStatement instanceof MySqlInsertStatement) {
            MySqlInsertStatement insertStatement = (MySqlInsertStatement) sqlStatement;

            String schema = SQLUtils.normalize(Optional.ofNullable(insertStatement.getTableSource()).map(i -> i.getSchema()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(insertStatement.getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schema, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileInsert((ShardingTable) logicTable, dataContext, drdsSql, optimizationContext);
                case GLOBAL:
                    return complieGlobalUpdate(optimizationContext, drdsSql, sqlStatement, (GlobalTable) logicTable);
                case NORMAL:
                    return complieNormalUpdate(optimizationContext, drdsSql, sqlStatement, (NormalTable) logicTable);
                case CUSTOM:
                    throw new UnsupportedOperationException();
            }
        } else if (sqlStatement instanceof MySqlUpdateStatement) {
            SQLExprTableSource tableSource = (SQLExprTableSource) ((MySqlUpdateStatement) sqlStatement).getTableSource();
            String schema = SQLUtils.normalize(Optional.ofNullable(tableSource).map(i -> i.getSchema()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(((MySqlUpdateStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schema, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileUpdate(logicTable, dataContext, optimizationContext, drdsSql, plus);
                case GLOBAL: {
                    return complieGlobalUpdate(optimizationContext, drdsSql, sqlStatement, (GlobalTable) logicTable);
                }
                case NORMAL: {
                    return complieNormalUpdate(optimizationContext, drdsSql, sqlStatement, (NormalTable) logicTable);
                }
                case CUSTOM:
                    throw new UnsupportedOperationException();
            }
        } else if (sqlStatement instanceof MySqlDeleteStatement) {
            SQLExprTableSource tableSource = (SQLExprTableSource) ((MySqlDeleteStatement) sqlStatement).getTableSource();
            String schema = SQLUtils.normalize(Optional.ofNullable(tableSource).map(i -> i.getSchema()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(((MySqlDeleteStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schema, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileDelete(logicTable, dataContext, optimizationContext, drdsSql, plus);
                case GLOBAL: {
                    return complieGlobalUpdate(optimizationContext, drdsSql, sqlStatement, (GlobalTable) logicTable);
                }
                case NORMAL: {
                    return complieNormalUpdate(optimizationContext, drdsSql, sqlStatement, (NormalTable) logicTable);
                }
                case CUSTOM:
                    throw new UnsupportedOperationException();
            }
        }
        return null;
    }

    @NotNull
    private MycatRel complieGlobalUpdate(OptimizationContext optimizationContext, DrdsSql drdsSql, SQLStatement sqlStatement, GlobalTable logicTable) {
        Distribution distribution = Distribution.of(logicTable);
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(distribution, sqlStatement, true);
        optimizationContext.saveAlways();
        return mycatUpdateRel;
    }

    @NotNull
    private MycatRel complieNormalUpdate(OptimizationContext optimizationContext, DrdsSql drdsSql, SQLStatement sqlStatement, NormalTable logicTable) {
        Distribution distribution = Distribution.of(logicTable);
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(distribution, sqlStatement, false);
        optimizationContext.saveAlways();
        return mycatUpdateRel;
    }

    private MycatRel compileDelete(TableHandler logicTable, MycatDataContext dataContext, OptimizationContext optimizationContext, DrdsSql drdsSql, SchemaPlus plus) {
        return compileQuery(dataContext.getDefaultSchema(), optimizationContext, plus, drdsSql);
    }

    private MycatRel compileUpdate(TableHandler logicTable, MycatDataContext dataContext, OptimizationContext optimizationContext, DrdsSql drdsSql, SchemaPlus plus) {
        return compileQuery(dataContext.getDefaultSchema(), optimizationContext, plus, drdsSql);
    }

    private MycatRel compileInsert(ShardingTableHandler logicTable,
                                   MycatDataContext dataContext,
                                   DrdsSql drdsSql,
                                   OptimizationContext optimizationContext) {
        MySqlInsertStatement mySqlInsertStatement = drdsSql.getSqlStatement();
        List<SQLIdentifierExpr> columnsTmp = (List) mySqlInsertStatement.getColumns();
        boolean autoIncrement = logicTable.isAutoIncrement();
        int autoIncrementIndexTmp = -1;
        ArrayList<Integer> shardingKeys = new ArrayList<>();
        CustomRuleFunction function = logicTable.function();
        List<SimpleColumnInfo> metaColumns;
        if (columnsTmp.isEmpty()) {//fill columns
            int index = 0;
            for (SimpleColumnInfo column : metaColumns = logicTable.getColumns()) {
                if (autoIncrement && logicTable.getAutoIncrementColumn() == column) {
                    autoIncrementIndexTmp = index;
                }
                if (function.isShardingKey(column.getColumnName())) {
                    shardingKeys.add(index);
                }
                mySqlInsertStatement.addColumn(new SQLIdentifierExpr(column.getColumnName()));
                index++;
            }
        } else {
            int index = 0;
            metaColumns = new ArrayList<>();
            for (SQLIdentifierExpr column : columnsTmp) {
                SimpleColumnInfo simpleColumnInfo = logicTable.getColumnByName(SQLUtils.normalize(column.getName()));
                metaColumns.add(simpleColumnInfo);
                if (autoIncrement && logicTable.getAutoIncrementColumn() == simpleColumnInfo) {
                    autoIncrementIndexTmp = index;
                }
                if (function.isShardingKey(simpleColumnInfo.getColumnName())) {
                    shardingKeys.add(index);
                }
                index++;
            }
            if (autoIncrement && autoIncrementIndexTmp == -1) {
                SimpleColumnInfo autoIncrementColumn = logicTable.getAutoIncrementColumn();
                if (function.isShardingKey(autoIncrementColumn.getColumnName())) {
                    shardingKeys.add(index);
                }
                metaColumns.add(autoIncrementColumn);
                mySqlInsertStatement.addColumn(new SQLIdentifierExpr(autoIncrementColumn.getColumnName()));
                class CountIndex extends MySqlASTVisitorAdapter {
                    int currentIndex = -1;

                    @Override
                    public void endVisit(SQLVariantRefExpr x) {
                        currentIndex = Math.max(x.getIndex(), currentIndex);
                        super.endVisit(x);
                    }
                }
                CountIndex countIndex = new CountIndex();
                mySqlInsertStatement.accept(countIndex);
            }
        }
        final int finalAutoIncrementIndex = autoIncrementIndexTmp;
        MycatInsertRel mycatInsertRel = MycatInsertRel.create(finalAutoIncrementIndex, shardingKeys, mySqlInsertStatement, logicTable);
        optimizationContext.saveParameterized();
        return mycatInsertRel;
    }

    private MycatRel compileQuery(String defaultSchemaName,
                                  OptimizationContext optimizationContext,
                                  SchemaPlus plus,
                                  DrdsSql drdsSql) {
        RelNode logPlan;
        RelNodeContext relNodeContext = null;
        if (drdsSql.getRelNode() != null) {
            if (drdsSql.getRelNode() instanceof MycatRel) {
                return (MycatRel) drdsSql.getRelNode();
            }
            logPlan = drdsSql.getRelNode();
        } else {
            relNodeContext = getRelRoot(defaultSchemaName, plus, drdsSql);
            logPlan = relNodeContext.getRoot().project(false);
        }

        if (logPlan instanceof TableModify) {
            LogicalTableModify tableModify = (LogicalTableModify) logPlan;
            switch (tableModify.getOperation()) {
                case DELETE:
                case UPDATE:
                    return planUpdate(tableModify, drdsSql, optimizationContext);
                default:
                    throw new UnsupportedOperationException("unsupported DML operation " + tableModify.getOperation());
            }
        }
        RelNode rboLogPlan = optimizeWithRBO(logPlan, drdsSql, optimizationContext);
        if (relNodeContext != null && !(rboLogPlan instanceof MycatView)) {
            RelFieldTrimmer relFieldTrimmer = new MycatRelFieldTrimmer(relNodeContext.getValidator(), relNodeContext.getRelBuilder());
            rboLogPlan = relFieldTrimmer.trim(rboLogPlan);
        }
        Collection<RelOptRule> rboInCbo;
        if (MetaClusterCurrent.exist(GSIService.class)) {
            rboInCbo = Collections.singletonList(
                    new MycatViewToIndexViewRule(optimizationContext, drdsSql.getParams())
            );
        } else {
            rboInCbo = Collections.emptyList();
        }
        MycatRel mycatRel = optimizeWithCBO(rboLogPlan, rboInCbo);
        mycatRel = (MycatRel) mycatRel.accept(new MatierialRewriter());
        return mycatRel;
    }

    @AllArgsConstructor
    @Getter
    static class RelNodeContext {
        final RelRoot root;
        final SqlToRelConverter sqlToRelConverter;
        final SqlValidator validator;
        final RelBuilder relBuilder;
    }


    private RelNodeContext getRelRoot(String defaultSchemaName,
                                      SchemaPlus plus, DrdsSql drdsSql) {

        List<Object> params = drdsSql.getParams();

        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema
                .from(plus),
                defaultSchemaName != null ? ImmutableList.of(defaultSchemaName) : ImmutableList.of(),
                MycatCalciteSupport.TypeFactory,
                MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());
        SqlValidator validator = getSqlValidator(params, catalogReader);
        RelOptCluster cluster = newCluster();
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                MycatCalciteSupport.config.getConvertletTable(),
                MycatCalciteSupport.sqlToRelConverterConfig);

        SQLStatement sqlStatement = drdsSql.getSqlStatement();
        MycatCalciteMySqlNodeVisitor mycatCalciteMySqlNodeVisitor = new MycatCalciteMySqlNodeVisitor();
        sqlStatement.accept(mycatCalciteMySqlNodeVisitor);
        SqlNode sqlNode = mycatCalciteMySqlNodeVisitor.getSqlNode();


        SqlNode validated = validator.validate(sqlNode);

        RelBuilder relBuilder = MycatCalciteSupport.relBuilderFactory.create(sqlToRelConverter.getCluster(), catalogReader);

        RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);
        drdsSql.setAliasList(
                root.fields.stream()
                        .map(Pair::getValue).collect(Collectors.toList()));
//        RelRoot root2 =
//                root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));

        RelNode newRelNode = RelDecorrelator.decorrelateQuery(root.rel, relBuilder);
        return new RelNodeContext(root.withRel(newRelNode), sqlToRelConverter, validator, relBuilder);
    }


    private SqlValidator getSqlValidator(List<Object> params, CalciteCatalogReader catalogReader) {
        return new SqlValidatorImpl(SqlOperatorTables.chain(catalogReader, MycatCalciteSupport.config.getOperatorTable()), catalogReader, MycatCalciteSupport.TypeFactory,
                MycatCalciteSupport.INSTANCE.getValidatorConfig()) {
            @Override
            protected void inferUnknownTypes(@Nonnull RelDataType inferredType, @Nonnull SqlValidatorScope scope, @Nonnull SqlNode node) {
                if (node != null && node instanceof SqlDynamicParam) {
                    RelDataType relDataType = deriveType(scope, node);
                    return;
                }
                super.inferUnknownTypes(inferredType, scope, node);
            }

            @Override
            public RelDataType getUnknownType() {
                return super.getUnknownType();
            }

            @Override
            public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
                RelDataType res = resolveDynamicParam(expr);
                if (res == null) {
                    return super.deriveType(scope, expr);
                } else {
                    return res;
                }
            }

            @Override
            public void validateLiteral(SqlLiteral literal) {
                if (literal.getTypeName() == SqlTypeName.DECIMAL) {
                    return;
                }
                super.validateLiteral(literal);
            }

            private RelDataType resolveDynamicParam(SqlNode expr) {
                if (expr != null && expr instanceof SqlDynamicParam) {
                    int index = ((SqlDynamicParam) expr).getIndex();
                    if (index < params.size()) {
                        Object o = params.get(index);
                        if (o == null) {
                            return super.typeFactory.createUnknownType();
                        } else {
                            SqlTypeName type = null;
                            if (o instanceof String) {
                                type = SqlTypeName.VARCHAR;
                            } else if (o instanceof Number) {
                                type = SqlTypeName.DECIMAL;
                            } else {
                                Class<?> aClass = o.getClass();
                                for (SqlType value : SqlType.values()) {
                                    if (value.clazz == aClass) {
                                        type = SqlTypeName.getNameForJdbcType(value.id);
                                    }
                                }
                            }

                            Objects.requireNonNull(type, () -> "unknown type:" + o.getClass());
                            return super.typeFactory.createSqlType(type);
                        }
                    }
                }
                return null;
            }

            @Override
            public RelDataType getValidatedNodeType(SqlNode node) {
                RelDataType relDataType = resolveDynamicParam(node);
                if (relDataType == null) {
                    return super.getValidatedNodeType(node);
                } else {
                    return relDataType;
                }
            }

            @Override
            public CalciteException handleUnresolvedFunction(SqlCall call, SqlFunction unresolvedFunction, List<RelDataType> argTypes, List<String> argNames) {
                return super.handleUnresolvedFunction(call, unresolvedFunction, argTypes, argNames);
            }

            @Override
            protected void addToSelectList(List<SqlNode> list, Set<String> aliases, List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
                super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
            }

            @Override
            protected void validateWhereOrOn(SqlValidatorScope scope, SqlNode condition, String clause) {
                if (!condition.getKind().belongsTo(SqlKind.COMPARISON)) {
                    condition = SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO,
                            condition, SqlTypeUtil.convertTypeToSpec(typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
                }
                super.validateWhereOrOn(scope, condition, clause);
            }
        };
    }

    private MycatRel planUpdate(LogicalTableModify tableModify,
                                DrdsSql drdsSql, OptimizationContext optimizationContext) {
        RelNode input = tableModify.getInput();
        if (input instanceof LogicalProject) {
            input = ((LogicalProject) input).getInput();
        }
        if (input instanceof Filter && ((Filter) input).getInput() instanceof LogicalTableScan) {
            AbstractMycatTable mycatTable = tableModify.getTable().unwrap(AbstractMycatTable.class);
            RexNode condition = ((Filter) input).getCondition();
            Distribution distribution = mycatTable.createDistribution();
            MycatUpdateRel mycatUpdateRel = MycatUpdateRel.create(tableModify.getCluster(),
                    distribution,
                    Collections.singletonList(condition),
                    drdsSql.getSqlStatement());
            optimizationContext.saveParameterized();
            return mycatUpdateRel;
        }
        AbstractMycatTable mycatTable = tableModify.getTable().unwrap(AbstractMycatTable.class);
        Distribution distribution = mycatTable.createDistribution();
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(
                distribution,
                drdsSql.getSqlStatement(), mycatTable.isBroadCast());
        optimizationContext.saveAlways();
        return mycatUpdateRel;
    }

    public Plan convertToExecuter(DrdsSql drdsSql, MycatDataContext dataContext, OptimizationContext optimizationContext) {
        Plan minCostPlan = planCache.getMinCostPlan(drdsSql.getParameterizedString(), drdsSql.getTypes());
        if (minCostPlan != null) {
            switch (minCostPlan.getType()) {
                case PHYSICAL:
                    return minCostPlan;
            }
        }
        drdsSql = convertToMycatRel(drdsSql, dataContext, optimizationContext);
        MycatRel mycatRel = (MycatRel) drdsSql.getRelNode();
        if (mycatRel instanceof MycatUpdateRel) {
            return new PlanImpl((MycatUpdateRel) mycatRel);
        } else if (mycatRel instanceof MycatInsertRel) {
            return new PlanImpl((MycatInsertRel) mycatRel);
        }
        CodeExecuterContext codeExecuterContext = getCodeExecuterContext(Objects.requireNonNull(mycatRel), drdsSql.isForUpdate());
        return new PlanImpl(mycatRel, codeExecuterContext, drdsSql.isForUpdate());
    }

    @NotNull
    public static CodeExecuterContext getCodeExecuterContext(MycatRel relNode, boolean forUpdate) {
        int fieldCount = relNode.getRowType().getFieldCount();
        HashMap<String, Object> context = new HashMap<>(2);
        StreamMycatEnumerableRelImplementor mycatEnumerableRelImplementor = new StreamMycatEnumerableRelImplementor(context);
        relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof MycatView) {
                    mycatEnumerableRelImplementor.collectLeafRelNode(other);
                } else if (other instanceof MycatTransientSQLTableScan) {
                    mycatEnumerableRelImplementor.collectLeafRelNode(other);
                }
                return super.visit(other);
            }
        });
        ClassDeclaration classDeclaration = mycatEnumerableRelImplementor.implementHybridRoot(relNode, EnumerableRel.Prefer.ARRAY);
        String code = Expressions.toString(classDeclaration.memberDeclarations, "\n", false);
        if (log.isDebugEnabled()) {
            log.debug("----------------------------------------code----------------------------------------");
            log.debug(code);
        }
        return CodeExecuterContext.of(mycatEnumerableRelImplementor.getLeafRelNodes(), context,
                asObjectArray(getBindable(classDeclaration, code, fieldCount)),
                code, forUpdate);
    }

    @NotNull
    private static ArrayBindable asObjectArray(ArrayBindable bindable) {
        if (bindable.getElementType().isArray()) {
            return bindable;
        }
        return new ArrayBindable() {
            @Override
            public Class<Object[]> getElementType() {
                return Object[].class;
            }

            @Override
            public Enumerable<Object[]> bind(NewMycatDataContext dataContext) {
                Enumerable enumerable = bindable.bind(dataContext);
                return enumerable.select(e -> {
                    return new Object[]{e};
                });
            }
        };
    }

    @SneakyThrows
    static ArrayBindable getBindable(ClassDeclaration expr, String s, int fieldCount) {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to instantiate java compiler", e);
        }
        final IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
        cbe.setClassName(expr.name);
        cbe.setExtendedClass(Utilities.class);
        cbe.setImplementedInterfaces(new Class[]{ArrayBindable.class});
        cbe.setParentClassLoader(EnumerableInterpretable.class.getClassLoader());
        if (CalciteSystemProperty.DEBUG.value()) {
            // Add line numbers to the generated janino class
            cbe.setDebuggingInformation(true, true, true);
        }
        return (ArrayBindable) cbe.createInstance(new StringReader(s));
    }

    public MycatRel optimizeWithCBO(RelNode logPlan, Collection<RelOptRule> relOptRules) {
        if (logPlan instanceof MycatRel) {
            return (MycatRel) logPlan;
        } else {
            boolean needJoinReorder = RelOptUtil.countJoins(logPlan) > 2;
            if (needJoinReorder){
                logPlan = preJoinReorder(logPlan);
            }
            RelOptCluster cluster = logPlan.getCluster();
            RelOptPlanner planner = cluster.getPlanner();
            planner.clear();
            MycatConvention.INSTANCE.register(planner);
            planner.addRule(CoreRules.PROJECT_TO_CALC);
            planner.addRule(CoreRules.FILTER_TO_CALC);
            planner.addRule(CoreRules.CALC_MERGE);

            //joinReorder
            if (needJoinReorder) {
                planner.addRule(CoreRules.MULTI_JOIN_OPTIMIZE);
            }

            if (relOptRules != null) {
                for (RelOptRule relOptRule : relOptRules) {
                    planner.addRule(relOptRule);
                }
            }

            if (log.isDebugEnabled()) {
                MycatRelOptListener mycatRelOptListener = new MycatRelOptListener();
                planner.addListener(mycatRelOptListener);
                log.debug(mycatRelOptListener.dump());
            }
            logPlan = planner.changeTraits(logPlan, cluster.traitSetOf(MycatConvention.INSTANCE));
            planner.setRoot(logPlan);
            RelNode bestExp = planner.findBestExp();

            return (MycatRel) bestExp;
        }
    }

    public static RelNode preJoinReorder(RelNode logPlan) {
            final HepProgram hep = new HepProgramBuilder()
                    .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                    .addMatchOrder(HepMatchOrder.BOTTOM_UP)
                    .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
                    .addMatchLimit(512)
                    .build();
            final HepPlanner hepPlanner = new HepPlanner(hep,
                    null, false, null, RelOptCostImpl.FACTORY);
            List<RelMetadataProvider> list = new ArrayList<>();
            list.add(DefaultRelMetadataProvider.INSTANCE);
            hepPlanner.registerMetadataProviders(list);
            hepPlanner.setRoot(logPlan);
            logPlan = hepPlanner.findBestExp();
        return logPlan;
    }

    static final ImmutableSet<RelOptRule> FILTER = ImmutableSet.of(
            JOIN_PUSH_TRANSITIVE_PREDICATES,
            CoreRules.JOIN_SUB_QUERY_TO_CORRELATE,
            CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
            CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
            CoreRules.FILTER_INTO_JOIN,
//            CoreRules.FILTER_INTO_JOIN_DUMB,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.FILTER_CORRELATE,
            CoreRules.PROJECT_CORRELATE_TRANSPOSE,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
//            CoreRules.FILTER_MULTI_JOIN_MERGE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_SET_OP_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
//            CoreRules.SEMI_JOIN_FILTER_TRANSPOSE,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
            CoreRules.JOIN_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_MERGE,
            CoreRules.JOIN_PUSH_EXPRESSIONS,
            CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES
//            CoreRules.PROJECT_CALC_MERGE,
//            CoreRules.FILTER_CALC_MERGE,
//            CoreRules.FILTER_TO_CALC,
//            CoreRules.PROJECT_TO_CALC,
//            CoreRules.CALC_REMOVE,
//            CoreRules.CALC_MERGE

    );

    private static RelNode optimizeWithRBO(RelNode logPlan, DrdsSql drdsSql, OptimizationContext optimizationContext) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchLimit(512);
        builder.addRuleCollection(FILTER);
        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(logPlan);
        RelNode bestExp = planner.findBestExp();
        SQLRBORewriter sqlrboRewriter = new SQLRBORewriter();
        RelNode accept = bestExp.accept(sqlrboRewriter);
        return accept;
    }

    public static RelOptCluster newCluster() {
        RelOptPlanner planner = new VolcanoPlanner();
        ImmutableList<RelTraitDef> TRAITS = ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
        for (RelTraitDef i : TRAITS) {
            planner.addRelTraitDef(i);
        }
        return RelOptCluster.create(planner, MycatCalciteSupport.RexBuilder);
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;

    public PlanCache getPlanCache() {
        return planCache;
    }


    @SneakyThrows
    public Future<Void> runOnDrds(MycatDataContext dataContext,
                                  SQLStatement statement, Response response) {
        DrdsSql drdsSql = this.preParse(statement);
        Plan plan = getPlan(dataContext, drdsSql);
        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
        List<Object> params = drdsSql.getParams();
        PlanImplementor planImplementor = new ObservablePlanImplementorImpl(
                transactionSession,
                dataContext, params, response);
        return impl(plan, planImplementor);
    }

    private Future<Void> impl(Plan plan, PlanImplementor planImplementor) {
        switch (plan.getType()) {
            case PHYSICAL:
                return planImplementor.execute(plan);
            case UPDATE:
                return planImplementor.execute((MycatUpdateRel) Objects.requireNonNull(plan.getPhysical()));
            case INSERT:
                return planImplementor.execute((MycatInsertRel) Objects.requireNonNull(plan.getPhysical()));
            default: {
                return VertxUtil.newFailPromise(new MycatException(MySQLErrorCode.ER_NOT_SUPPORTED_YET, "不支持的执行计划"));
            }
        }
    }


    public Plan getPlan(MycatDataContext dataContext, DrdsSql drdsSql) {
        OptimizationContext optimizationContext = new OptimizationContext();
        Plan plan = this.convertToExecuter(drdsSql, dataContext, optimizationContext);
        getPlanCache().put(drdsSql.getParameterizedString(), drdsSql.getTypes(), plan);
        return plan;
    }

    public Future<Void> runHbtOnDrds(MycatDataContext dataContext, String statement, Response response) {
        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
        List<Object> params = Collections.emptyList();
        PlanImplementor planImplementor = new ObservablePlanImplementorImpl(
                transactionSession,
                dataContext, params, response);
        PlanImpl hbtPlan = getHbtPlan(dataContext, statement);
        return planImplementor.execute(hbtPlan);
    }

    public PlanImpl getHbtPlan(MycatDataContext dataContext, String statement) {
        DrdsRunner drdsRunners = this;
        MycatRel mycatRel = drdsRunners.doHbt(statement);
        CodeExecuterContext codeExecuterContext = getCodeExecuterContext(mycatRel, false);
        return new PlanImpl(mycatRel, codeExecuterContext, false);
    }


//    @NotNull
//    public static CompletableFuture<Enumerable<Object[]>> getJdbcExecuter(Plan plan, MycatDataContext context, List<Object> params) {
//        CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
//        JdbcConnectionUsage connectionUsage = JdbcConnectionUsage.computeJdbcTargetConnection(context, params, codeExecuterContext);
//        CompletableFuture<IdentityHashMap<RelNode, List<Enumerable<Object[]>>>> collect = connectionUsage.collect(MetaClusterCurrent.wrapper(JdbcConnectionManager.class), params);
//        return collect.thenCompose(map -> {
//            JdbcMycatDataContextImpl jdbcMycatDataContext = new JdbcMycatDataContextImpl(context, codeExecuterContext, map, params, plan.forUpdate());
//            ArrayBindable bindable = codeExecuterContext.getBindable();
//            Enumerable<Object[]> bindObservable = bindable.bind(jdbcMycatDataContext);
//            CalciteRowMetaData metaData = plan.getMetaData();
//            return CompletableFuture.completedFuture(bindObservable);
//        });
//    }


}