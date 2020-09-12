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

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLInsertStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlExportParameterVisitor;
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.SimpleColumnInfo;
import io.mycat.TableHandler;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.MycatCalciteMySqlNodeVisitor;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatCatalogReader;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.hbt.HBTQueryConvertor;
import io.mycat.hbt.SchemaConvertor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.parser.HBTParser;
import io.mycat.hbt.parser.ParseNode;
import io.mycat.hbt4.*;
import io.mycat.hbt4.executor.MycatPreparedStatementUtil;
import io.mycat.hbt4.logical.rel.MycatInsertRel;
import io.mycat.hbt4.logical.rel.MycatUpdateRel;
import io.mycat.metadata.*;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.*;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.JDBCType;
import java.util.*;

public class DrdsRunner {
    final static Logger log = LoggerFactory.getLogger(DrdsRunner.class);
    DrdsConst config;
    DatasourceFactory factory;
    PlanCache planCache;
    MycatDataContext dataContext;

    public DrdsRunner(DrdsConst config, DatasourceFactory factory, PlanCache planCache, MycatDataContext dataContext) {
        this.config = config;
        this.factory = factory;
        this.planCache = planCache;
        this.dataContext = dataContext;
    }

//    public List<String> explainSql(String originalSql) {
//        List<String> lines = new ArrayList<>(1);
//        Iterable<RowBaseIterator> objects = (Iterable) preParse(originalSql, Collections.emptyList());
//        RowBaseIterator rowBaseIterator = objects.iterator().next();
//        while (rowBaseIterator.next()) {
//            lines.add(rowBaseIterator.getString(1));
//        }
//        return lines;
//    }

    @SneakyThrows
    public MycatRel doHbt(String hbtText) {
        log.debug("reveice hbt");
        log.debug(hbtText);
        HBTParser hbtParser = new HBTParser(hbtText);
        ParseNode statement = hbtParser.statement();
        SchemaConvertor schemaConvertor = new SchemaConvertor();
        Schema originSchema = schemaConvertor.transforSchema(statement);
        SchemaPlus plus = convertRoSchemaPlus(config, factory);
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema
                .from(plus),
                ImmutableList.of(),
                MycatCalciteSupport.INSTANCE.TypeFactory,
                MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());
        RelOptCluster cluster = newCluster();
        RelBuilder relBuilder = MycatCalciteSupport.INSTANCE.relBuilderFactory.create(cluster, catalogReader);
        HBTQueryConvertor hbtQueryConvertor = new HBTQueryConvertor(Collections.emptyList(), relBuilder);
        RelNode relNode = hbtQueryConvertor.complie(originSchema);
        relNode = relNode.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                AbstractMycatTable table = scan.getTable().unwrap(AbstractMycatTable.class);
                if (table != null) {
                    return View.of(scan, table.computeDataNode());

                }
                return super.visit(scan);
            }
        });
        return optimizeWithCBO(relNode);
    }

    public Iterable<DrdsSql> preParse(String stmtList, List<Object> inputParameters) {
        List<SQLStatement> sqlStatements = SQLUtils.parseStatements(stmtList, DbType.mysql, true);
        return preParse(sqlStatements, inputParameters);
    }


    public Iterable<DrdsSql> preParse(List<SQLStatement> sqlStatements, List<Object> inputParameters) {
        Iterator<SQLStatement> iterator = sqlStatements.iterator();
        return () -> new Iterator<DrdsSql>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public DrdsSql next() {

                List<Object> params = new ArrayList<>();
                SQLStatement sqlStatement = iterator.next();
                StringBuilder sb = new StringBuilder();
                MycatPreparedStatementUtil.collect(sqlStatement, sb, inputParameters, params);
                String string = sb.toString();
                sqlStatement = SQLUtils.parseSingleMysqlStatement(string);
                return DrdsSql.of(sqlStatement, string, params);
            }
        };
    }


    @SneakyThrows
    public Iterable<DrdsSql> convertToMycatRel(Iterable<DrdsSql> stmtList) {
        SchemaPlus plus = convertRoSchemaPlus(config, factory);
        return convertToMycatRel(planCache, stmtList, plus);
    }

    private SchemaPlus convertRoSchemaPlus(DrdsConst config, DatasourceFactory factory) {
        SchemaPlus plus = CalciteSchema.createRootSchema(false).plus();
        MycatCalciteSupport.INSTANCE.functions.forEach((k,v)->plus.add(k, ScalarFunctionImpl.create(v,"eval")));
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

    public void execute(MycatRel relNode1, ExecutorImplementor executorImplementor) {
        String s = MycatCalciteSupport.INSTANCE.convertToMycatRelNodeText(relNode1);
        executorImplementor.implementRoot(relNode1);
        return;

//        factory.open();
//        implement.open();
//        if (implement instanceof MycatInsertExecutor) {
//            MycatInsertExecutor insertExecutor = (MycatInsertExecutor) implement;
//            return new long[]{insertExecutor.lastInsertId, insertExecutor.affectedRow};
//        }
//        if (implement instanceof MycatUpdateExecutor) {
//            MycatUpdateExecutor updateExecutor = (MycatUpdateExecutor) implement;
//            return new long[]{updateExecutor.lastInsertId, updateExecutor.affectedRow};
//        }
//        RelDataType rowType = relNode1.getRowType();
//
//
//        return new EnumeratorRowIterator(new CalciteRowMetaData(rowType.getFieldList()), Linq4j.asEnumerable(() -> implement.outputObjectIterator()).enumerator(),
//                () -> {
//                });
    }


    public Iterable<DrdsSql> convertToMycatRel(PlanCache planCache,
                                               Iterable<DrdsSql> stmtList, SchemaPlus plus) {
        Iterator<DrdsSql> iterator = stmtList.iterator();
        return () -> new Iterator<DrdsSql>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public DrdsSql next() {
                RelOptCluster cluster = newCluster();
                DrdsSql drdsSql = iterator.next();
                MycatRel rel;
                Plan minCostPlan = planCache.getMinCostPlan(drdsSql.getParameterizedString());
                if (minCostPlan != null) {
                    switch (minCostPlan.getType()) {
                        case PARSE:
                            drdsSql.setRelNode(minCostPlan.getRelNode());
                            OptimizationContext optimizationContext = new OptimizationContext(drdsSql.getParams(), planCache);
                            rel = dispatch(optimizationContext, drdsSql, plus);
                            break;
                        case FINAL:
                            rel = (MycatRel) minCostPlan.getRelNode();
                            break;
                        default:
                            throw new UnsupportedOperationException();
                    }
                } else {
                    OptimizationContext optimizationContext = new OptimizationContext(drdsSql.getParams(), planCache);
                    rel = dispatch(optimizationContext, drdsSql, plus);
                }
                drdsSql.setRelNode(rel);
                return drdsSql;
            }
        };
    }

    public MycatRel dispatch(OptimizationContext optimizationContext,
                             DrdsSql drdsSql,
                             SchemaPlus plus) {
        SQLStatement sqlStatement = drdsSql.getSqlStatement();
        if (sqlStatement instanceof SQLSelectStatement) {
            return compileQuery(dataContext.getDefaultSchema(), optimizationContext, plus, drdsSql);
        }
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        metadataManager.resolveMetadata(sqlStatement);
        String defaultSchema = dataContext.getDefaultSchema();
        if (sqlStatement instanceof MySqlInsertStatement) {
            MySqlInsertStatement insertStatement = (MySqlInsertStatement) sqlStatement;
            SchemaObject schemaObject = (insertStatement).getTableSource().getSchemaObject();
            String schema = SQLUtils.normalize(Optional.ofNullable(schemaObject).map(i -> i.getSchema()).map(i -> i.getName()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(insertStatement.getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schema, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileInsert((ShardingTable) logicTable, dataContext, drdsSql, optimizationContext);
                case GLOBAL:
                    return complieGlobalUpdate(optimizationContext, drdsSql, sqlStatement, (GlobalTableHandler) logicTable);
                case NORMAL:
                    return complieNormalUpdate(optimizationContext, drdsSql, sqlStatement, (NormalTableHandler) logicTable);
                case CUSTOM:
                    throw new UnsupportedOperationException();
            }
        } else if (sqlStatement instanceof MySqlUpdateStatement) {
            SQLExprTableSource tableSource = (SQLExprTableSource) ((MySqlUpdateStatement) sqlStatement).getTableSource();
            SchemaObject schemaObject = tableSource.getSchemaObject();
            String schema = SQLUtils.normalize(Optional.ofNullable(schemaObject).map(i -> i.getSchema()).map(i -> i.getName()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(((MySqlUpdateStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schema, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileUpdate(logicTable, dataContext, optimizationContext, drdsSql, plus);
                case GLOBAL: {
                    return complieGlobalUpdate(optimizationContext, drdsSql, sqlStatement, (GlobalTableHandler) logicTable);
                }
                case NORMAL: {
                    return complieNormalUpdate(optimizationContext, drdsSql, sqlStatement, (NormalTableHandler) logicTable);
                }
                case CUSTOM:
                    throw new UnsupportedOperationException();
            }
        } else if (sqlStatement instanceof MySqlDeleteStatement) {
            SQLExprTableSource tableSource = (SQLExprTableSource) ((MySqlDeleteStatement) sqlStatement).getTableSource();
            SchemaObject schemaObject = tableSource.getSchemaObject();
            String schema = SQLUtils.normalize(Optional.ofNullable(schemaObject).map(i -> i.getSchema()).map(i -> i.getName()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(((MySqlDeleteStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schema, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileDelete(logicTable, dataContext, optimizationContext, drdsSql, plus);
                case GLOBAL: {
                    return complieGlobalUpdate(optimizationContext, drdsSql, sqlStatement, (GlobalTableHandler) logicTable);
                }
                case NORMAL: {
                    return complieNormalUpdate(optimizationContext, drdsSql, sqlStatement, (NormalTableHandler) logicTable);
                }
                case CUSTOM:
                    throw new UnsupportedOperationException();
            }
        }
        return null;
    }

    @NotNull
    private MycatRel complieGlobalUpdate(OptimizationContext optimizationContext, DrdsSql drdsSql, SQLStatement sqlStatement, GlobalTableHandler logicTable) {
        GlobalTableHandler globalTableHandler = logicTable;
        Distribution distribution = Distribution.of(globalTableHandler.getGlobalDataNode(), false, Distribution.Type.BroadCast);
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(distribution, sqlStatement);
        optimizationContext.saveAlways(drdsSql.getParameterizedString(), mycatUpdateRel);
        return mycatUpdateRel;
    }

    @NotNull
    private MycatRel complieNormalUpdate(OptimizationContext optimizationContext, DrdsSql drdsSql, SQLStatement sqlStatement, NormalTableHandler logicTable) {
        NormalTableHandler normalTableHandler = logicTable;
        Distribution distribution = Distribution.of(ImmutableList.of(normalTableHandler.getDataNode()), false, Distribution.Type.PHY);
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(distribution, sqlStatement);
        optimizationContext.saveAlways(drdsSql.getParameterizedString(), mycatUpdateRel);
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
                SQLVariantRefExpr sqlVariantRefExpr = new SQLVariantRefExpr("?");
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
                sqlVariantRefExpr.setIndex(countIndex.currentIndex + 1);
                for (SQLInsertStatement.ValuesClause valuesClause : mySqlInsertStatement.getValuesList()) {
                    valuesClause.addValue(sqlVariantRefExpr);
                }
            }
        }
        final int finalAutoIncrementIndex = autoIncrementIndexTmp;
        MycatInsertRel mycatInsertRel = MycatInsertRel.create(finalAutoIncrementIndex, shardingKeys, mySqlInsertStatement, logicTable);
        optimizationContext.saveParameterized(drdsSql.getParameterizedString(), mycatInsertRel);
        return mycatInsertRel;
    }

    private MycatRel compileQuery(String defaultSchemaName,
                                  OptimizationContext optimizationContext,
                                  SchemaPlus plus,
                                  DrdsSql drdsSql) {
        SQLStatement sqlStatement = drdsSql.getSqlStatement();
        RelNode logPlan;
        if (drdsSql.getRelNode() != null) {
            if (drdsSql.getRelNode() instanceof MycatRel) {
                return (MycatRel) drdsSql.getRelNode();
            }
            logPlan = drdsSql.getRelNode();
        } else {
            logPlan = getRelRoot(defaultSchemaName, plus, sqlStatement);
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
        MycatRel cboLogPlan = optimizeWithCBO(rboLogPlan);
        if (!optimizationContext.predicateOnPhyView && !optimizationContext.predicateOnView) {
            //全表扫描
            optimizationContext.saveAlways(drdsSql.getParameterizedString(), cboLogPlan);
        } else if (!optimizationContext.predicateOnPhyView && optimizationContext.predicateOnView) {
            //缓存rel
            optimizationContext.saveParameterized(drdsSql.getParameterizedString(), cboLogPlan);
        } else {
            //仅缓存sql解析
            optimizationContext.saveParse(drdsSql.getParameterizedString(), logPlan);
        }
        return cboLogPlan;
    }

    private RelNode getRelRoot(String defaultSchemaName, SchemaPlus plus, SQLStatement sqlStatement) {
        MycatCalciteMySqlNodeVisitor mycatCalciteMySqlNodeVisitor = new MycatCalciteMySqlNodeVisitor();
        sqlStatement.accept(mycatCalciteMySqlNodeVisitor);
        SqlNode sqlNode = mycatCalciteMySqlNodeVisitor.getSqlNode();
        MycatCatalogReader catalogReader = new MycatCatalogReader(CalciteSchema
                .from(plus),
                defaultSchemaName != null ? ImmutableList.of(defaultSchemaName) : ImmutableList.of(),
                MycatCalciteSupport.INSTANCE.TypeFactory,
                MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());
        SqlValidator validator =

                new SqlValidatorImpl(ChainedSqlOperatorTable.of(catalogReader,MycatCalciteSupport.INSTANCE.config.getOperatorTable()), catalogReader, MycatCalciteSupport.INSTANCE.TypeFactory,
                        MycatCalciteSupport.INSTANCE.getValidatorConfig()) {
                    @Override
                    protected void inferUnknownTypes(@Nonnull RelDataType inferredType, @Nonnull SqlValidatorScope scope, @Nonnull SqlNode node) {

                        super.inferUnknownTypes(inferredType, scope, node);
                    }
                };
        SqlNode validated = validator.validate(sqlNode);
        RelOptCluster cluster = newCluster();
        RelBuilder relBuilder = MycatCalciteSupport.INSTANCE.relBuilderFactory.create(cluster, catalogReader);
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

        return root2.withRel(
                RelDecorrelator.decorrelateQuery(root.rel, relBuilder)).project();
    }

    private MycatRel planUpdate(LogicalTableModify tableModify,
                                DrdsSql drdsSql, OptimizationContext optimizationContext) {
        RelNode input = tableModify.getInput();
        if (input instanceof Filter && ((Filter) input).getInput() instanceof LogicalTableScan) {
            AbstractMycatTable mycatTable = tableModify.getTable().unwrap(AbstractMycatTable.class);
            RexNode condition = ((Filter) input).getCondition();
            Distribution distribution = mycatTable.computeDataNode(ImmutableList.of(condition));
            MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(tableModify.getCluster(),
                    distribution,
                    drdsSql.getSqlStatement());
            optimizationContext.saveParameterized(drdsSql.getParameterizedString(), mycatUpdateRel);
            return mycatUpdateRel;
        }
        AbstractMycatTable mycatTable = tableModify.getTable().unwrap(AbstractMycatTable.class);
        Distribution distribution = mycatTable.computeDataNode();
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(tableModify.getCluster(),
                distribution,
                drdsSql.getSqlStatement());
        optimizationContext.saveAlways(drdsSql.getParameterizedString(), mycatUpdateRel);
        return mycatUpdateRel;
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

    private static RelNode optimizeWithRBO(RelNode logPlan, DrdsSql drdsSql, OptimizationContext optimizationContext) {
        boolean complex = (isComplex(logPlan));
        optimizationContext.setComplex(complex);
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addMatchLimit(128);
        builder.addRuleCollection(FILTER);
        if (false) {
            MycatFilterPhyViewRule mycatFilterPhyViewRule = new MycatFilterPhyViewRule(optimizationContext);
            ImmutableList<RelOptRule> relOptRules = ImmutableList.of(mycatFilterPhyViewRule,
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
        builder.addRuleCollection(ImmutableList.of(
                new MycatFilterViewRule(optimizationContext),
                MycatProjectViewRule.INSTANCE,
                MycatJoinViewRule.INSTANCE,
                MycatAggregateViewRule.INSTANCE,
                MycatSortViewRule.INSTANCE
        ));

        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(logPlan);
        RelNode bestExp = planner.findBestExp();
        RelNode accept = bestExp.accept(new SQLRBORewriter(optimizationContext));
        return accept.accept(new MappingSqlFunctionRewriter());
    }

    public static RelOptCluster newCluster() {
        RelOptPlanner planner = new VolcanoPlanner();
        ImmutableList<RelTraitDef> TRAITS = ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
        for (RelTraitDef i : TRAITS) {
            planner.addRelTraitDef(i);
        }
        FILTER.forEach(f -> planner.addRule(f));
        return RelOptCluster.create(planner, MycatCalciteSupport.INSTANCE.RexBuilder);
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;

    private static class Parameterized {
        private String parameterizedString;
        private List<Object> parameters;

        public String getParameterizedString() {
            return parameterizedString;
        }

        public List<Object> getParameters() {
            return parameters;
        }

        public static Parameterized invoke(SQLStatement mySqlInsertStatement) {
            Parameterized parameterized = new Parameterized();
            StringBuilder sb = new StringBuilder();
            List<Object> params = new ArrayList<>();
            MySqlExportParameterVisitor parameterVisitor = new MySqlExportParameterVisitor(params, sb, true);
            mySqlInsertStatement.accept(parameterVisitor);
            parameterized.parameterizedString = sb.toString();
            parameterized.parameters = params;
            return parameterized;
        }
    }
}