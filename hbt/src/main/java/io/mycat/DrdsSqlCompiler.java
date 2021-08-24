/**
 * Copyright (C) <2021>  <chen junwen>
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

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.mycat.calcite.*;
import io.mycat.calcite.localrel.LocalRules;
import io.mycat.calcite.logical.MycatViewIndexViewRule;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatProject;
import io.mycat.calcite.physical.MycatTopN;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.rewriter.*;
import io.mycat.calcite.rules.*;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.calcite.table.*;
import io.mycat.hbt.HBTQueryConvertor;
import io.mycat.hbt.SchemaConvertor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.parser.HBTParser;
import io.mycat.hbt.parser.ParseNode;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.MultiJoin;
import org.apache.calcite.rel.rules.MycatHepJoinClustering;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static io.mycat.DrdsExecutorCompiler.getCodeExecuterContext;

@Getter
public class DrdsSqlCompiler {
    private final static Logger log = LoggerFactory.getLogger(DrdsSqlCompiler.class);
    private final SchemaPlus schemas;
    private final DrdsConst config;
    private final CalciteCatalogReader catalogReader;

    public DrdsSqlCompiler(DrdsConst config) {
        this.schemas = DrdsRunnerHelper.convertRoSchemaPlus(config);
        this.config = config;
        this.catalogReader = new CalciteCatalogReader(CalciteSchema
                .from(this.schemas),
                ImmutableList.of(),
                MycatCalciteSupport.TypeFactory,
                MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());
    }

    @SneakyThrows
    public Plan doHbt(String hbtText) {
        log.debug("reveice hbt");
        log.debug(hbtText);
        HBTParser hbtParser = new HBTParser(hbtText);
        ParseNode statement = hbtParser.statement();
        SchemaConvertor schemaConvertor = new SchemaConvertor();
        Schema originSchema = schemaConvertor.transforSchema(statement);
        SchemaPlus plus = this.schemas;

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
                        Partition partition = ((MycatPhysicalTable) table).getPartition();
                        MycatPhysicalTable mycatPhysicalTable = (MycatPhysicalTable) table;
                        SqlNode sqlNode = MycatCalciteSupport.INSTANCE.convertToSqlTemplate(
                                scan,
                                MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(partition.getTargetName()),
                                false
                        );
                        SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(partition.getTargetName());
                        return new MycatTransientSQLTableScan(cluster,
                                mycatPhysicalTable.getRowType(),
                                partition.getTargetName(), sqlNode.toSqlString(dialect)
                                .getSql());
                    }
                }
                return super.visit(scan);
            }
        });
        bestExp = bestExp.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                return SQLRBORewriter.view(scan).orElse(scan);
            }
        });
        MycatRel mycatRel = optimizeWithCBO(bestExp, Collections.emptyList());
        CodeExecuterContext codeExecuterContext = getCodeExecuterContext(ImmutableMap.of(), mycatRel, false);
        return new PlanImpl(mycatRel, codeExecuterContext, mycatRel.getRowType().getFieldNames());
    }

    public MycatRel dispatch(OptimizationContext optimizationContext,
                             DrdsSql drdsSql) {
        return dispatch(optimizationContext, drdsSql, schemas);
    }

    public MycatRel dispatch(OptimizationContext optimizationContext,
                             DrdsSql drdsSql,
                             SchemaPlus plus) {
        SQLStatement sqlStatement = drdsSql.getParameterizedStatement();
        if (sqlStatement instanceof SQLSelectStatement) {
            return compileQuery(optimizationContext, plus, drdsSql);
        }
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);

        if (sqlStatement instanceof MySqlInsertStatement) {
            MySqlInsertStatement insertStatement = (MySqlInsertStatement) sqlStatement;
            String schemaName = SQLUtils.normalize(insertStatement.getTableSource().getSchema());
            String tableName = SQLUtils.normalize(insertStatement.getTableName().getSimpleName());
            TableHandler logicTable = Objects.requireNonNull(metadataManager.getTable(schemaName, tableName));
            switch (logicTable.getType()) {
                case SHARDING:
                    MycatInsertRel mycatInsertRel = new MycatInsertRel(sqlStatement, false);
                    optimizationContext.saveAlways();
                    return mycatInsertRel;
                case GLOBAL:
                    return complieGlobalUpdate(optimizationContext, drdsSql, sqlStatement, (GlobalTable) logicTable);
                case NORMAL:
                    return complieNormalUpdate(optimizationContext, drdsSql, sqlStatement, (NormalTable) logicTable);
                case CUSTOM:
                    throw new UnsupportedOperationException();
            }
        } else if (sqlStatement instanceof MySqlUpdateStatement) {
            SQLExprTableSource tableSource = (SQLExprTableSource) ((MySqlUpdateStatement) sqlStatement).getTableSource();
            String schemaName = SQLUtils.normalize(tableSource.getSchema());
            String tableName = SQLUtils.normalize(((MySqlUpdateStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schemaName, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileUpdate(logicTable, optimizationContext, drdsSql, plus);
                case GLOBAL: {
                    return complieGlobalUpdate(optimizationContext, drdsSql, sqlStatement, (GlobalTable) logicTable);
                }
                case NORMAL: {
                    return complieNormalUpdate(optimizationContext, drdsSql, sqlStatement, (NormalTable) logicTable);
                }
                case CUSTOM:
                    throw new UnsupportedOperationException();
            }
        } else if (sqlStatement instanceof SQLReplaceStatement) {
            SQLExprTableSource tableSource = (SQLExprTableSource) ((SQLReplaceStatement) sqlStatement).getTableSource();
            String schemaName = SQLUtils.normalize(Optional.ofNullable(tableSource).map(i -> i.getSchema()).orElse(null));
            String tableName = SQLUtils.normalize(((SQLReplaceStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schemaName, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileUpdate(logicTable, optimizationContext, drdsSql, plus);
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
            String schemaName = SQLUtils.normalize(Optional.ofNullable(tableSource).map(i -> i.getSchema()).orElse(null));
            String tableName = SQLUtils.normalize(((MySqlDeleteStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = metadataManager.getTable(schemaName, tableName);
            switch (logicTable.getType()) {
                case SHARDING:
                    return compileDelete(logicTable, optimizationContext, drdsSql, plus);
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
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(sqlStatement, true);
        optimizationContext.saveAlways();
        return mycatUpdateRel;
    }

    @NotNull
    private MycatRel complieNormalUpdate(OptimizationContext optimizationContext, DrdsSql drdsSql, SQLStatement sqlStatement, NormalTable logicTable) {
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(sqlStatement);
        optimizationContext.saveAlways();
        return mycatUpdateRel;
    }

    private MycatRel compileDelete(TableHandler logicTable, OptimizationContext optimizationContext, DrdsSql drdsSql, SchemaPlus plus) {
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(drdsSql.getParameterizedStatement());
        optimizationContext.saveAlways();
        return mycatUpdateRel;
    }

    private MycatRel compileUpdate(TableHandler logicTable, OptimizationContext optimizationContext, DrdsSql drdsSql, SchemaPlus plus) {
        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(drdsSql.getParameterizedStatement());
        optimizationContext.saveAlways();
        return mycatUpdateRel;
    }


    public MycatRel compileQuery(
            OptimizationContext optimizationContext,
            SchemaPlus plus,
            DrdsSql drdsSql) {
        RelNode logPlan;
        RelNodeContext relNodeContext = null;
        {
            relNodeContext = getRelRoot(plus, drdsSql);
            logPlan = relNodeContext.getRoot().rel;
            optimizationContext.relNodeContext = relNodeContext;
        }

//        if (logPlan instanceof TableModify) {
//            LogicalTableModify tableModify = (LogicalTableModify) logPlan;
//            switch (tableModify.getOperation()) {
//                case DELETE:
//                case UPDATE:
//                    return planUpdate(tableModify, drdsSql, optimizationContext);
//                default:
//                    throw new UnsupportedOperationException("unsupported DML operation " + tableModify.getOperation());
//            }
//        }
        RelDataType finalRowType = logPlan.getRowType();
        RelNode rboLogPlan = optimizeWithRBO(logPlan);
//        if (!RelOptUtil.areRowTypesEqual(rboLogPlan.getRowType(), logPlan.getRowType(), true)) {
//            rboLogPlan = relNodeContext.getRelBuilder().push(rboLogPlan).rename(logPlan.getRowType().getFieldNames()).build();
//        }
//        rboLogPlan = rboLogPlan.accept(new RelShuttleImpl(){
//            @Override
//            public RelNode visit(TableScan scan) {
//                return SQLRBORewriter.view(scan).orElse(scan);
//            }
//        });
//        rboLogPlan = rboLogPlan.accept(new ToLocalConverter());
        MycatRel mycatRel = optimizeWithCBO(rboLogPlan, Collections.emptyList());
        if (!RelOptUtil.areRowTypesEqual(mycatRel.getRowType(), finalRowType, true)) {
            Project relNode = (Project) relNodeContext.getRelBuilder().push(mycatRel).rename(finalRowType.getFieldNames()).build();
            mycatRel = MycatProject.create(relNode.getInput(), relNode.getProjects(), relNode.getRowType());
        }
        return mycatRel;
    }

    public RelNodeContext getRelRoot(DrdsSql drdsSql) {
        return getRelRoot(this.schemas, drdsSql);
    }

    public RelNodeContext getRelRoot(
            SchemaPlus plus, DrdsSql drdsSql) {
        CalciteCatalogReader catalogReader = DrdsRunnerHelper.newCalciteCatalogReader(plus);
        SqlValidator validator = DrdsRunnerHelper.getSqlValidator(drdsSql, catalogReader);
        RelOptCluster cluster = newCluster();
        SqlToRelConverter sqlToRelConverter = new SqlToRelConverter(
                NOOP_EXPANDER,
                validator,
                catalogReader,
                cluster,
                MycatCalciteSupport.config.getConvertletTable(),
                MycatCalciteSupport.sqlToRelConverterConfig);

        SQLStatement sqlStatement = drdsSql.getParameterizedStatement();
        MycatCalciteMySqlNodeVisitor mycatCalciteMySqlNodeVisitor = new MycatCalciteMySqlNodeVisitor();
        sqlStatement.accept(mycatCalciteMySqlNodeVisitor);
        SqlNode sqlNode = mycatCalciteMySqlNodeVisitor.getSqlNode();
        SqlNode validated = validator.validate(sqlNode);
        RelDataType parameterRowType = validator.getParameterRowType(sqlNode);
        RelBuilder relBuilder = MycatCalciteSupport.relBuilderFactory.create(sqlToRelConverter.getCluster(), catalogReader);

        RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);
//        root = root.withRel(propagateRelHints(root.rel,validator,sqlToRelConverter));
        RelNode newRelNode = RelDecorrelator.decorrelateQuery(root.rel, relBuilder);

        return new RelNodeContext(root.withRel(newRelNode), sqlToRelConverter, validator, relBuilder, catalogReader, parameterRowType);
    }

//    private MycatRel planUpdate(LogicalTableModify tableModify,
//                                DrdsSql drdsSql, OptimizationContext optimizationContext) {
//        MycatLogicTable mycatTable = (MycatLogicTable) tableModify.getTable().unwrap(AbstractMycatTable.class);
//        RelNode input = tableModify.getInput();
//        if (input instanceof LogicalProject) {
//            input = ((LogicalProject) input).getInput();
//        }
//        RexNode condition;
//        if (input instanceof Filter && ((Filter) input).getInput() instanceof LogicalTableScan) {
//            RelDataType rowType = input.getRowType();
//             condition = ((Filter) input).getCondition();
//        }else {
//            condition = MycatCalciteSupport.RexBuilder.makeLiteral(true);
//        }
//        MycatUpdateRel mycatUpdateRel = new MycatUpdateRel(
//                drdsSql.getParameterizedStatement(),
//                mycatTable.getTable().getSchemaName(),
//                mycatTable.getTable().getTableName(),
//                condition);
//        optimizationContext.saveAlways();
//        return mycatUpdateRel;
//    }


    public MycatRel optimizeWithCBO(RelNode logPlan, Collection<RelOptRule> relOptRules) {
        if (logPlan instanceof MycatRel) {
            return (MycatRel) logPlan;
        } else {
            RelOptCluster cluster = logPlan.getCluster();
            RelOptPlanner planner = cluster.getPlanner();
            planner.clear();
            MycatConvention.INSTANCE.register(planner);
            ImmutableList.Builder<RelOptRule> listBuilder = ImmutableList.builder();
            listBuilder.addAll(MycatExtraSortRule.RULES);
            listBuilder.addAll(LocalRules.CBO_RULES);

            //算子交换
            // Filter/Join, TopN/Join, Agg/Join, Filter/Agg, Sort/Project, Join/TableLookup
            listBuilder.add(CoreRules.JOIN_PUSH_EXPRESSIONS);
            listBuilder.add(CoreRules.FILTER_INTO_JOIN);

//          TopN/Join
            listBuilder.add(CoreRules.SORT_JOIN_TRANSPOSE.config.withOperandFor(MycatTopN.class, Join.class).toRule());

            listBuilder.add(CoreRules.FILTER_SET_OP_TRANSPOSE.config.toRule());
            listBuilder.add(CoreRules.AGGREGATE_JOIN_TRANSPOSE.config.withOperandFor(Aggregate.class, Join.class, false).toRule());

            //Sort/Project
            listBuilder.add(CoreRules.SORT_PROJECT_TRANSPOSE.config.withOperandFor(Sort.class, Project.class).toRule());

            //index
            listBuilder.add(MycatViewIndexViewRule.DEFAULT_CONFIG.toRule());
            if (config.bkaJoin()) {
                //TABLELOOKUP
                listBuilder.add(MycatTableLookupSemiJoinRule.INSTANCE);
                listBuilder.add(MycatTableLookupCombineRule.INSTANCE);
                listBuilder.add(MycatJoinTableLookupTransposeRule.LEFT_INSTANCE);
                listBuilder.add(MycatJoinTableLookupTransposeRule.RIGHT_INSTANCE);
                listBuilder.add(MycatValuesJoinRule.INSTANCE);
            }


            listBuilder.build().forEach(c -> planner.addRule(c));
            MycatConvention.INSTANCE.register(planner);

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
            RelNode accept = bestExp.accept(new MatierialRewriter());
            return (MycatRel) accept;
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
            CoreRules.FILTER_INTO_JOIN,
            CoreRules.JOIN_CONDITION_PUSH,
            CoreRules.SORT_JOIN_TRANSPOSE,
            CoreRules.PROJECT_CORRELATE_TRANSPOSE,
            CoreRules.FILTER_AGGREGATE_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_SET_OP_TRANSPOSE,
            CoreRules.FILTER_PROJECT_TRANSPOSE,
            CoreRules.FILTER_REDUCE_EXPRESSIONS,
//            CoreRules.JOIN_REDUCE_EXPRESSIONS,
            CoreRules.PROJECT_REDUCE_EXPRESSIONS,
            CoreRules.FILTER_MERGE,
            CoreRules.JOIN_PUSH_EXPRESSIONS,
            CoreRules.JOIN_PUSH_TRANSITIVE_PREDICATES

    );

    private RelNode optimizeWithRBO(RelNode logPlan) {
        Program subQueryProgram = getSubQueryProgram();
        RelNode unSubQuery = subQueryProgram.run(null, logPlan, null, Collections.emptyList(), Collections.emptyList());
        RelNode unAvg = resolveAggExpr(unSubQuery);
        unAvg = unAvg.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(TableScan scan) {
                return SQLRBORewriter.view(scan).orElse(scan);
            }
        });
        RelNode joinClustering = toMultiJoin(unAvg).map(relNode -> {
            HepProgramBuilder builder = new HepProgramBuilder();

            builder.addGroupBegin();
            builder.addRuleInstance(MycatHepJoinClustering.Config.DEFAULT.toRule());
            builder.addGroupEnd();
            builder.addMatchLimit(64);
            builder.addGroupBegin();
            builder.addRuleInstance(CoreRules.MULTI_JOIN_OPTIMIZE);
            builder.addGroupEnd();

            HepPlanner planner = new HepPlanner(builder.build());
            planner.setRoot(relNode);
            RelNode bestExp = planner.findBestExp();

            return bestExp;
        }).flatMap(relNode -> {
            class MultiJoinFinder extends RelShuttleImpl {
                boolean multiJoin = false;

                @Override
                public RelNode visit(RelNode other) {
                    if (other instanceof MultiJoin) {
                        multiJoin = true;
                    }
                    return super.visit(other);
                }
            }
            MultiJoinFinder multiJoinFinder = new MultiJoinFinder();
            relNode.accept(multiJoinFinder);

            if (multiJoinFinder.multiJoin) {
                return Optional.empty();
            }
            return Optional.of(relNode);
        }).orElse(unAvg);

        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addGroupBegin().addRuleCollection(ImmutableList.of(
                AggregateExpandDistinctAggregatesRule.Config.DEFAULT.toRule(),
                CoreRules.AGGREGATE_ANY_PULL_UP_CONSTANTS,
                CoreRules.PROJECT_MERGE,
                CoreRules.PROJECT_CORRELATE_TRANSPOSE,
                CoreRules.PROJECT_SET_OP_TRANSPOSE,
                CoreRules.PROJECT_JOIN_TRANSPOSE,
                CoreRules.PROJECT_WINDOW_TRANSPOSE,
                CoreRules.PROJECT_FILTER_TRANSPOSE,
                ProjectRemoveRule.Config.DEFAULT.toRule()
        )).addGroupEnd().addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin().addRuleCollection(FILTER).addGroupEnd().addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addGroupBegin().addRuleInstance(CoreRules.PROJECT_MERGE).addGroupEnd().addMatchOrder(HepMatchOrder.ARBITRARY);
        builder.addGroupBegin()
                .addRuleCollection(LocalRules.RBO_RULES)
                .addRuleInstance( MycatAggDistinctRule.Config.DEFAULT.toRule())
                .addGroupEnd().addMatchOrder(HepMatchOrder.BOTTOM_UP);
        builder.addMatchLimit(1024);
        HepPlanner planner = new HepPlanner(builder.build());
        planner.setRoot(joinClustering);
        RelNode bestExp = planner.findBestExp();
        return bestExp;
    }

    private Optional<RelNode> toMultiJoin(RelNode logPlan) {
        if (RelOptUtil.countJoins(logPlan) > 1) {
            final HepProgram hep = new HepProgramBuilder()
                    .addRuleInstance(CoreRules.FILTER_INTO_JOIN)
                    .addMatchOrder(HepMatchOrder.BOTTOM_UP)
                    .addRuleInstance(CoreRules.JOIN_TO_MULTI_JOIN)
                    .build();
            final Program program1 =
                    Programs.of(hep, false, DefaultRelMetadataProvider.INSTANCE);
            return Optional.of(program1.run(null, logPlan, null, Collections.emptyList(), Collections.emptyList()));
        } else {
            return Optional.empty();
        }
    }

    private RelNode resolveAggExpr(RelNode logPlan) {
        final HepProgram hepProgram = new HepProgramBuilder()
                .addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS)
                .addRuleInstance(CoreRules.PROJECT_TO_LOGICAL_PROJECT_AND_WINDOW)
                .build();
        final HepPlanner planner = new HepPlanner(hepProgram);
        planner.setRoot(logPlan);
        RelNode rootRel = planner.findBestExp();
        return rootRel;
    }

    @NotNull
    private Program getSubQueryProgram() {
        final HepProgramBuilder builder = HepProgram.builder();
        builder.addRuleCollection(
                ImmutableList.of(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                        CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                        CoreRules.JOIN_SUB_QUERY_TO_CORRELATE));
        Program subQuery = Programs.of(builder.build(), true, DefaultRelMetadataProvider.INSTANCE);
        return subQuery;
    }

    @NotNull
    public CalciteCatalogReader newCalciteCatalogReader() {
        return DrdsRunnerHelper.newCalciteCatalogReader(schemas);
    }

    public static RelOptCluster newCluster() {
        RelOptPlanner planner = new VolcanoPlanner();
        ImmutableList<RelTraitDef> TRAITS = ImmutableList.of(ConventionTraitDef.INSTANCE, RelCollationTraitDef.INSTANCE);
        for (RelTraitDef i : TRAITS) {
            planner.addRelTraitDef(i);
        }
        RelOptCluster relOptCluster = RelOptCluster.create(planner, MycatCalciteSupport.RexBuilder);
        HintStrategyTable hintStrategies = HintTools.createHintStrategies();
        relOptCluster.setHintStrategies(hintStrategies);
        return relOptCluster;
    }

    private static final RelOptTable.ViewExpander NOOP_EXPANDER = (rowType, queryString, schemaPath, viewPath) -> null;

}