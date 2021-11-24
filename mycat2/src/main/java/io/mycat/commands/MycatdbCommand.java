/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.commands;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLHint;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLNumericLiteralExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelect;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.parser.SQLType;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.google.common.collect.ImmutableClassToInstanceMap;
import io.mycat.*;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.DrdsRunnerHelper;
import io.mycat.calcite.MycatHint;
import io.mycat.calcite.spm.*;
import io.mycat.monitor.LogEntryHolder;
import io.mycat.monitor.MycatSQLLogMonitor;
import io.mycat.sqlhandler.SQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.ShardingSQLHandler;
import io.mycat.sqlhandler.dcl.*;
import io.mycat.sqlhandler.ddl.*;
import io.mycat.sqlhandler.dml.*;
import io.mycat.sqlhandler.dql.*;
import io.mycat.util.SqlTypeUtil;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.*;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public enum MycatdbCommand {
    /**/
    INSTANCE;
    final static Logger logger = LoggerFactory.getLogger(MycatdbCommand.class);
    static final ImmutableClassToInstanceMap<SQLHandler> sqlHandlerMap;

    static {
        ImmutableClassToInstanceMap.Builder<SQLHandler> builder = ImmutableClassToInstanceMap.builder();
        try {
            final HashSet<SQLHandler> sqlHandlers = new HashSet<>();
            sqlHandlers.add(new ShardingSQLHandler());
            sqlHandlers.add(new InsertSQLHandler());
            sqlHandlers.add(new DeleteSQLHandler());
            sqlHandlers.add(new UpdateSQLHandler());
            sqlHandlers.add(new TruncateSQLHandler());
            sqlHandlers.add(new ReplaceSQLHandler());
            sqlHandlers.add(new SetSQLHandler());
            sqlHandlers.add(new CommitSQLHandler());
            sqlHandlers.add(new KillSQLHandler());
            sqlHandlers.add(new RollbackSQLHandler());
            sqlHandlers.add(new SavepointSQLHandler());
            sqlHandlers.add(new ReleaseSavepointSQLHandler());
            sqlHandlers.add(new SetTransactionSQLHandler());
            sqlHandlers.add(new StartTransactionSQLHandler());
            sqlHandlers.add(HintHandler.INSTANCE);
            sqlHandlers.add(new UseSQLHandler());
            sqlHandlers.add(new LoadDataInFileSQLHandler());

            sqlHandlers.add(new AlterDatabaseSQLHandler());
            sqlHandlers.add(new AlterTableSQLHandler());
            sqlHandlers.add(new CreateDatabaseSQLHandler());
            sqlHandlers.add(new CreateIndexSQLHandler());
            sqlHandlers.add(new CreateTableSQLHandler());
            sqlHandlers.add(new CreateViewSQLHandler());
            sqlHandlers.add(new DropDatabaseSQLHandler());
            sqlHandlers.add(new DropTableSQLHandler());
            sqlHandlers.add(new DropViewSQLHandler());
            sqlHandlers.add(new RenameTableSQLHandler());
            sqlHandlers.add(new ShowCreateDatabaseHandler());
            //explain
            sqlHandlers.add(new ExplainSQLHandler());

            //show
            sqlHandlers.add(new ShowCharacterSetSQLHandler());
            sqlHandlers.add(new ShowCollationSQLHandler());
            sqlHandlers.add(new ShowColumnsSQLHandler());
            sqlHandlers.add(new ShowCreateTableSQLHandler());
            sqlHandlers.add(new ShowDatabasesHanlder());
            sqlHandlers.add(new ShowPluginsSQLHandler());

            sqlHandlers.add(new ShowEnginesSQLHandler());
            sqlHandlers.add(new ShowErrorsSQLHandler());
            sqlHandlers.add(new ShowIndexesSQLHandler());
            sqlHandlers.add(new ShowProcedureStatusSQLHandler());
            sqlHandlers.add(new ShowProcessListSQLHandler());
            sqlHandlers.add(new ShowStatusSQLHandler());
            sqlHandlers.add(new ShowTablesSQLHandler());
            sqlHandlers.add(new ShowTableStatusSQLHandler());
            sqlHandlers.add(new ShowTriggersSQLHandler());
            sqlHandlers.add(new ShowVariantsSQLHandler());
            sqlHandlers.add(new ShowWarningsSQLHandler());
            sqlHandlers.add(new ShowCreateFunctionHanlder());
            sqlHandlers.add(new CreateTableSQLHandler());
            sqlHandlers.add(new CreateSequenceHandler());
            sqlHandlers.add(new DropSequenceSQLHandler());
            sqlHandlers.add(new ShowFunctionStatusHandler());
            //Analyze
            sqlHandlers.add(new AnalyzeHanlder());
            sqlHandlers.add(new DropIndexSQLHandler());
            sqlHandlers.add(new MySQLCheckHandler());
            sqlHandlers.add(new ShowStatisticHandler());
            sqlHandlers.add(new MysqlShowDatabaseStatusHandler());
            //DDL
            sqlHandlers.add(new SQLDropFunctionHandler());
            sqlHandlers.add(new SQLCreateFunctionHandler());
            //Procedure
            sqlHandlers.add(new SQLCreateProcedureHandler());
            sqlHandlers.add(new SQLCallStatementHandler());
            sqlHandlers.add(new SQLDropProcedureHandler());

            for (SQLHandler sqlHandler : sqlHandlers) {
                Class statementClass = sqlHandler.getStatementClass();
                Objects.requireNonNull(statementClass);
                builder.put(statementClass, sqlHandler);
            }

        } catch (Throwable e) {
            logger.error("", e);
        }
        sqlHandlerMap = builder.build();
    }

    MycatdbCommand() {

    }

    public Future<Void> executeQuery(String text,
                                     MycatDataContext dataContext,
                                     IntFunction<Response> responseFactory) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(text);
            }
            if (text.charAt(0) == 'B') {
                //baseline
                Response response = responseFactory.apply(1);
                return handleBaseline(text, dataContext, response);
            }
            LinkedList<SQLStatement> statements = parse(text);
            if (statements.isEmpty()) {
                throw new MycatException("Illegal syntax:" + text);
            }
            Response response = responseFactory.apply(statements.size());
            Future<Void> future = Future.succeededFuture();
            for (SQLStatement sqlStatement : statements) {
                future = future.flatMap(new Function<Void, Future<Void>>() {
                    @Override
                    @SneakyThrows
                    public Future<Void> apply(Void unused) {
                        return execute(dataContext, response, sqlStatement);
                    }
                });
            }
            return future;
        } catch (Throwable e) {
            Response response = responseFactory.apply(1);
            if (isNavicatClientStatusQuery(text)) {
                return response.proxySelectToPrototype(text);
            }
            return Future.failedFuture(e);
        }
    }

    private Future<Void> handleBaseline(String text, MycatDataContext dataContext, Response response) {
        QueryPlanCache queryPlanCache = MetaClusterCurrent.wrapper(QueryPlanner.class).getPlanCache();
        if (text.startsWith("BASELINE ")) {
            text = text.substring("BASELINE".length()).trim();
            boolean fix = false;
            if (text.startsWith("ADD") || (fix = text.startsWith("FIX"))) {
                text = text.substring("ADD".length()).trim();

                SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(text);
                DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sqlStatement, dataContext.getDefaultSchema());
                PlanResultSet planResultSet = queryPlanCache.add(fix, drdsSqlWithParams);
                //BASELINE_ID | STATUS
                ResultSetBuilder builder = ResultSetBuilder.create();
                builder.addColumnInfo("BASELINE_ID", JDBCType.VARCHAR);
                builder.addColumnInfo("STATUS", JDBCType.VARCHAR);
                builder.addObjectRowPayload(Arrays.asList(String.valueOf(planResultSet.getBaselineId()), String.valueOf(planResultSet.isOk() ? "OK" : "ERROR")));
                return response.sendResultSet(builder.build());
            }
            if (text.startsWith("LIST") || text.equalsIgnoreCase("showAllPlans") || text.startsWith("ALL")) {
                ResultSetBuilder builder = ResultSetBuilder.create();
                builder.addColumnInfo("BASELINE_ID", JDBCType.VARCHAR)
                        .addColumnInfo("PARAMETERIZED_SQL", JDBCType.VARCHAR)
                        .addColumnInfo("PLAN_ID", JDBCType.VARCHAR)
                        .addColumnInfo("EXTERNALIZED_PLAN", JDBCType.VARCHAR)
                        .addColumnInfo("FIXED", JDBCType.VARCHAR)
                        .addColumnInfo("ACCEPTED", JDBCType.VARCHAR);
                for (Baseline baseline : queryPlanCache.list()) {
                    for (BaselinePlan baselinePlan : baseline.getPlanList()) {
                        String BASELINE_ID = String.valueOf(baselinePlan.getBaselineId());
                        String PARAMETERIZED_SQL = String.valueOf(baselinePlan.getSql());
                        String PLAN_ID = String.valueOf(baselinePlan.getId());
                        String EXTERNALIZED_PLAN = "";
                        try {
                            CodeExecuterContext attach = (CodeExecuterContext) baselinePlan.attach();
                            EXTERNALIZED_PLAN = new PlanImpl(attach.getMycatRel(), attach, Collections.emptyList()).dumpPlan();
                        } catch (Throwable throwable) {
                            logger.error("", throwable);
                        }
                        String FIXED = Optional.ofNullable(baseline.getFixPlan()).filter(i -> i.getId() == baselinePlan.getId())
                                .map(u -> "true").orElse("false");
                        String ACCEPTED = "true";

                        builder.addObjectRowPayload(Arrays.asList(BASELINE_ID, PARAMETERIZED_SQL, PLAN_ID, EXTERNALIZED_PLAN, FIXED, ACCEPTED));
                    }
                }
                return response.sendResultSet(() -> builder.build());
            }
            if (text.startsWith("LOAD_PLAN")) {
                text = text.substring("LOAD_PLAN".length()).trim();
                queryPlanCache.loadPlan(Long.parseLong(text));
                return response.sendOk();
            }
            if (text.startsWith("LOAD_ALL_BASELINES")) {
                queryPlanCache.loadBaselines();
                return response.sendOk();
            }
            if (text.startsWith("LOAD")) {
                text = text.substring("LOAD".length()).trim();
                queryPlanCache.loadBaseline(Long.parseLong(text));
                return response.sendOk();
            }
            if (text.startsWith("PERSIST_ALL_BASELINES")) {
                queryPlanCache.saveBaselines();
                return response.sendOk();
            }
            if (text.startsWith("PERSIST_PLAN")) {
                text = text.substring("PERSIST_PLAN".length()).trim();
                queryPlanCache.persistPlan(Long.parseLong(text));
                return response.sendOk();
            }
            if (text.startsWith("PERSIST")) {
                text = text.substring("PERSIST".length()).trim();
                queryPlanCache.persistBaseline(Long.parseLong(text));
                return response.sendOk();
            }
            if (text.startsWith("CLEAR_PLAN")) {
                text = text.substring("CLEAR_PLAN".length()).trim();
                queryPlanCache.clearPlan(Long.parseLong(text));
                return response.sendOk();
            }
            if (text.startsWith("CLEAR")) {
                text = text.substring("CLEAR".length()).trim();
                queryPlanCache.clearBaseline(Long.parseLong(text));
                return response.sendOk();
            }
            if (text.startsWith("DELETE_PLAN")) {
                text = text.substring("DELETE_PLAN".length()).trim();
                queryPlanCache.deletePlan(Long.parseLong(text));
                return response.sendOk();
            }
            if (text.startsWith("DELETE")) {
                text = text.substring("DELETE".length()).trim();
                queryPlanCache.deleteBaseline(Long.parseLong(text));
                return response.sendOk();
            }
            if (text.startsWith("UNFIX")) {
                text = text.substring("UNFIX".length()).trim();
                queryPlanCache.unFix(Long.parseLong(text));
                return response.sendOk();
            }

        }
        return Future.failedFuture("unknown baseline cmd " + text);
    }

    private static boolean isNavicatClientStatusQuery(String text) {
        if (Objects.equals(
                "SELECT STATE AS `状态`, ROUND(SUM(DURATION),7) AS `期间`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS `百分比` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID= GROUP BY STATE ORDER BY SEQ",
                text)) {
            return true;
        }
        if (Objects.equals("SHOW /*!50002 GLOBAL */ STATUS", text)) {
            return true;
        }
        return false;
    }

    @NotNull
    private static Map<String, Object> getHintRoute(SQLStatement sqlStatement) {
        List<SQLHint> hints = new LinkedList<>();
        MySqlASTVisitorAdapter mySqlASTVisitorAdapter = new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLSelect x) {
                hints.addAll(x.getHints());
                hints.addAll(x.getFirstQueryBlock().getHints());
                return false;
            }
        };

        sqlStatement.accept(mySqlASTVisitorAdapter);
        hints.addAll(Optional.ofNullable(sqlStatement.getHeadHintsDirect()).orElse(Collections.emptyList()));
        for (SQLHint hint : hints) {
            MycatHint mycatHint = null;
            if (hint instanceof SQLCommentHint) {
                String text = ((SQLCommentHint) hint).getText();
                mycatHint = new MycatHint(text);
            }
            if (mycatHint != null) {
                HashMap<String, Object> map = new HashMap<>();
                map.put("REP_BALANCE_TYPE", ReplicaBalanceType.NONE);
                for (MycatHint.Function function : mycatHint.getFunctions()) {
                    String name = function.getName();
                    switch (name.toUpperCase()) {
                        case "MERGE_UNION_SIZE": {
                            MycatHint.Argument argument = function.getArguments().get(0);
                            SQLNumericLiteralExpr value = (SQLNumericLiteralExpr) argument.getValue();
                            map.put("MERGE_UNION_SIZE", value.getNumber());
                        }
                        case "EXECUTE_TIMEOUT": {
                            MycatHint.Argument argument = function.getArguments().get(0);
                            SQLNumericLiteralExpr value = (SQLNumericLiteralExpr) argument.getValue();
                            long time = value.getNumber().longValue();//milliseconds毫秒
                            map.put("EXECUTE_TIMEOUT", time);
                            continue;
                        }
                        case "MASTER": {
                            map.put("REP_BALANCE_TYPE", ReplicaBalanceType.MASTER);
                            continue;
                        }
                        case "SLAVE": {
                            map.put("REP_BALANCE_TYPE", ReplicaBalanceType.SLAVE);
                            continue;
                        }
                        case "INDEX": {
                            map.put("INDEX", function.getArguments().stream().map(i -> i.toString()).collect(Collectors.toList()));
                            continue;
                        }
//                        case "SCAN": {
//                            List<MycatHint.Argument> arguments = function.getArguments();
//                            Map<String, List<String>> collect = arguments.stream().collect(Collectors.groupingBy(m -> SQLUtils.toSQLString(m.getName()),
//                                    Collectors.mapping(v -> SQLUtils.toSQLString(v.getValue()), Collectors.toList())));
//                            NameMap<List<String>> nameMap = NameMap.immutableCopyOf(collect);
//                            List<String> condition = Optional.ofNullable(nameMap.get("CONDITION", false)).orElse(Collections.emptyList());
//                            List<String> logicalTables = Optional.ofNullable(nameMap.get("TABLE", false)).orElse(Collections.emptyList());
//                            List<String> physicalTables = Optional.ofNullable(nameMap.get("PHYSICAL_TABLE", false)).orElse(Collections.emptyList());
//                            List<String> targets = Optional.ofNullable(nameMap.get("TARGET", false)).orElse(Collections.emptyList());
//                            map.put("SCAN_TABLE", logicalTables);
//                            map.put("SCAN_DATANODE", physicalTables);
//                            map.put("SCAN_TARGET", targets.stream().flatMap(i-> Arrays.stream(i.split(",")).map(j->SQLUtils.normalize(j.toString()))).collect(Collectors.toList()));
//                            map.put("SCAN_CONDITION", condition);
//                            continue;
//                        }
                        case "DATANODE":
                        case "TARGET": {
                            List<MycatHint.Argument> arguments = function.getArguments();
                            map.put("TARGET", arguments.stream().map(i -> SQLUtils.toSQLString(i.getValue())).collect(Collectors.toList()));
                            continue;
                        }
                        case "SCHEMA": {
                            List<MycatHint.Argument> arguments = function.getArguments();
                            arguments.stream().map(i -> SQLUtils.toSQLString(i.getValue())).findFirst()
                                    .ifPresent(s -> map.put("SCHEMA", SQLUtils.normalize(s)));
                            continue;
                        }
                    }
                }
                return map;
            }
        }
        return Collections.emptyMap();
    }

    public static Future<Void> execute(MycatDataContext dataContext, Response receiver, SQLStatement sqlStatement) {


        String sql = sqlStatement.toString();
        SQLType sqlType = SQLParserUtils.getSQLType(sql, DbType.mysql);
        MycatSQLLogMonitor logMonitor = MetaClusterCurrent.wrapper(MycatSQLLogMonitor.class);
        //////////////////////////////////////////////////////////////////////////////////////
        dataContext.putProcessStateMap(Collections.emptyMap());
        sqlStatement.setAfterSemi(false);//remove semi
        boolean existSqlResultSetService = MetaClusterCurrent.exist(SqlResultSetService.class);
        //////////////////////////////////apply transaction///////////////////////////////////
        TransactionSession transactionSession = dataContext.getTransactionSession();
        Future future = transactionSession.openStatementState();
        LogEntryHolder logRecord = logMonitor.startRecord(dataContext, null, sqlType, sql);
        future = future.flatMap(unused -> {
            try {
                //////////////////////////////////////////////////////////////////////////////////////
                if (existSqlResultSetService && !transactionSession.isInTransaction() && sqlStatement instanceof SQLSelectStatement) {
                    SqlResultSetService sqlResultSetService
                            = MetaClusterCurrent.wrapper(SqlResultSetService.class);
                    Optional<Observable<MysqlPayloadObject>> baseIteratorOptional = sqlResultSetService.get((SQLSelectStatement) sqlStatement);
                    if (baseIteratorOptional.isPresent()) {
                        return receiver.sendResultSet(baseIteratorOptional.get());
                    }
                }
                Map<String, Object> hintRoute = getHintRoute(sqlStatement);
                if (!hintRoute.isEmpty()) {
                    dataContext.putProcessStateMap(hintRoute);
                    Object targetArray = hintRoute.getOrDefault("TARGET", null);
                    if (targetArray != null) {
                        String sqlText = sqlStatement.toString();
                        boolean select = !SqlTypeUtil.isDml(sqlType);
                        if (!(targetArray instanceof List)) {
                            targetArray = Collections.singletonList(targetArray.toString());
                        }
                        if (select) {
                            return receiver.proxySelect((List) targetArray, sqlText);
                        }
                        return receiver.proxyUpdate((List) targetArray, sqlText);
                    }
                }
                SQLRequest<SQLStatement> request = new SQLRequest<>(sqlStatement);

                Class aClass = sqlStatement.getClass();
                SQLHandler instance = sqlHandlerMap.getInstance(aClass);
                if (instance != null) {
                    return instance.execute(request, dataContext, receiver);
                } else {
                    if (sqlStatement instanceof MySqlShowStatement) {
                        logger.warn("ignore SQL prototype statement:{}", sqlStatement);
                        return receiver.proxySelectToPrototype(sqlStatement.toString());
                    } else {
                        logger.warn("ignore SQL statement:{}", sqlStatement);
                        return receiver.sendOk();
                    }
                }
            } catch (Throwable throwable) {
                logger.error("",throwable);
                return Future.failedFuture(throwable);
            }
        });

        future = future.onComplete((Handler<AsyncResult>) event -> {
            if (event.succeeded()) {
                logRecord.recordSQLEnd(true, Collections.emptyMap(), "");
            } else {
                String localizedMessage = Optional.ofNullable(event.cause()).map(i -> i.getLocalizedMessage()).orElse("");
                logRecord.recordSQLEnd(false, Collections.emptyMap(), localizedMessage);
            }
        });
        return future;
    }

    @NotNull
    public LinkedList<SQLStatement> parse(String text) {
        text = text.trim();
        LinkedList<SQLStatement> resStatementList = new LinkedList<>();
        if (text.startsWith("begin") || text.startsWith("BEGIN")) {
            SQLStartTransactionStatement sqlStartTransactionStatement = new SQLStartTransactionStatement();
            resStatementList.add(sqlStartTransactionStatement);
            text = text.substring("begin".length());
            text = text.trim();
            if (text.startsWith(";")) {
                text = text.substring(1);
            }
            text = text.trim();
            if (text.isEmpty()) {
                return resStatementList;
            }
        }
        return parseMySQLString(text, resStatementList);
    }

    @NotNull
    private String convertSql(String text, DbType type) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(text, type, true);
        List<SQLStatement> sqlStatements = parser.parseStatementList();
        StringBuilder out = new StringBuilder();
        SQLASTOutputVisitor outputVisitor = SQLUtils.createOutputVisitor(out, type);
        for (SQLStatement sqlStatement : sqlStatements) {
            sqlStatement.accept(outputVisitor);
        }
        return out.toString();
    }


    private LinkedList<SQLStatement> parseMySQLString(String text, LinkedList<SQLStatement> statementList) {
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(text, DbType.mysql, true);
        parser.parseStatementList(statementList, -1, null);
        return statementList;
    }

}