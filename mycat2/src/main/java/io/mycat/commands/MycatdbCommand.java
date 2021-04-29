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
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.google.common.collect.ImmutableClassToInstanceMap;
import io.mycat.*;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.sqlhandler.SQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.ShardingSQLHandler;
import io.mycat.sqlhandler.dcl.*;
import io.mycat.sqlhandler.ddl.*;
import io.mycat.sqlhandler.ddl.CreateSequenceHandler;
import io.mycat.sqlhandler.dml.*;
import io.mycat.sqlhandler.dql.*;
import io.mycat.sqlrecorder.SqlRecord;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.Function;
import java.util.function.IntFunction;

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
            sqlHandlers.add(new SetTransactionSQLHandler());
            sqlHandlers.add(new StartTransactionSQLHandler());
            sqlHandlers.add(new HintHandler());
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

            //explain
            sqlHandlers.add(new ExplainSQLHandler());

            //show
            sqlHandlers.add(new ShowCharacterSetSQLHandler());
            sqlHandlers.add(new ShowCollationSQLHandler());
            sqlHandlers.add(new ShowColumnsSQLHandler());
            sqlHandlers.add(new ShowCreateTableSQLHandler());
            sqlHandlers.add(new ShowDatabasesHanlder());
            sqlHandlers.add(new ShowDatabaseSQLHandler());
            sqlHandlers.add(new ShowDatabaseStatusSQLHandler());
            sqlHandlers.add(new ShowEnginesSQLHandler());
            sqlHandlers.add(new ShowErrorsSQLHandler());
            sqlHandlers.add(new ShowIndexesSQLHandler());
            sqlHandlers.add(new ShowProcedureStatusSQLHandler());
            sqlHandlers.add(new ShowProcessListSQLHandler());
            sqlHandlers.add(new ShowStatusSQLHandler());
            sqlHandlers.add(new ShowTablesSQLHandler());
            sqlHandlers.add(new ShowTableStatusSQLHandler());
            sqlHandlers.add(new ShowVariantsSQLHandler());
            sqlHandlers.add(new ShowWarningsSQLHandler());
            sqlHandlers.add(new ShowCreateFunctionHanlder());
            sqlHandlers.add(new CreateTableSQLHandler());
            sqlHandlers.add(new CreateSequenceHandler());
            sqlHandlers.add(new DropSequenceSQLHandler());
            //Analyze
            sqlHandlers.add(new AnalyzeHanlder());
            sqlHandlers.add(new DropIndexSQLHandler());
            sqlHandlers.add(new MySQLCheckHandler());

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
                        SqlRecord sqlRecord = dataContext.startSqlRecord();
                        sqlRecord.setTarget(dataContext.getUser().getHost());
                        sqlRecord.setSql(sqlStatement);
                        return execute(dataContext, response, sqlStatement);
                    }
                });
            }
            return future;
        } catch (Throwable e) {
            Response response = responseFactory.apply(1);
            if (isNavicatClientStatusQuery(text)) {
                return response.sendOk();
            }
            return Future.failedFuture(e);
        }
    }

    private static boolean isNavicatClientStatusQuery(String text) {
        if (Objects.equals(
                "SELECT STATE AS `状态`, ROUND(SUM(DURATION),7) AS `期间`, CONCAT(ROUND(SUM(DURATION)/*100,3), '%') AS `百分比` FROM INFORMATION_SCHEMA.PROFILING WHERE QUERY_ID= GROUP BY STATE ORDER BY SEQ",
                text)) {
            return true;
        }
        return false;
    }

    public static Future<Void> execute(MycatDataContext dataContext, Response receiver, SQLStatement sqlStatement) {
        boolean existSqlResultSetService = MetaClusterCurrent.exist(SqlResultSetService.class);
        //////////////////////////////////apply transaction///////////////////////////////////
        TransactionSession transactionSession = dataContext.getTransactionSession();
        return transactionSession.openStatementState().flatMap(unused -> {
            //////////////////////////////////////////////////////////////////////////////////////
            if (existSqlResultSetService && !transactionSession.isInTransaction() && sqlStatement instanceof SQLSelectStatement) {
                SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
                Optional<Observable<MysqlPayloadObject>> baseIteratorOptional = sqlResultSetService.get((SQLSelectStatement) sqlStatement);
                if (baseIteratorOptional.isPresent()) {
                    return receiver.sendResultSet(baseIteratorOptional.get());
                }
            }
            SQLRequest<SQLStatement> request = new SQLRequest<>(sqlStatement);
            Class aClass = sqlStatement.getClass();
            SQLHandler instance = sqlHandlerMap.getInstance(aClass);
            if (instance != null) {
                return instance.execute(request, dataContext, receiver);
            } else {
                if (sqlStatement instanceof MySqlShowStatement) {
                    return receiver.proxySelectToPrototype(sqlStatement.toString());
                }else {
                    logger.warn("ignore SQL statement:{}",sqlStatement);
                    return receiver.sendOk();
                }
            }
        });
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