package io.mycat.commands;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.druid.sql.parser.SQLParserUtils;
import com.alibaba.druid.sql.parser.SQLStatementParser;
import com.alibaba.druid.sql.visitor.SQLASTOutputVisitor;
import com.google.common.collect.ImmutableClassToInstanceMap;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.DefaultDatasourceFactory;
import io.mycat.calcite.ExecutorImplementor;
import io.mycat.calcite.ResponseExecutorImplementor;
import io.mycat.calcite.executor.TempResultSetFactoryImpl;
import io.mycat.sqlhandler.SQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.ShardingSQLHandler;
import io.mycat.sqlhandler.dcl.*;
import io.mycat.sqlhandler.ddl.*;
import io.mycat.sqlhandler.dml.*;
import io.mycat.sqlhandler.dql.*;
import io.mycat.sqlrecorder.SqlRecord;
import io.mycat.Response;
import io.mycat.util.VertxUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.impl.future.PromiseInternal;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
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

    public PromiseInternal<Collection<AsyncResult<Void>>> executeQuery(String text,
                                                                 MycatDataContext dataContext,
                                                                 IntFunction<Response> responseFactory) {
        PromiseInternal<Collection<AsyncResult<Void>> > promise = VertxUtil.newPromise();
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(text);
            }
            LinkedList<SQLStatement> statements = parse(text);
            Response response = responseFactory.apply(statements.size());

            Collection<AsyncResult<Void>> resultList = new ConcurrentLinkedQueue<>();
            int totalCount = statements.size();
            for (SQLStatement sqlStatement : statements) {
                try {
                    SqlRecord sqlRecord = dataContext.startSqlRecord();
                    sqlRecord.setTarget(dataContext.getUser().getHost());
                    sqlRecord.setSql(sqlStatement);
                    PromiseInternal<Void> execute = execute(dataContext, response, sqlStatement);
                    execute.onComplete(e->{
                        dataContext.getTransactionSession().closeStatenmentState();
                        resultList.add(e);
                        if(e.failed()){
                            promise.tryFail(e.cause());
                        }else if(resultList.size() == totalCount){
                            promise.tryComplete(resultList);
                        }
                    });
                }catch (Exception e){
                    promise.tryFail(e);
                }
            }
            if(resultList.size() == totalCount){
                promise.tryComplete(resultList);
            }
            return promise;
        } catch (Throwable e) {
            Response response = responseFactory.apply(1);
            if (isNavicatClientStatusQuery(text)) {
                PromiseInternal<Void> promiseInternal = response.sendOk();
                promiseInternal.onComplete(o-> promise.tryComplete(Collections.singletonList(o)));
                return promise;
            }
            promise.tryFail(e);
            return promise;
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

    public static PromiseInternal<Void> execute(MycatDataContext dataContext, Response receiver, SQLStatement sqlStatement) throws Exception {
        boolean existSqlResultSetService = MetaClusterCurrent.exist(SqlResultSetService.class);

        //////////////////////////////////apply transaction///////////////////////////////////
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.openStatementState();
        //////////////////////////////////////////////////////////////////////////////////////
        if (existSqlResultSetService && !transactionSession.isInTransaction() && sqlStatement instanceof SQLSelectStatement) {
            SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
            Optional<RowBaseIterator> baseIteratorOptional = sqlResultSetService.get((SQLSelectStatement) sqlStatement);
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
            return receiver.proxySelectToPrototype(sqlStatement.toString());
        }
    }

    private static boolean isHbt(String text) {
        boolean hbt = false;
        char c = text.charAt(0);
        if ((c == 'e' || c == 'E') && text.length() > 12) {
            hbt = "execute plan".equalsIgnoreCase(text.substring(0, 12));
        }
        return hbt;
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
            if (text.isEmpty()){
                return resStatementList;
            }
        }
        return parseMySQLString(text, resStatementList);
    }

    @NotNull
    private String convertSql(String text,DbType type) {
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