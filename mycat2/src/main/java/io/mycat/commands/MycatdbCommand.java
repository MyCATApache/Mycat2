package io.mycat.commands;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import com.alibaba.fastsql.sql.visitor.SQLASTOutputVisitor;
import com.google.common.collect.ImmutableClassToInstanceMap;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.hbt4.DefaultDatasourceFactory;
import io.mycat.hbt4.ExecutorImplementor;
import io.mycat.hbt4.ResponseExecutorImplementor;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.proxy.session.MycatSession;
import io.mycat.sqlhandler.SQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.sqlhandler.dcl.*;
import io.mycat.sqlhandler.ddl.*;
import io.mycat.sqlhandler.dml.*;
import io.mycat.sqlhandler.dql.*;
import io.mycat.util.Response;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

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
            sqlHandlers.add(new SelectSQLHandler());
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

    public void executeQuery(String text, MycatSession session, MycatDataContext dataContext) {
        try {
            if (logger.isDebugEnabled()) {
                logger.debug(text);
            }
            LinkedList<SQLStatement> statements = parse(text, dataContext);
            Response receiver;
            if (statements.size() == 1 && statements.get(0) instanceof MySqlExplainStatement) {
                receiver = new ReceiverImpl(session, statements.size(), false, false);
            } else {
                receiver = new ReceiverImpl(session, statements.size(), false, false);
            }
            for (SQLStatement sqlStatement : statements) {
                execute(dataContext, receiver, sqlStatement);
            }
        } catch (Throwable e) {
            if (isNavicatClientStatusQuery(text)) {
                session.writeOkEndPacket();
                return;
            }
            session.setLastMessage(e);
            session.writeErrorEndPacketBySyncInProcessError();
            return;
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

    public static void execute(MycatDataContext dataContext, Response receiver, SQLStatement sqlStatement) throws Exception {
        boolean existSqlResultSetService = MetaClusterCurrent.exist(SqlResultSetService.class);

        //////////////////////////////////apply transaction///////////////////////////////////
        TransactionSession transactionSession = dataContext.getTransactionSession();
        transactionSession.ensureTranscation();
        //////////////////////////////////////////////////////////////////////////////////////
        if (existSqlResultSetService && !transactionSession.isInTransaction() && sqlStatement instanceof SQLSelectStatement) {
            SqlResultSetService sqlResultSetService = MetaClusterCurrent.wrapper(SqlResultSetService.class);
            Optional<RowBaseIterator> baseIteratorOptional = sqlResultSetService.get((SQLSelectStatement) sqlStatement);
            if (baseIteratorOptional.isPresent()) {
                receiver.sendResultSet(baseIteratorOptional.get());
                return;
            }
        }
        SQLRequest<SQLStatement> request = new SQLRequest<>(sqlStatement);
        Class aClass = sqlStatement.getClass();
        SQLHandler instance = sqlHandlerMap.getInstance(aClass);
        if (instance != null) {
            instance.execute(request, dataContext, receiver);
        } else {
            receiver.tryBroadcastShow(sqlStatement.toString());
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

    @SneakyThrows
    private static void executeHbt(MycatDataContext dataContext, String substring, Response receiver) {
        try (DefaultDatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext)) {
            TempResultSetFactoryImpl tempResultSetFactory = new TempResultSetFactoryImpl();
            ExecutorImplementor executorImplementor = new ResponseExecutorImplementor(datasourceFactory, tempResultSetFactory, receiver);
            DrdsRunners.runHbtOnDrds(dataContext, substring, executorImplementor);
        }
    }

    @NotNull
    private LinkedList<SQLStatement> parse(String text, MycatDataContext dataContext) {
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
        MycatUser user = dataContext.getUser();
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