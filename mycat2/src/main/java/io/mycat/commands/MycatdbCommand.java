package io.mycat.commands;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.client.MycatRequest;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLHandler;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.sqlHandler.dcl.*;
import io.mycat.sqlHandler.ddl.*;
import io.mycat.sqlHandler.dml.*;
import io.mycat.sqlHandler.dql.*;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.ContextExecuter;
import io.mycat.util.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
/**
 * @author Junwen Chen
 **/
public enum MycatdbCommand implements MycatCommand {
    INSTANCE;
    final static Logger logger = LoggerFactory.getLogger(MycatdbCommand.class);
    final Collection<SQLHandler> sqlHandlers = new ArrayList<>();

    MycatdbCommand() {
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
        sqlHandlers.add(new HintSQLHandler());
        sqlHandlers.add(new ShowDatabasesHanlder());
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

        //Analyze
        sqlHandlers.add(new AnalyzeHanlder());
    }

    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        executeQuery(request, context, response);
        return true;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        MycatDBClientMediator client = MycatDBs.createClient(context);
        try {
            SQLStatement statement = SQLUtils.parseSingleMysqlStatement(request.getText());
            statement.accept(new ContextExecuter(client.sqlContext()));
            for (SQLHandler sqlHandler : sqlHandlers) {
                ExecuteCode executeCode = sqlHandler.explain(new SQLRequest<>(statement, client.sqlContext(), request), context, response);
                if (executeCode == ExecuteCode.PERFORMED) {
                    return true;
                }
            }
            response.sendError(new MycatException("no support query. sql={} class={}", request.getText(), statement.getClass()));
        } catch (Throwable e) {
            boolean isRun = false;
            try {
                final String finalSql = request.getText().trim();
                if (finalSql.startsWith("execute ")) {
                    response.sendResultSet(()->client.executeRel(finalSql), () -> client.explainRel(finalSql));
                    isRun = true;
                }
            } catch (Throwable e1) {
                logger.error("", e1);
            } finally {
                if (!isRun) {
                    response.sendError(e);
                }
            }
            return true;
        }
        return true;
    }

    @Override
    public String getName() {
        return "mycatdb";
    }

    private void executeQuery(MycatRequest req, MycatDataContext dataContext, Response receiver) {
        int totalSqlMaxCode = 0;
        MycatDBClientMediator db = MycatDBs.createClient(dataContext);
        SQLStatement statement = null;
        try {
            String text = req.getText();
            Iterator<SQLStatement> iterator = parse(text);
            while (iterator.hasNext()) {
                statement = iterator.next();
                statement.accept(new ContextExecuter(db.sqlContext()));
                receiver.setHasMore(iterator.hasNext());
                SQLRequest<SQLStatement> request = new SQLRequest<>(statement, db.sqlContext(), req);
                try {
                    ExecuteCode executeCode = ExecuteCode.NOT_PERFORMED;
                    for (SQLHandler sqlHandler : sqlHandlers) {
                        executeCode = sqlHandler.execute(request, dataContext, receiver);
                        if (executeCode == ExecuteCode.PERFORMED) {
                            return;
                        }
                    }
                } catch (Throwable e) {
                    receiver.sendError(e);
                    return;
                } finally {
                    iterator.remove();//help gc
                }
            }
            if (!(statement instanceof com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExecuteStatement)) {
                logger.error("no support query. sql={} class={}", req.getText(), statement.getClass());
                receiver.proxyShow(statement);
            }else {
                throw new RuntimeException("may be hbt");
            }
        } catch (Throwable e) {
            boolean isRun = false;
            try {
                String trim = req.getText().trim();
                String pre = "execute plan ";
                if (trim.toLowerCase().startsWith(pre)) {
                    final String finalSql = trim.substring(pre.length());
                    receiver.sendResultSet(()->db.executeRel(finalSql), () -> db.explainRel(finalSql));
                    isRun = true;
                }
            } catch (Throwable e1) {
                logger.error("", e1);
            } finally {
                if (!isRun) {
                    receiver.sendError(e);
                }
            }
            return;
        }
    }

    @NotNull
    private Iterator<SQLStatement> parse(String text) {
        text = text.trim();
        if (text.startsWith("begin") || text.startsWith("BEGIN")) {
            SQLStartTransactionStatement sqlStartTransactionStatement = new SQLStartTransactionStatement();
            return new Iterator<SQLStatement>() {
                boolean hasNext = true;

                @Override
                public void remove() {

                }

                @Override
                public boolean hasNext() {
                    try {
                        return hasNext;
                    } finally {
                        hasNext = false;
                    }
                }

                @Override
                public SQLStatement next() {
                    return sqlStartTransactionStatement;
                }
            };
        }
        SQLStatementParser parser = SQLParserUtils.createSQLStatementParser(text, DbType.mysql, false);
        LinkedList<SQLStatement> statementList = new LinkedList<SQLStatement>();
        parser.parseStatementList(statementList, -1, null);
        return statementList.iterator();
    }

}