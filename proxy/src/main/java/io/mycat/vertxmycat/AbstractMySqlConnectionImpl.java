package io.mycat.vertxmycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.ResultSetHandler;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.sqlclient.*;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class AbstractMySqlConnectionImpl extends AbstractMySqlConnection {
    static final Logger LOGGER = LoggerFactory.getLogger(AbstractMySqlConnectionImpl.class);
    volatile Handler<Throwable> exceptionHandler;
    volatile Handler<Void> closeHandler;
    volatile MySQLClientSession mySQLClientSession;

    public AbstractMySqlConnectionImpl(MySQLClientSession mySQLClientSession) {
        this.mySQLClientSession = mySQLClientSession;
    }

    @Override
    public Future<PreparedStatement> prepare(String sql) {
        return Future.succeededFuture(new MycatVertxPreparedStatement(sql, this));
    }

    @Override
    public MySQLConnection exceptionHandler(Handler<Throwable> handler) {
        exceptionHandler = handler;
        return this;
    }

    @Override
    public MySQLConnection closeHandler(Handler<Void> handler) {
        closeHandler = handler;
        return this;
    }

    @Override
    public Future<Void> ping() {
        Promise<Void> promise = Promise.promise();
        ResultSetHandler.DEFAULT
                .request(mySQLClientSession, MySQLCommandType.COM_PING, new byte[]{},
                        commandResponse(promise));
        return promise.future();
    }


    @Override
    public Future<Void> specifySchema(String schemaName) {
        Promise<Void> promise = Promise.promise();
        ResultSetHandler.DEFAULT
                .request(mySQLClientSession, MySQLCommandType.COM_INIT_DB, schemaName.getBytes(StandardCharsets.UTF_8),
                        commandResponse(promise));
        return promise.future();
    }


    @Override
    public Future<Void> resetConnection() {
        Promise<Void> promise = Promise.promise();
        ResultSetHandler.DEFAULT
                .request(mySQLClientSession, MySQLCommandType.COM_RESET_CONNECTION, new byte[]{},
                        commandResponse(promise));
        return promise.future();
    }

    @Override
    public Query<RowSet<Row>> query(String sql) {
        return new RowSetQuery(sql, this);
    }

    public static String apply(String parameterizedSql, List<Object> parameters) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(parameterizedSql);
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                SQLReplaceable parent = (SQLReplaceable) x.getParent();
                parent.replace(x, SQLExprUtils.fromJavaObject(parameters.get(x.getIndex())));
            }
        });
        return sqlStatement.toString();
    }

    @Override
    public PreparedQuery<RowSet<Row>> preparedQuery(String sql) {
        return new RowSetMySqlPreparedQuery(sql, this);
    }

    @NotNull
    public static List<Object> toObjects(Tuple tuple) {
        int size = tuple.size();
        ArrayList<Object> objects = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            objects.add(tuple.getValue(i));
        }
        return objects;
    }


    @Override
    public Future<Void> close() {
        if (mySQLClientSession == null) {
            return VertxUtil.newSuccessPromise();
        } else {
            Promise<Void> promise = Promise.promise();
            PromiseInternal<Void> close = mySQLClientSession.close(true, "close");
            mySQLClientSession = null;
            close.onComplete(event -> {
                if (closeHandler != null) {
                    try {
                        closeHandler.handle(null);
                    } catch (Throwable throwable) {
                        if (exceptionHandler != null) {
                            exceptionHandler.handle(throwable);
                        }
                    }
                }
                promise.tryComplete();
            });
            return promise.future();
        }
    }

    @NotNull
    private ResultSetCallBack<MySQLClientSession> commandResponse(Promise<Void> promise) {
        return new CommandResponse(promise);
    }

}
