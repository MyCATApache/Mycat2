package io.mycat.vertxmycat;

import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ProxyReactorThread;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.util.VertxUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.sqlclient.Query;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.impl.command.QueryCommandBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collector;

public class RowSetQuery implements Query<RowSet<Row>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(RowSetQuery.class);
    private final String sql;
    private final AbstractMySqlConnectionImpl mySQLClientSession;

    public RowSetQuery(String sql, AbstractMySqlConnectionImpl mySQLClientSession) {
        this.sql = sql;
        this.mySQLClientSession = mySQLClientSession;
        if (LOGGER.isDebugEnabled()){
            LOGGER.debug("RowSetQuery sessionId:{}",mySQLClientSession.mySQLClientSession.sessionId());
        }
    }

    @Override
    public void execute(Handler<AsyncResult<RowSet<Row>>> handler) {
        Future<RowSet<Row>> future = execute();
        if (future != null) {
            future.onComplete(handler);
        }
    }

    @Override
    public Future<RowSet<Row>> execute() {
        VertxRowSetImpl vertxRowSet = new VertxRowSetImpl();
        StreamMysqlCollector streamMysqlCollector = new StreamMysqlCollector() {
            @Override
            public void onColumnDefinitions(MySQLRowDesc columnDefinitions) {
                vertxRowSet.setColumnDescriptor(columnDefinitions.columnDescriptor());
            }

            @Override
            public void onRow(Row row) {
                vertxRowSet.list.add(row);
            }

        };
        return runTextQuery(sql, mySQLClientSession.mySQLClientSession, streamMysqlCollector)
                .map(voidSqlResult -> vertxRowSet);
    }


    @Override
    public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collectorArg) {
        return new SqlResultCollectingQuery(sql, mySQLClientSession, collectorArg);
    }

    @Override
    public <U> Query<RowSet<U>> mapping(Function<Row, U> mapper) {
        throw new UnsupportedOperationException();
    }

    public static PromiseInternal<SqlResult<Void>> runTextQuery(String curSql,
                                    MySQLClientSession mySQLClientSession,
                                    Collector collectorArg) {
      return   runTextQuery(curSql, mySQLClientSession, (StreamMysqlCollector) collectorArg);
    }


    public static PromiseInternal<SqlResult<Void>> runTextQuery(String curSql,
                                                                MySQLClientSession mySQLClientSession,
                                                                StreamMysqlCollector collectorArg) {
        PromiseInternal<SqlResult<Void>> promise = VertxUtil.newPromise();
        if (mySQLClientSession.getIOThread() == Thread.currentThread()) {
            VertxMycatTextCollector<Object, Object> resultSetHandler = new VertxMycatTextCollector<Object, Object>((Collector) collectorArg);
           if (LOGGER.isDebugEnabled()){
               LOGGER.debug("session id:{} sql:{}", mySQLClientSession.sessionId(), curSql,new Throwable());
           }
            resultSetHandler.request(mySQLClientSession, MySQLCommandType.COM_QUERY, curSql.getBytes(),
                    new ResultSetCallBack<MySQLClientSession>() {
                        @Override
                        public void onFinishedSendException(Exception exception, Object sender,
                                                            Object attr) {
                            MycatException mycatException = new MycatException(MySQLErrorCode.ER_UNKNOWN_ERROR, exception.getMessage());
                            LOGGER.error("session id:{} sql:{}", mySQLClientSession.sessionId(), curSql,mycatException);
                            promise.tryFail(mycatException);
                        }

                        @Override
                        public void onFinishedException(Exception exception, Object sender, Object attr) {
                            MycatException mycatException = new MycatException(MySQLErrorCode.ER_UNKNOWN_ERROR, exception.getMessage());
                            LOGGER.error("session id:{} sql:{}", mySQLClientSession.sessionId(), curSql,mycatException);
                            promise.tryFail(mycatException);
                        }

                        @Override
                        public void onFinished(boolean monopolize, MySQLClientSession mysql,
                                               Object sender, Object attr) {
                            if (LOGGER.isDebugEnabled()){
                                LOGGER.debug("onFinished session id:{} sql:{}", mySQLClientSession.sessionId(), curSql);
                            }
                            MySqlResult<Void> mySqlResult = new MySqlResult<>(
                                    resultSetHandler.getRowCount(),
                                    resultSetHandler.getAffectedRows(),
                                    resultSetHandler.getLastInsertId(), null,
                                    Optional.ofNullable(resultSetHandler.getRowResultDecoder())
                                            .map(i -> i.rowDesc)
                                            .map(i -> i.columnDescriptor())
                                            .orElse(Collections.emptyList())
                            );
                            promise.complete(mySqlResult);
                        }

                        @Override
                        public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                                                  MySQLClientSession mysql, Object sender, Object attr) {
                            MycatException mycatException = new MycatException(errorPacket.getErrorCode(), errorPacket.getErrorMessageString());
                            LOGGER.error("onErrorPacket session id:{} sql:{}", mySQLClientSession.sessionId(), curSql,mycatException);
                            promise.tryFail(mycatException);
                        }
                    });
        } else {
            mySQLClientSession.getIOThread().addNIOJob(new NIOJob() {
                @Override
                public void run(ReactorEnvThread reactor) throws Exception {
                    if (LOGGER.isDebugEnabled()){
                        LOGGER.error("nio job session id:{} sql:{}", mySQLClientSession.sessionId(), curSql,new Throwable());
                    }
                    runTextQuery(curSql, mySQLClientSession, collectorArg).onComplete(promise);
                }

                @Override
                public void stop(ReactorEnvThread reactor, Exception reason) {
                    promise.tryFail(reason);
                }

                @Override
                public String message() {
                    return "proxy query text result set";
                }
            });
        }
        return promise;
    }

}