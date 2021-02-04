package io.mycat.vertxmycat;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.MycatException;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.beans.mysql.packet.OkPacket;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.handler.backend.ResultSetHandler;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.util.VertxUtil;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.mysqlclient.MySQLClient;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.mysqlclient.impl.protocol.ColumnDefinition;
import io.vertx.sqlclient.*;
import io.vertx.sqlclient.desc.ColumnDescriptor;
import io.vertx.sqlclient.impl.command.QueryCommandBase;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class AbstractMySqlConnectionImpl extends AbstractMySqlConnection {
    static final Logger LOGGER = LoggerFactory.getLogger(AbstractMySqlConnectionImpl.class);
    volatile Handler<Throwable> exceptionHandler;
    volatile Handler<Void> closeHandler;
    final MySQLClientSession mySQLClientSession;

    public AbstractMySqlConnectionImpl(MySQLClientSession mySQLClientSession) {
        this.mySQLClientSession = mySQLClientSession;
    }

    @Override
    public Future<PreparedStatement> prepare(String sql) {
        return Future.succeededFuture(new PreparedStatement() {
            @Override
            public PreparedQuery<RowSet<Row>> query() {
                return AbstractMySqlConnectionImpl.this.preparedQuery(sql);
            }

            @Override
            public Cursor cursor(Tuple args) {
                throw new UnsupportedOperationException();
            }

            @Override
            public RowStream<Row> createStream(int fetch, Tuple args) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<Void> close() {
                return Future.succeededFuture();
            }

            @Override
            public void close(Handler<AsyncResult<Void>> completionHandler) {

            }
        });
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
        return new RowSetQuery(sql);
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
        return new AbstractMySqlPreparedQuery<RowSet<Row>>() {

            @Override
            public Future<RowSet<Row>> execute(Tuple tuple) {
                int size = tuple.size();
                List<Object> list = new ArrayList<>(size);
                for (int i = 0; i < tuple.size(); i++) {
                    list.add(tuple.getValue(i));
                }
                Query<RowSet<Row>> query = query(apply(sql, list));
                return query.execute();
            }

            @Override
            public Future<RowSet<Row>> executeBatch(List<Tuple> batch) {
                throw new UnsupportedOperationException();
            }

            @Override
            public <R> PreparedQuery<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {

                return new AbstractMySqlPreparedQuery<SqlResult<R>>() {

                    @Override
                    public Future<SqlResult<R>> execute(Tuple tuple) {
                        RowSetQuery rowSetQuery = new RowSetQuery(apply(sql, toObjects(tuple)));
                        Query<SqlResult<R>> collecting = rowSetQuery.collecting(collector);
                        return collecting.execute();
                    }

                    @Override
                    public Future<SqlResult<R>> executeBatch(List<Tuple> batch) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <R> PreparedQuery<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public <U> PreparedQuery<RowSet<U>> mapping(Function<Row, U> mapper) {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public Future<SqlResult<R>> execute() {
                        throw new UnsupportedOperationException();
                    }
                };
            }

            @Override
            public <U> PreparedQuery<RowSet<U>> mapping(Function<Row, U> mapper) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Future<RowSet<Row>> execute() {
                return query(sql).execute();
            }

        };
    }

    @NotNull
    private List<Object> toObjects(Tuple tuple) {
        int size = tuple.size();
        ArrayList<Object> objects = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            objects.add(tuple.getValue(i));
        }
        return objects;
    }

    private static void runTextQuery(String curSql,
                                     MySQLClientSession mySQLClientSession,
                                     Collector collectorArg) {
        runTextQuery(curSql, mySQLClientSession, (StreamMysqlCollector) collectorArg);
    }

    @NotNull
    private static void runTextQuery(String curSql,
                                     MySQLClientSession mySQLClientSession,
                                     StreamMysqlCollector collectorArg) {
        if (mySQLClientSession.getIOThread() == Thread.currentThread()) {

            VertxMycatTextCollector<Object, Object> resultSetHandler = new VertxMycatTextCollector<Object, Object>((Collector) collectorArg);
            LOGGER.debug("session id:{} sql:{}", mySQLClientSession.sessionId(), curSql);
            resultSetHandler.request(mySQLClientSession, MySQLCommandType.COM_QUERY, curSql.getBytes(),
                    new ResultSetCallBack<MySQLClientSession>() {
                        @Override
                        public void onFinishedSendException(Exception exception, Object sender,
                                                            Object attr) {
                            collectorArg.onError(MySQLErrorCode.ER_UNKNOWN_ERROR, exception.getMessage());
                        }

                        @Override
                        public void onFinishedException(Exception exception, Object sender, Object attr) {
                            collectorArg.onError(MySQLErrorCode.ER_UNKNOWN_ERROR, exception.getMessage());
                        }

                        @Override
                        public void onFinished(boolean monopolize, MySQLClientSession mysql,
                                               Object sender, Object attr) {
                            OkPacket okPacket = mysql.getBackendPacketResolver();
                            collectorArg.onFinish(0,
                                    okPacket.getServerStatus(), okPacket.getOkAffectedRows(), okPacket.getOkLastInsertId());
                        }

                        @Override
                        public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                                                  MySQLClientSession mysql, Object sender, Object attr) {
                            collectorArg.onError(errorPacket.getErrorCode(), errorPacket.getErrorMessageString());
                        }
                    });
        } else {
            mySQLClientSession.getIOThread().addNIOJob(new NIOJob() {
                @Override
                public void run(ReactorEnvThread reactor) throws Exception {
                    runTextQuery(curSql, mySQLClientSession, collectorArg);
                }

                @Override
                public void stop(ReactorEnvThread reactor, Exception reason) {
                    collectorArg.onError(MySQLErrorCode.ER_UNKNOWN_ERROR, reason.getMessage());
                }

                @Override
                public String message() {
                    return "proxy query text result set";
                }
            });
        }
    }

    @Override
    public Future<Void> close() {
        Promise<Void> promise = Promise.promise();
        Thread thread = Thread.currentThread();
        if (thread instanceof ReactorEnvThread) {
            try {
                mySQLClientSession.getSessionManager().addIdleSession(mySQLClientSession);
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
            } catch (Throwable throwable) {
                promise.fail(throwable);
            }
        } else {
            mySQLClientSession.getIOThread()
                    .addNIOJob(new NIOJob() {
                        @Override
                        public void run(ReactorEnvThread reactor) throws Exception {
                            Future<Void> close = close();
                            close.onComplete(event -> {
                                promise.tryComplete();
                            });
                        }

                        @Override
                        public void stop(ReactorEnvThread reactor, Exception reason) {
                            Future<Void> close = close();
                            close.onComplete(event -> promise.tryComplete());
                        }

                        @Override
                        public String message() {
                            return "closing " + mySQLClientSession;
                        }
                    });
        }
        return promise.future();
    }

    @NotNull
    private ResultSetCallBack<MySQLClientSession> commandResponse(Promise<Void> promise) {
        return new ResultSetCallBack<MySQLClientSession>() {
            @Override
            public void onFinishedSendException(Exception exception, Object sender,
                                                Object attr) {
                promise.fail(exception);
            }

            @Override
            public void onFinishedException(Exception exception, Object sender, Object attr) {
                promise.fail(exception);
            }

            @Override
            public void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender,
                                   Object attr) {
                promise.tryComplete();
            }

            @Override
            public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                                      MySQLClientSession mysql, Object sender, Object attr) {
                promise.fail(new MycatException(errorPacket.getErrorCode(), errorPacket.getErrorMessageString()));
            }
        };
    }

    private class RowSetQuery implements Query<RowSet<Row>> {
        private final String sql;
        ColumnDefinition[] columnDefinitionList;

        public RowSetQuery(String sql) {
            this.sql = sql;
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
            PromiseInternal<RowSet<Row>> promise = VertxUtil.newPromise();
            StreamMysqlCollector streamMysqlCollector = new StreamMysqlCollector() {
                private VertxRowSetImpl vertxRowSet = new VertxRowSetImpl();

                @Override
                public void onColumnDefinitions(MySQLRowDesc columnDefinitions, QueryCommandBase queryCommand) {
                    this.vertxRowSet.setColumnDescriptor(columnDefinitions.columnDescriptor());
                }

                @Override
                public void onRow(Row row) {
                    vertxRowSet.list.add(row);
                }

                @Override
                public void onFinish(int sequenceId, int serverStatusFlags, long affectedRows, long lastInsertId) {
                    vertxRowSet.setAffectRow(affectedRows);
                    vertxRowSet.setLastInsertId(lastInsertId);
                    promise.tryComplete(vertxRowSet);
                }

                @Override
                public void onError(int error, String errorMessage) {
                    promise.tryFail(new MycatException(error, errorMessage));
                }
            };


            runTextQuery(sql, mySQLClientSession, streamMysqlCollector);
            return promise.future();
        }


        @Override
        public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collectorArg) {
            return new Query<SqlResult<R>>() {
                @Override
                public void execute(Handler<AsyncResult<SqlResult<R>>> handler) {
                    Future<SqlResult<R>> future = execute();
                    if (future != null) {
                        future.onComplete(handler);
                    }
                }

                @Override
                public Future<SqlResult<R>> execute() {
                    VertxRowSetImpl<R> objects = new VertxRowSetImpl<>();
                    PromiseInternal<SqlResult<R>> promise = VertxUtil.newPromise();
                    new StreamMysqlCollector() {
                        private BiConsumer<Object, Row> accumulator;
                        private Object o;
                        private Function<Object, Object> finisher;
                        private int count;

                        @Override
                        public void onColumnDefinitions(MySQLRowDesc columnDefinitions, QueryCommandBase queryCommand) {
                            objects.setColumnDescriptor(columnDefinitions.columnDescriptor());
                            this.o = collectorArg.supplier().get();
                            this.accumulator = (BiConsumer) collectorArg.accumulator();
                            this.finisher = (Function) collectorArg.finisher();
                            if (collectorArg instanceof StreamMysqlCollector) {
                                StreamMysqlCollector collectorArg1 = (StreamMysqlCollector) collectorArg;
                                collectorArg1.onColumnDefinitions(columnDefinitions, queryCommand);
                            }
                        }

                        @Override
                        public void onRow(Row row) {
                            count++;
                            accumulator.accept(o, row);
                        }

                        @Override
                        public void onFinish(int sequenceId, int serverStatusFlags, long affectedRows, long lastInsertId) {
                            Object apply = this.finisher.apply(o);
                            if (collectorArg instanceof StreamMysqlCollector) {
                                StreamMysqlCollector collectorArg1 = (StreamMysqlCollector) collectorArg;
                                collectorArg1.onFinish(sequenceId, serverStatusFlags, affectedRows, lastInsertId);

                            }
                            promise.tryComplete(new SqlResult() {

                                @Override
                                public int rowCount() {
                                    return (int) affectedRows;
                                }

                                @Override
                                public List<String> columnsNames() {
                                    return objects.columnsNames();
                                }

                                @Override
                                public List<ColumnDescriptor> columnDescriptors() {
                                    return objects.columnDescriptors();
                                }

                                @Override
                                public int size() {
                                    return count;
                                }

                                @Override
                                public Object value() {
                                    return apply;
                                }

                                @Override
                                public SqlResult next() {
                                    return null;
                                }

                                @Override
                                public Object property(PropertyKind propertyKind) {
                                    if (MySQLClient.LAST_INSERTED_ID == propertyKind) {
                                        Object lastInsertId1 = lastInsertId;
                                        return lastInsertId1;
                                    }
                                    return null;
                                }
                            });
                        }

                        @Override
                        public void onError(int error, String errorMessage) {
                            if (collectorArg instanceof StreamMysqlCollector) {
                                StreamMysqlCollector collectorArg1 = (StreamMysqlCollector) collectorArg;
                                collectorArg1.onError(error, errorMessage);
                            }
                            promise.fail(errorMessage);
                        }
                    };
                    runTextQuery(sql, mySQLClientSession,
                            collectorArg);
                    return promise.future();
                }

                @Override
                public <R> Query<SqlResult<R>> collecting(Collector<Row, ?, R> collector) {
                    throw new UnsupportedOperationException();
                }

                @Override
                public <U> Query<RowSet<U>> mapping(Function<Row, U> mapper) {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public <U> Query<RowSet<U>> mapping(Function<Row, U> mapper) {
            throw new UnsupportedOperationException();
        }
    }
}
