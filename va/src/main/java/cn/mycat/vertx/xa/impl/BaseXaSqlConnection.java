/**
 * Copyright [2021] [chen junwen]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.ImmutableParticipantLog;
import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.State;
import cn.mycat.vertx.xa.XaLog;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.mysqlclient.MySQLConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlConnection;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;


public class BaseXaSqlConnection extends AbstractXaSqlConnection {
    private final static Logger LOGGER = LoggerFactory.getLogger(BaseXaSqlConnection.class);
    protected final ConcurrentHashMap<String, SqlConnection> map = new ConcurrentHashMap<>();
    protected final Map<SqlConnection, State> connectionState = Collections.synchronizedMap(new IdentityHashMap<>());
    private final Supplier<MySQLManager> mySQLManagerSupplier;
    protected volatile String xid;


    public BaseXaSqlConnection(Supplier<MySQLManager> mySQLManagerSupplier, XaLog xaLog) {
        super(xaLog);
        this.mySQLManagerSupplier = mySQLManagerSupplier;
    }

    protected MySQLManager mySQLManager(){
        return mySQLManagerSupplier.get();
    }


    private String getDatasourceName(SqlConnection connection) {
        return map.entrySet().stream().filter(p -> p.getValue() == connection).map(e -> e.getKey())
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("unknown connection " + connection));
    }

    /**
     * 1.not allow Nested transaction,double begin.
     * 2.alloc unique xid
     *
     * @param handler the callback handler
     */
    public void begin(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            handler.handle(Future.failedFuture(new IllegalArgumentException("occur Nested transaction")));
            return;
        }
        inTranscation = true;
        xid = log.nextXid();
        log.beginXa(xid);
        handler.handle(Future.succeededFuture());
    }


    public Future<SqlConnection> getConnection(String targetName) {
        MySQLManager mySQLManager = mySQLManager();
        if (inTranscation) {
            if (map.containsKey(targetName)) {
                return Future.succeededFuture(map.get(targetName));
            } else {
                Future<SqlConnection> sqlConnectionFuture = mySQLManager.getConnection(targetName);

                return sqlConnectionFuture.compose(connection -> {
                    map.put(targetName, connection);
                    changeTo(connection, State.XA_INITED);
                    Future<RowSet<Row>> execute = connection.query(String.format(XA_START, xid)).execute();
                    return execute.map(r -> changeTo(connection, State.XA_STARTED));
                });
            }
        }
        return mySQLManager.getConnection(targetName);
    }

    /**
     *
     * <p>
     * XA_START to XA_END to XA_ROLLBACK
     * XA_ENDED to XA_ROLLBACK
     * XA_PREPARED to XA_ROLLBACK
     * <p>
     * client blocks until rollback successfully
     *
     * @param handler the callback handler
     */
    public void rollback(Handler<AsyncResult<Void>> handler) {
        logParticipants();
        Function<SqlConnection, Future<Object>> function = new Function<SqlConnection, Future<Object>>() {
            @Override
            public Future apply(SqlConnection c) {
                Future future = Future.succeededFuture();
                switch (connectionState.get(c)) {
                    case XA_INITED:
                        return future;
                    case XA_STARTED:
                        future = future.compose(unused -> {
                            return c.query(String.format(XA_END, xid)).execute()
                                    .map(u -> changeTo(c, State.XA_ENDED));
                        });
                    case XA_ENDED:
                    case XA_PREPARED:
                        future = future.compose(unuse -> c.query(String.format(XA_ROLLBACK, xid)).execute())
                                .map(u -> changeTo(c, State.XA_ROLLBACKED));
                }
                return future;
            }
        };
        executeAll((Function) function)
                .onComplete(event -> {
                    log.logRollback(xid, event.succeeded());
                    if (event.succeeded()) {
                        inTranscation = false;
                        clearConnections((Handler) handler);
                    } else {
                        retryRollback(handler, function);
                    }
                });
    }

    /**
     * retry has a delay time for datasource need duration to recover
     *
     * @param handler
     * @param function
     */
    private void retryRollback(Handler<AsyncResult<Void>> handler, Function<SqlConnection, Future<Object>> function) {
        MySQLManager mySQLManager = mySQLManager();
        List<Future<Object>> collect = computePrepareRollbackTargets().stream().map(c -> mySQLManager.getConnection(c).flatMap(function)).collect(Collectors.toList());
        CompositeFuture.all((List) collect)
                .onComplete(event -> {
                    log.logRollback(xid, event.succeeded());
                    if (event.failed()) {
                        mySQLManager.setTimer(log.retryDelay(), () -> retryRollback(handler, function));
                        return;
                    }
                    inTranscation = false;
                    clearConnections(handler);
                });
    }

    /**
     * before XA_PREPARE,should log the participants
     */
    private void logParticipants() {
        ImmutableParticipantLog[] participantLogs = new ImmutableParticipantLog[map.size()];
        int index = 0;
        for (Map.Entry<String, SqlConnection> e : map.entrySet()) {
            participantLogs[index] = new ImmutableParticipantLog(e.getKey(),
                    log.getExpires(),
                    connectionState.get(e.getValue()));
            index++;
        }
        log.log(xid, participantLogs);
    }

    protected SqlConnection changeTo(SqlConnection c, State state) {
        connectionState.put(c, state);
        log.log(xid, getDatasourceName(c), state);
        return c;
    }

    protected void changeTo(String c, State state) {
        connectionState.put(map.get(c), state);
        log.log(xid, c, state);
    }

    public void commit(Handler<AsyncResult<Void>> handler) {
        commitXa(() -> Future.succeededFuture(), handler);
    }

    /**
     * @param beforeCommit for the native connection commit or some exception test
     * @param handler the callback handler
     */
    public void commitXa(Supplier<Future> beforeCommit, Handler<AsyncResult<Void>> handler) {
        logParticipants();
        CompositeFuture xaEnd = executeAll(new Function<SqlConnection, Future>() {
            @Override
            public Future apply(SqlConnection connection) {
                Future future = Future.succeededFuture();
                switch (connectionState.get(connection)) {
                    case XA_INITED:
                        future = future
                                .compose(unuse -> connection.query(String.format(XA_START, xid)).execute())
                                .map(u -> changeTo(connection, State.XA_STARTED));
                    case XA_STARTED:
                        future = future
                                .compose(unuse -> connection.query(String.format(XA_END, xid)).execute())
                                .map(u -> changeTo(connection, State.XA_ENDED));
                    case XA_ENDED:
                    default:
                }
                return future;
            }

        });
        xaEnd.onFailure(throwable -> handler.handle(Future.failedFuture(throwable)));
        xaEnd.onSuccess(event -> {
            executeAll(connection -> {
                if (connectionState.get(connection) != State.XA_PREPARED) {
                    return connection.query(String.format(XA_PREPARE, xid)).execute()
                            .map(c -> changeTo(connection, State.XA_PREPARED));
                }
                return Future.succeededFuture();
            })
                    .onFailure(throwable -> {
                        log.logPrepare(xid, false);
                        //客户端触发回滚
                        handler.handle(Future.failedFuture(throwable));
                    })
                    .onSuccess(compositeFuture -> {
                        log.logPrepare(xid, true);
                        Future future;
                        try {
                            /**
                             * if log commit fail ,occur exception,other transcations rollback.
                             */
                            log.logCommitBeforeXaCommit(xid);
                            /**
                             * if native connection has inner commited,
                             * but it didn't received the commit response.
                             * should check the by manually.
                             */
                            future = beforeCommit.get();
                        } catch (Throwable throwable) {
                            future = Future.failedFuture(throwable);
                        }
                        future.onFailure((Handler<Throwable>) throwable -> {
                            log.logCancelCommitBeforeXaCommit(xid);
                            //客户端触发回滚
                            /**
                             * the client received exception ,it must  rollback.
                             */
                            handler.handle(Future.failedFuture(throwable));
                        });
                        future.onSuccess(event16 -> {
                            executeAll(connection -> {
                                return connection.query(String.format(XA_COMMIT, xid)).execute()
                                        .map(c -> changeTo(connection, State.XA_COMMITED));
                            })
                                    .onFailure(ignored -> {

                                        log.logCommit(xid, false);
                                        //retry
                                        retryCommit(handler);
                                    })
                                    .onSuccess(ignored -> {
                                        inTranscation = false;
                                        log.logCommit(xid, true);
                                        clearConnections(result -> handler.handle(((AsyncResult) result)));
                                    });
                        });
                    });
        });
    }

    /**
     * use new connection to retry the connection.
     *
     * @param handler
     */
    private void retryCommit(Handler<AsyncResult<Void>> handler) {
        MySQLManager mySQLManager = mySQLManager();
        CompositeFuture all = CompositeFuture.all(computePrepareCommittedTargets().stream()
                .map(s -> mySQLManager.getConnection(s)
                        .compose(c -> {
                            return c.query(String.format(XA_COMMIT, xid))
                                    .execute().compose(rows -> {
                                        changeTo(s, State.XA_COMMITED);
                                        c.close();
                                        return Future.succeededFuture();
                                    }, throwable -> {
                                        c.close();
                                        return Future.failedFuture(throwable);
                                    });
                        })).collect(Collectors.toList()));
        all.onSuccess(event -> {
            inTranscation = false;
            log.logCommit(xid, true);
            clearConnections(event2 -> handler.handle(Future.succeededFuture()));
        });
        all.onFailure(event -> {
            mySQLManager.setTimer(log.retryDelay(), () -> retryCommit(handler));
        });
    }

    private List<String> computePrepareCommittedTargets() {
        List<String> collect = connectionState.entrySet().stream()
                .filter(i -> i.getValue() != State.XA_COMMITED)
                .map(i -> i.getKey())
                .map(k -> getDatasourceName(k)).collect(Collectors.toList());
        return collect;
    }

    private List<String> computePrepareRollbackTargets() {
        List<String> collect = connectionState.entrySet().stream()
                .filter(i -> i.getValue() != State.XA_ROLLBACKED)
                .map(i -> i.getKey())
                .map(k -> getDatasourceName(k)).collect(Collectors.toList());
        return collect;
    }

    public CompositeFuture executeAll(Function<SqlConnection, Future> connectionFutureFunction) {
        if (map.isEmpty()) {
            return CompositeFuture.any(Future.succeededFuture(), Future.succeededFuture());
        }
        List<Future> futures = map.values().stream().map(connectionFutureFunction).collect(Collectors.toList());
        return CompositeFuture.all(futures);
    }

    /**
     * @param handler close and call handler
     */
    public void close(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            rollback(ignored -> clearConnections(event -> handler.handle(Future.succeededFuture())));
        } else {
            clearConnections(event -> handler.handle(Future.succeededFuture()));
        }
    }

    public void closeStatementState(Handler<AsyncResult<Void>> handler) {
        if (!inTranscation) {
            xid = null;
            clearConnections(handler);
        }else {
            handler.handle(Future.succeededFuture());
        }
    }

    @Override
    public String getXid() {
        return xid;
    }

    /**
     * before clear connections,it should check not be in transaction
     *
     * @param handler
     */
    private void clearConnections(Handler<AsyncResult<Void>> handler) {
        Future future;
        if (inTranscation) {
            future = executeAll(c -> {
                Future objectFuture;
                if (c instanceof MySQLConnection) {
                    MySQLConnection c1 = (MySQLConnection) c;
                    objectFuture = c1.resetConnection();
                } else {
                    objectFuture = Future.succeededFuture();
                }
                return objectFuture.onComplete(ignored -> c.close());
            });
        } else {
            future = executeAll(c -> {
                return c.close();
            });
        }

        inTranscation = false;
        map.clear();
        connectionState.clear();
        handler.handle((Future) Future.succeededFuture());
    }
}

