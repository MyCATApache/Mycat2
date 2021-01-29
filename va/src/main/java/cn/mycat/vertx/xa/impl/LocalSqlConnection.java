/**
 * Copyright [2021] [chen junwen]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.MySQLManager;
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

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * xid  always be null;
 */
public class LocalSqlConnection extends AbstractXaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalSqlConnection.class);
    protected final ConcurrentHashMap<String, SqlConnection> map = new ConcurrentHashMap<>();
    protected final MySQLManager mySQLManager;

    public LocalSqlConnection(MySQLManager mySQLManager, XaLog xaLog) {
        super(xaLog);
        this.mySQLManager = mySQLManager;
    }

    @Override
    public void begin(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            handler.handle(Future.failedFuture(new IllegalArgumentException("occur Nested transaction")));
            return;
        }
        inTranscation = true;
        handler.handle(Future.succeededFuture());
    }

    public Future<SqlConnection> getConnection(String targetName) {
        if (inTranscation) {
            if (map.containsKey(targetName)) {
                return Future.succeededFuture(map.get(targetName));
            } else {
                Future<SqlConnection> sqlConnectionFuture = mySQLManager.getConnection(targetName);
                return sqlConnectionFuture.compose(connection -> {
                    map.put(targetName, connection);
                    Future<RowSet<Row>> execute = connection.query("begin").execute();
                    return execute.map(r -> connection);
                });
            }
        }
        return mySQLManager.getConnection(targetName);
    }

    @Override
    public void rollback(Handler<AsyncResult<Void>> handler) {
        List<Future> rollback = map.values().stream().map(c -> c.query("rollback").execute()).collect(Collectors.toList());
        CompositeFuture.all(rollback).onComplete(event -> {
            map.clear();
            inTranscation = false;
            //每一个记录日志
            handler.handle((AsyncResult) event);
        });
    }

    @Override
    public void commit(Handler<AsyncResult<Void>> handler) {
        List<Future> rollback = map.values().stream().map(c -> c.query("commit").execute()).collect(Collectors.toList());
        CompositeFuture.all(rollback).onComplete(event -> {
            map.clear();
            inTranscation = false;
            //每一个记录日志
            handler.handle((AsyncResult) event);
        });
    }

    @Override
    public void commitXa(Supplier<Future> beforeCommit, Handler<AsyncResult<Void>> handler) {
        beforeCommit.get().onComplete((Handler<AsyncResult>) event -> {
            if (event.succeeded()){
                commit(handler);
            }else {
                handler.handle(Future.failedFuture(event.cause()));
            }
        });
    }

    @Override
    public void close(Handler<AsyncResult<Void>> handler) {
        clearConnections(new Handler<AsyncResult<Void>>() {
            @Override
            public void handle(AsyncResult<Void> event) {
                inTranscation = false;
                handler.handle(Future.succeededFuture());
            }
        });
    }

    @Override
    public void closeStatementState(Handler<AsyncResult<Void>> handler) {
        if (!inTranscation) {
            clearConnections(handler);
        }
    }

    private void clearConnections(Handler<AsyncResult<Void>> handler) {
        if (inTranscation) {
            executeAll(c -> {
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
            executeAll(c -> {
                return c.close();
            });
        }
        map.clear();
        handler.handle((Future) Future.succeededFuture());
    }

    public CompositeFuture executeAll(Function<SqlConnection, Future> connectionFutureFunction) {
        if (map.isEmpty()) {
            return CompositeFuture.any(Future.succeededFuture(), Future.succeededFuture());
        }
        List<Future> futures = map.values().stream().map(connectionFutureFunction).collect(Collectors.toList());
        return CompositeFuture.all(futures);
    }
}
