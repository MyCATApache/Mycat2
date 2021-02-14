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

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
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
    public Future<Void> begin() {
        if (inTranscation) {
            return (Future.failedFuture(new IllegalArgumentException("occur Nested transaction")));
        }
        inTranscation = true;
        return Future.succeededFuture();
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
        return mySQLManager.getConnection(targetName)
                .map(connection -> {
                  addCloseConnection(connection);
                    return connection;
                });
    }

    @Override
    public Future<Void> rollback() {
        List<Future> rollback = map.values().stream().map(c -> c.query("rollback").execute()).collect(Collectors.toList());
        return CompositeFuture.all(rollback).onComplete(event -> {
            map.clear();
            inTranscation = false;
            //每一个记录日志
        }).mapEmpty().flatMap(o -> closeStatementState());
    }

    @Override
    public Future<Void> commit() {
        List<Future> rollback = map.values().stream().map(c -> c.query("commit").execute()).collect(Collectors.toList());
        return CompositeFuture.all(rollback).onComplete(event -> {
            map.clear();
            inTranscation = false;
            //每一个记录日志
        }).mapEmpty().flatMap(o -> closeStatementState());
    }

    @Override
    public Future<Void> commitXa(Supplier<Future> beforeCommit) {
        return Future.future(promise -> {
            beforeCommit.get().onComplete((Handler<AsyncResult>) result -> {
                if (result.succeeded()) {
                    commit().onComplete(promise);
                } else {
                    promise.fail(result.cause());
                }
            });
        });
    }

    @Override
    public Future<Void> close() {
        return clearConnections().onComplete(event -> inTranscation = false).mapEmpty();
    }



    @Override
    public String getXid() {
        return null;
    }

    @Override
    public Future<Void> clearConnections() {
        if (inTranscation) {
            return dealCloseConnections();
        } else {
           return executeAll(SqlClient::close).onComplete(event -> {
                map.clear();
            }).mapEmpty().flatMap(o -> dealCloseConnections());
        }
    }

    @Override
    public Future<Void> closeStatementState() {
        return super.closeStatementState().flatMap(unused -> clearConnections());
    }

    public CompositeFuture executeAll(Function<SqlConnection, Future> connectionFutureFunction) {
        if (map.isEmpty()) {
            return CompositeFuture.any(Future.succeededFuture(), Future.succeededFuture());
        }
        List<Future> futures = map.values().stream().map(connectionFutureFunction).collect(Collectors.toList());
        return CompositeFuture.all(futures);
    }
}
