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

import cn.mycat.vertx.xa.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.SqlConnection;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * xid  always be null;
 */
public class LocalSqlConnection extends AbstractXaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(LocalSqlConnection.class);
    protected final ConcurrentHashMap<String, SqlConnection> map = new ConcurrentHashMap<>();
    protected final List<SqlConnection> extraConnections = new CopyOnWriteArrayList<>();
    protected final List<Future<Void>> closeList = new CopyOnWriteArrayList<>();
    protected final Supplier<MySQLManager> mySQLManagerSupplier;

    public LocalSqlConnection(Supplier<MySQLManager> mySQLManagerSupplier, XaLog xaLog) {
        super(xaLog);
        this.mySQLManagerSupplier = mySQLManagerSupplier;
    }

    public MySQLManager mySQLManager() {
        return mySQLManagerSupplier.get();
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
                Future<SqlConnection> sqlConnectionFuture = mySQLManager().getConnection(targetName);
                return sqlConnectionFuture.compose(connection -> {
                    map.put(targetName, connection);
                    return connection.query(getTransactionIsolation().getCmd())
                            .execute().flatMap(rows -> connection.query("begin")
                                    .execute().mapEmpty()).map(r -> connection);
                });
            }
        }
        return mySQLManager().getConnection(targetName)
                .map(connection -> {
                    if (!map.containsKey(targetName)) {
                        map.put(targetName, connection);
                    } else {
                        extraConnections.add(connection);
                    }
                    return connection;
                });
    }

    @Override
    public Future<Void> rollback() {
        List<Future> rollback = map.values().stream().map(c -> c.query("rollback").execute()).collect(Collectors.toList());
        return CompositeFuture.join(rollback).eventually(event -> {
            inTranscation = false;
            //每一个记录日志
            return Future.succeededFuture();
        }).mapEmpty().flatMap(o -> closeStatementState());
    }

    @Override
    public Future<Void> commit() {
        List<Future> rollback = map.values().stream().map(c -> c.query("commit").execute()).collect(Collectors.toList());
        return CompositeFuture.join(rollback).onComplete(event -> {
            inTranscation = false;
            //每一个记录日志
        }).mapEmpty().flatMap(o -> closeStatementState());
    }

    @Override
    public Future<Void> commitXa(Function<ImmutableCoordinatorLog, Future<Void>> beforeCommit) {
        return Future.future(promise -> {
            beforeCommit.apply(null).onComplete(result -> {
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
        Future future = CompositeFuture.join((List) closeList);
        closeList.clear();
        for (SqlConnection extraConnection : extraConnections) {
            future = future.compose(unused -> extraConnection.close());
        }
        future = future.onComplete(event -> extraConnections.clear());
        for (SqlConnection connection : map.values()) {
            future = future.compose(unused -> connection.close());
        }
        future = future.onComplete(event -> map.clear());
        return future.onComplete(event -> inTranscation = false).mapEmpty();
    }


    @Override
    public String getXid() {
        return null;
    }

    @Override
    public void addCloseFuture(Future<Void> future) {
        closeList.add(future);
    }

    @Override
    public Future<Void> closeStatementState() {
        Future<Void> future = CompositeFuture.join((List) closeList).mapEmpty();
        closeList.clear();
        for (SqlConnection extraConnection : extraConnections) {
            future = future.compose(unused -> extraConnection.close());
        }
        future = future.onComplete(event -> extraConnections.clear());
        if (inTranscation) {
            return future;
        } else {
            for (SqlConnection connection : map.values()) {
                future = future.compose(unused -> connection.close());
            }
            future = future.onComplete(event -> map.clear());
            return future;
        }
    }
}
