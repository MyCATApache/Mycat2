/**
 * Copyright (C) <2022>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.mysqlclient;

import io.mycat.commands.AbstractMycatDatasourcePool;
import io.mycat.commands.JdbcDatasourcePoolImpl;
import io.mycat.monitor.DatabaseInstanceEntry;
import io.mycat.monitor.InstanceMonitor;
import io.mycat.newquery.NewMycatConnection;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

public class MycatNativeDatasourcePool extends AbstractMycatDatasourcePool {
    VertxPoolConnectionImpl vertxPoolConnection;

    public MycatNativeDatasourcePool(VertxPoolConnectionImpl vertxConnectionPool, String targetName) {
        super(targetName);
        this.vertxPoolConnection = vertxConnectionPool;
    }

    @Override
    public Future<NewMycatConnection> getConnection() {
        return vertxPoolConnection.getConnection().map(connection -> {
            DatabaseInstanceEntry stat = DatabaseInstanceEntry.stat(targetName);
            stat.plusCon();
            stat.plusQps();
            return new VertxMycatConnectionPool(targetName,connection, vertxPoolConnection){
                long start;

                @Override
                public void onSend() {
                    start = System.currentTimeMillis();
                    onActiveTimestamp(start);
                }

                @Override
                public void onRev() {
                    long end = System.currentTimeMillis();
                    onActiveTimestamp(end);
                    InstanceMonitor.plusPrt(end - start);
                }

                @Override
                public Future<Void> close() {
                    stat.decCon();
                    return super.close();
                }

                @Override
                public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
                    return Observable.create(emitter -> {
                        JdbcDatasourcePoolImpl jdbcDatasourcePool = new JdbcDatasourcePoolImpl(targetName);
                        Future<NewMycatConnection> connectionFuture = jdbcDatasourcePool.getConnection();
                        connectionFuture.onSuccess(event -> {
                            Observable<VectorSchemaRoot> observable = event.prepareQuery(sql, params, allocator);
                            observable.subscribe(vectorSchemaRoot -> emitter.onNext(vectorSchemaRoot),
                                    throwable -> emitter.onError(throwable),
                                    () -> emitter.onComplete());
                        });
                        connectionFuture.onFailure(event -> emitter.onError(event));
                    });
                }

                @Override
                public Future<List<Object>> call(String sql) {
                    JdbcDatasourcePoolImpl jdbcDatasourcePool = new JdbcDatasourcePoolImpl(targetName);
                    Future<NewMycatConnection> connectionFuture = jdbcDatasourcePool.getConnection();
                    return connectionFuture.flatMap(connection -> connection.call(sql));
                }
            };
        });
    }

    @Override
    public int getAvailableNumber() {
        return vertxPoolConnection.getAvailableNumber();
    }

    @Override
    public int getUsedNumber() {
        return vertxPoolConnection.getUsedNumber();
    }

    @Override
    public void close() {
        vertxPoolConnection.close();
    }
}
