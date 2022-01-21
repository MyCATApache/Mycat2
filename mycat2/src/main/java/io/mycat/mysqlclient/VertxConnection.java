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

import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.mysqlclient.command.OkCommand;
import io.mycat.mysqlclient.command.QueryCommand;
import io.mycat.mysqlclient.command.ResponseBufferCommand;
import io.mycat.mysqlclient.decoder.ByteArrayDecoder;
import io.mycat.mysqlclient.decoder.StringArrayDecoder;
import io.mycat.newquery.NewMycatConnectionConfig;
import io.mycat.newquery.SqlResult;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;


@Getter
public class VertxConnection {
    final static Logger logger = LoggerFactory.getLogger(VertxConnection.class);
    final NetSocket netSocket;
    private long connectionId;
    private VertxPoolConnectionImpl.Config config;
    private VertxConnectionPool vertxConnectionPool;
    private int serverstatus;
    Future<Void> future = Future.succeededFuture();


    public VertxConnection(NetSocket netSocket, long connectionId, VertxPoolConnectionImpl.Config config, VertxConnectionPool vertxConnectionPool) {
        this.netSocket = netSocket;
        this.connectionId = connectionId;
        this.config = config;
        this.vertxConnectionPool = vertxConnectionPool;
        checkException();
    }

    private void checkException() {
        this.netSocket.exceptionHandler(event -> {
            logger.error("", event);
            VertxConnection.this.vertxConnectionPool.kill(VertxConnection.this);
        });
    }

    public Future<SqlResult> update(String sql) {
        synchronized (VertxConnection.this) {
            Future<SqlResult> sqlResultFuture = future.transform(unused -> Future.future(promise -> {
                checkException();
                OkCommand queryCommand = new OkCommand(sql, netSocket, promise);
                netSocket.handler(queryCommand);
                queryCommand.write();
                promise.future().onSuccess(event -> VertxConnection.this.serverstatus = queryCommand.serverstatus);
            }));
            future = sqlResultFuture.mapEmpty();
            return sqlResultFuture;
        }
    }


    public <T> Observable<Object[]> queryAsObjectArray(String sql) {
        return query(sql, new StringArrayDecoder());
    }

    public <T> Observable<byte[][]> queryAsByteArray(String sql) {
        return query(sql, new ByteArrayDecoder());
    }

    public synchronized <T> Observable<T> query(String sql, Decoder<T> decoder) {
        logger.debug("VertxConnection{}:netSocket{}", this.hashCode(), netSocket.hashCode());
        return Observable.create(emitter -> {
            synchronized (VertxConnection.this) {
                future = future.transform(new Function<AsyncResult<Void>, Future<Void>>() {
                    @Override
                    public Future<Void> apply(AsyncResult<Void> voidAsyncResult) {
                        return Future.future(new Handler<Promise<Void>>() {
                            @Override
                            public void handle(Promise<Void> promise) {
                                checkException();
                                QueryCommand<T> queryCommand = new QueryCommand(netSocket, sql, config, decoder, emitter) {
                                    @Override
                                    public void onEnd() {
                                        super.onEnd();
                                        promise.tryComplete();
                                    }
                                };
                                netSocket.handler(queryCommand);
                                queryCommand.write();
                                promise.future().onSuccess(event -> VertxConnection.this.serverstatus = queryCommand.getServerstatus());
                            }
                        });
                    }
                });
            }
        });
    }


    public Observable<Buffer> query(String sql) {
        Observable<Buffer> objectObservable = Observable.create(emitter -> {
            synchronized (VertxConnection.this) {
                future = future.flatMap(new Function<Void, Future<Void>>() {
                    @Override
                    public Future<Void> apply(Void unused) {
                        return Future.future((promise) -> {
                            checkException();
                            ResponseBufferCommand bufferedResponseHandler = new ResponseBufferCommand(netSocket, sql, config, !NewMycatConnectionConfig.PASS_HALF_PACKET, emitter) {
                                @Override
                                public void onEnd() {
                                    super.onEnd();
                                    promise.tryComplete();
                                }
                            };
                            netSocket.handler(bufferedResponseHandler);
                            bufferedResponseHandler.write();
                        });
                    }
                });
            }
        });
        return objectObservable;
    }

    Future<Void> close() {
        return netSocket.close();
    }

    public boolean checkVaildForRecycle(){
        boolean autocommit = MySQLServerStatusFlags.statusCheck(serverstatus,MySQLServerStatusFlags.AUTO_COMMIT );
        return autocommit;
    }
}