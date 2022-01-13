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

import io.mycat.MycatCore;
import io.mycat.mysqlclient.command.ConnectHandler;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.*;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetSocket;
import lombok.Data;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class VertxPoolConnectionImpl implements VertxConnectionPool {
    final static Logger logger = LoggerFactory.getLogger(MycatCore.class);
    Config config;
    ConcurrentLinkedQueue<VertxConnection> connections = new ConcurrentLinkedQueue<>();
    Vertx vertx;
    boolean closed;
    AtomicInteger useCount = new AtomicInteger();
    long lastActiveTime = System.currentTimeMillis();

    @Data
    public static class Config {
        int port = 3306;
        String host = "127.0.0.1";
        String username = "root";
        String password = "123456";
        String database = "mysql";
        boolean clientDeprecateEof = true;

        int maxCon = 1000;
        int minCon = 1;
        long timer = TimeUnit.SECONDS.toMillis(30);
        int retry = 3;
    }

    @SneakyThrows
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        VertxPoolConnectionImpl vertxConnectionPool = new VertxPoolConnectionImpl(new Config(), vertx);
        Future<VertxConnection> connection = vertxConnectionPool.getConnection();
        VertxConnection vertxConnection = connection.toCompletionStage().toCompletableFuture().get();
        System.out.println();
    }


    public VertxPoolConnectionImpl(Config config, Vertx vertx) {
        this.config = config;
        this.vertx = vertx;
        this.vertx.setPeriodic(config.timer, event -> {
            long dur = System.currentTimeMillis() - lastActiveTime;
            if (dur > TimeUnit.MINUTES.toMillis(1)) {
                synchronized (VertxPoolConnectionImpl.this) {
                    Integer availableNumber = getAvailableNumber();
                    int minCon = config.getMinCon();
                    int distance = availableNumber - minCon;

                    //async
                    List<Future<VertxConnection>> idleConnections = new ArrayList<>();
                    for (int i = 0; i < distance; i++) {
                        idleConnections.add(getConnectionWithMaxCountLimit());
                    }
                    CompositeFuture compositeFuture = CompositeFuture.join((List) idleConnections).onSuccess(event12 -> {
                        for (Future<VertxConnection> idleConnection : idleConnections) {
                            idleConnection.onSuccess(event1 -> kill(event1));
                        }
                    });
                    compositeFuture.onComplete(event13 -> {
                        for (int i = 0; i < config.getMinCon(); i++) {
                            Future<VertxConnection> connectionFuture = getConnectionWithMaxCountLimit();
                            connectionFuture.onSuccess(vertxConnection -> {
                                Observable<Object[]> observable = vertxConnection.queryAsObjectArray("select 1");
                                AtomicBoolean success = new AtomicBoolean(false);
                                observable.subscribe(objects -> success.set(true), throwable -> success.set(false), () -> {
                                    if (success.get()) {
                                        recycle(vertxConnection);
                                    } else {
                                        kill(vertxConnection);
                                    }
                                });
                            }).onFailure(new Handler<Throwable>() {
                                @Override
                                public void handle(Throwable event) {

                                }
                            });
                        }
                    });
                }
            }
        });
    }

    private Future<VertxConnection> innerCreateConnection() {
        return Future.future(promise -> {
            NetClient netClient = vertx.createNetClient();
            Future<NetSocket> netSocketFuture = netClient.connect(config.getPort(), config.getHost());
            netSocketFuture = netSocketFuture.onSuccess(netSocket -> {
                netSocket.exceptionHandler(event -> {
                    logger.error("", event);
                    promise.tryFail(event);
                });
                ConnectHandler connectHandler = new ConnectHandler(netSocket, config, VertxPoolConnectionImpl.this);
                Future<VertxConnection> vertxConnectionFuture = connectHandler.handleInitialHandshake();
                vertxConnectionFuture.onSuccess(vertxConnection -> promise.tryComplete(vertxConnection));
                vertxConnectionFuture.onFailure(event -> promise.tryFail(event));
            });
            netSocketFuture.onFailure(event -> promise.tryFail(event));
        });
    }


    @Override
    public Future<VertxConnection> getConnection() {
        lastActiveTime = System.currentTimeMillis();
        Future<VertxConnection> tryGetFuture = getConnectionWithMaxCountLimit();
        return tryGetFuture.recover(throwable -> {
            logger.error("try get connection fail", throwable);
            Future<VertxConnection> future = Future.failedFuture(throwable);
            for (int i = 0; i < config.retry; i++) {
                int count = i;
                future = future.recover(throwable1 -> {
                    logger.error("try get connection fail try count:{}", count, throwable);
                    return getConnectionWithMaxCountLimit();
                });
            }
            return future;
        });
    }

    private Future<VertxConnection> getConnectionWithMaxCountLimit() {
        return Future.future(new Handler<Promise<VertxConnection>>() {

            @Override
            public void handle(Promise<VertxConnection> promise) {
                if (closed) {
                    promise.tryFail("pool has closed");
                    return;
                }
                VertxConnection connection = connections.poll();
                if (connection == null) {
                    synchronized (this) {
                        Integer usedNumber = getUsedNumber();
                        int maxCon = config.maxCon;
                        if (usedNumber < maxCon) {
                            innerCreateConnection().map(c -> {
                                useCount.incrementAndGet();
                                return c;
                            }).onComplete(promise);
                        } else {
                            promise.tryFail("pool has closed");
                        }
                    }
                }
                useCount.incrementAndGet();
                promise.tryComplete(connection);
                return;
            }
        });
    }

    @Override
    public synchronized void close() {
        closed = true;
        for (VertxConnection connection : connections) {
            connection.close();
        }
    }

    @Override
    public void recycle(VertxConnection connection) {
        synchronized (this) {
            useCount.decrementAndGet();
            connections.offer(connection);
        }

    }

    @Override
    public void kill(VertxConnection connection) {
        synchronized (this) {
            connections.remove(connection);
            connection.close();
        }
    }

    public Integer getAvailableNumber() {
        return connections.size();
    }

    public Integer getUsedNumber() {
        return useCount.get();
    }
}
