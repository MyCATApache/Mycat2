/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.vertx;

import io.mycat.MetaClusterCurrent;
import io.mycat.MycatServer;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.config.MycatServerConfig;
import io.mycat.config.ServerConfig;
import io.mycat.config.ThreadPoolExecutorConfig;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

public class VertxMycatServer implements MycatServer {
    private static final Logger LOGGER = LoggerFactory.getLogger(VertxMycatServer.class);
    MycatSessionManager server;
    private MycatServerConfig serverConfig;

    public VertxMycatServer(MycatServerConfig serverConfig) {
        this.serverConfig = serverConfig;
    }

    public static void main(String[] args) {

    }

    @Override
    public RowBaseIterator showNativeDataSources() {
        return server.showNativeDataSources();
    }

    @Override
    public RowBaseIterator showConnections() {
        return server.showConnections();
    }

    @Override
    public RowBaseIterator showReactors() {
        return server.showReactors();
    }

    @Override
    public RowBaseIterator showBufferUsage(long sessionId) {
        return server.showBufferUsage(sessionId);
    }

    @Override
    public RowBaseIterator showNativeBackends() {
        return server.showNativeBackends();
    }

    @Override
    public void start() {
        ServerConfig server = serverConfig.getServer();
        ThreadPoolExecutorConfig workerPool = server.getWorkerPool();
        DeploymentOptions workerOpts = new DeploymentOptions()
                .setWorker(true)
                .setInstances(1)
                .setMaxWorkerExecuteTime(workerPool.getTaskTimeout())
                .setMaxWorkerExecuteTimeUnit(TimeUnit.valueOf(workerPool.getTimeUnit()))
                .setHa(true)
                .setWorkerPoolName("vertx-mycat")
                .setWorkerPoolSize(workerPool.getMaxPoolSize());
        this.server = new MycatSessionManager(serverConfig);
        Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
        vertx.deployVerticle(this.server, workerOpts);
    }

    public static class MycatSessionManager extends AbstractVerticle implements MycatServer {
        private final ConcurrentLinkedDeque<VertxSession> sessions = new ConcurrentLinkedDeque<>();
        private MycatServerConfig serverConfig;

        public MycatSessionManager(MycatServerConfig serverConfig) {
            this.serverConfig = serverConfig;
        }

        @Override
        public void start() throws Exception {
            NetServer netServer = vertx.createNetServer();//创建代理服务器
            netServer.connectHandler(socket -> {
                VertxMySQLAuthHandler vertxMySQLAuthHandler = new VertxMySQLAuthHandler(socket, MycatSessionManager.this);
            }).listen(this.serverConfig.getServer().getPort(),
                    this.serverConfig.getServer().getIp(), listenResult -> {//代理服务器的监听端口
                        if (listenResult.succeeded()) {
                            LOGGER.info("Mycat Vertx server started up.");
                        } else {
                            LOGGER.error("Mycat Vertx server exit. because: " + listenResult.cause().getMessage(), listenResult.cause());
                            System.exit(1);
                        }
                    });
        }

        public void addSession(VertxSession vertxSession) {
            NetSocket socket = vertxSession.getSocket();
            socket.closeHandler(event -> {
                String message = "session:{} is closing:{}";
                LOGGER.info(message, vertxSession);
            });
            sessions.add(vertxSession);
        }

        @Override
        public RowBaseIterator showNativeDataSources() {
            return demo();
        }

        @Override
        public RowBaseIterator showConnections() {
            return demo();
        }

        @Override
        public RowBaseIterator showReactors() {
            return demo();
        }

        @Override
        public RowBaseIterator showBufferUsage(long sessionId) {
            return demo();
        }

        @Override
        public RowBaseIterator showNativeBackends() {
            return demo();
        }
    }

    private static RowBaseIterator demo() {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("demo", JDBCType.VARCHAR);
        resultSetBuilder.addObjectRowPayload(Arrays.asList("unsupported"));
        return resultSetBuilder.build();
    }
}