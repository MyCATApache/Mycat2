/**
 * Copyright (C) <2021>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy;

import io.mycat.GlobalConst;
import io.mycat.MycatException;
import io.mycat.NativeMycatServer;
import io.mycat.ScheduleUtil;
import io.mycat.beans.MySQLDatasource;
import io.mycat.config.DatasourceConfigProvider;
import io.mycat.proxy.handler.backend.BackendConCreateHandler;
import io.mycat.proxy.handler.backend.IdleHandler;
import io.mycat.proxy.handler.backend.ResultSetHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.SessionManager;
import io.mycat.util.StringUtil;
import io.mycat.util.VertxUtil;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.impl.future.PromiseInternal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static io.mycat.beans.mysql.MySQLCommandType.COM_QUERY;

public class MySQLDatasourcePool extends MySQLDatasource implements SessionManager<MySQLClientSession> {
    private final DatasourceConfigProvider provider;
    private final NativeMycatServer server;
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDatasourcePool.class);

    final LinkedBlockingDeque<MySQLClientSession> allSessions = new LinkedBlockingDeque<>();
    final LinkedBlockingDeque<MySQLClientSession> idleSessions = new LinkedBlockingDeque<>();
    volatile ScheduledFuture<?> scheduledFuture;

    public MySQLDatasourcePool(String name, DatasourceConfigProvider provider, NativeMycatServer server) {
        super(provider.get().get(name));
        this.provider = provider;
        this.server = server;

        scheduledFuture = ScheduleUtil.getTimer().scheduleAtFixedRate(this::idleConnectCheck,
                this.datasourceConfig.getIdleTimeout(),
                this.datasourceConfig.getIdleTimeout(),
                TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean isValid() {
        return this.datasourceConfig.equals(this.provider.get().get(this.getName()));
    }

    public final Future<Void> addIdleSession(MySQLClientSession session) {
        if(session.getIOThread() == Thread.currentThread()){
            if (!session.checkOpen()) {
                return removeSession(session, false, "has close");
            }
            if (session.isMonopolized()) {
                LOGGER.error("sessionId:{} isMonopolized", session.sessionId());
                return removeSession(session, false, "monopolized");
            }
            try {
                /**
                 * mycat对应透传模式对mysql session的占用
                 * niohandler对应透传以及task类对mysql session的占用
                 */
                assert session.getMycat() == null;
                assert !session.hasClosed();
                assert session.currentProxyBuffer() == null;
                assert !session.isIdle();
                session.setCursorStatementId(-1);
                session.resetPacket();
                session.setIdle(true);
                session.switchNioHandler(IdleHandler.INSTANCE);
                session.change2ReadOpts();
                idleSessions.addFirst(session);
                MycatMonitor.onAddIdleMysqlSession(session);
                return VertxUtil.newSuccessPromise();
            } catch (Exception e) {
                LOGGER.error("", e);
                return removeSession(session, false, e.getMessage());
            }
        }else {
            PromiseInternal<Void> promise = VertxUtil.newPromise();
            session.getIOThread().addNIOJob(new NIOJob() {
                @Override
                public void run(ReactorEnvThread reactor) throws Exception {
                    addIdleSession(session).onComplete(promise);
                }

                @Override
                public void stop(ReactorEnvThread reactor, Exception reason) {
                    addIdleSession(session).onComplete(promise);
                }

                @Override
                public String message() {
                    return "add idle session";
                }
            });
            return promise.future();
        }
    }


    public Future<MySQLClientSession> createSession() {
        if (!(Thread.currentThread() instanceof ReactorEnvThread)) {
            return waitForIdleSession(false);
        }
        int limitCount = this.getSessionLimitCount();
        synchronized (this) {
            if (allSessions.size() >= limitCount && idleSessions.isEmpty()) {
                return waitForIdleSession(true);
            }
        }
        if (idleSessions.isEmpty()) {
            Future<MySQLClientSession> future = innerCreateCon();
            return future.recover(new Function<Throwable, Future<MySQLClientSession>>() {
                final AtomicInteger retryCount = new AtomicInteger(0);
                final long startTime = System.currentTimeMillis();

                @Override
                public Future<MySQLClientSession> apply(Throwable t) {
                    PromiseInternal<MySQLClientSession> promise = VertxUtil.newPromise();
                    long now = System.currentTimeMillis();
                    long maxConnectTimeout = MySQLDatasourcePool.this.getMaxConnectTimeout();
                    if (retryCount.get() >= limitCount) {
                        promise.tryFail(new MycatException("retry get connection fail:" + getName()));
                    } else if (startTime + maxConnectTimeout > now) {
                        promise.tryFail(new MycatException("retry get connection timeout:" + getName()));
                    } else {
                        retryCount.incrementAndGet();
                        waitForIdleSession(true).recover(this).onComplete(promise);
                    }
                    return promise.future();
                }
            });
        } else {
            MySQLClientSession session = idleSessions.removeFirst();
            if(session.checkOpen()){
                session.setIdle(false);
                session.switchNioHandler(null);
                return Future.succeededFuture(session);
            }else {
               return session.close(false,"has closed")
                               .flatMap(unused -> createSession());

            }

        }
    }

    private Future<MySQLClientSession> waitForIdleSession(boolean wait) {
        PromiseInternal<MySQLClientSession> promise = VertxUtil.newPromise();
        ReactorEnvThread reactorEnvThread;
        if (Thread.currentThread() instanceof ReactorEnvThread) {
            reactorEnvThread = (ReactorEnvThread) Thread.currentThread();
        } else {
            reactorEnvThread = this.server.getReactorManager().getRandomReactor();
        }
        if (wait) {
            ScheduleUtil.getTimer().schedule(() -> {
                        nextStageForSession(promise, reactorEnvThread);
                    },
                    datasourceConfig.getMaxConnectTimeout() / datasourceConfig.getMaxRetryCount(),
                    TimeUnit.MILLISECONDS);
        } else {
            nextStageForSession(promise, reactorEnvThread);
        }
        return promise;
    }

    private void nextStageForSession(PromiseInternal<MySQLClientSession> promise, ReactorEnvThread reactorEnvThread) {
        reactorEnvThread.addNIOJob(new NIOJob() {
            @Override
            public void run(ReactorEnvThread reactor) throws Exception {
                createSession().onComplete(promise);
            }

            @Override
            public void stop(ReactorEnvThread reactor, Exception reason) {
                promise.tryFail(reason);
            }

            @Override
            public String message() {
                return "wait for idle session";
            }
        });
    }

    private Future<MySQLClientSession> innerCreateCon() {
        PromiseInternal<MySQLClientSession> promise = VertxUtil.newPromise();
        new BackendConCreateHandler(this, promise);
        return promise.flatMap(session -> {
            synchronized (allSessions) {
                allSessions.add(session);
            }
            MySQLDatasource datasource = session.getDatasource();
            String sql = datasource.getInitSqlForProxy();
            if (!StringUtil.isEmpty(sql)) {
                PromiseInternal<MySQLClientSession> promiseInternal = VertxUtil.newPromise();
                ResultSetHandler
                        .DEFAULT
                        .request(session, COM_QUERY,
                                sql.getBytes(StandardCharsets.UTF_8),
                                promiseInternal
                        );
                return promiseInternal.onFailure(event -> {
                    session.close(false, "initSql fail");
                });
            } else {
                return Future.succeededFuture(session);
            }
        });
    }

    private Future<Void> idleConnectCheck() {
        List<MySQLClientSession> mySQLClientSessions = new ArrayList<>(idleSessions);
        List<MySQLClientSession> needCloseConnections;
        int minCon = datasourceConfig.getMinCon();
        if (mySQLClientSessions.size() > minCon) {
            needCloseConnections = mySQLClientSessions.subList(minCon, mySQLClientSessions.size());
        } else {
            needCloseConnections = Collections.emptyList();
        }
        ArrayList<Future> futures = new ArrayList<>(mySQLClientSessions.size());
        for (MySQLClientSession needCloseConnection : needCloseConnections) {
            idleSessions.remove(needCloseConnection);
            needCloseConnection.setIdle(false);
            needCloseConnection.switchNioHandler(null);
            futures.add(removeSession(needCloseConnection, true, "idle check"));
        }
        for (MySQLClientSession mySQLClientSession : mySQLClientSessions) {
            idleSessions.remove(mySQLClientSession);
            mySQLClientSession.setIdle(false);
            mySQLClientSession.switchNioHandler(null);
            futures.add(checkIfNeedHeartBeat(mySQLClientSession));
        }
        return (Future) CompositeFuture.all(futures);
    }

    private Future<Void> checkIfNeedHeartBeat(MySQLClientSession session) {
        if (!session.getDatasource().isValid()) {
            return session.close(false, "not valid").future();
        }
        if (!session.checkOpen()) {
            return session.close(false, "not open").future();
        }
        if (session.getDatasource().getIdleTimeout() + session.getLastActiveTime()
                > System.currentTimeMillis()) {
            return session.close(false, "mysql session  close because of idle").future();
        }
        return sendPing(session);
    }

    private Future<Void> sendPing(MySQLClientSession session) {
        idleSessions.remove(session);
        session.setIdle(false);
        session.switchNioHandler(null);
        Promise<MySQLClientSession> promise = VertxUtil.newPromise();
        ResultSetHandler.DEFAULT
                .request(session, COM_QUERY, GlobalConst.SINGLE_NODE_HEARTBEAT_SQL.getBytes(StandardCharsets.UTF_8), promise);
        return promise.future().onSuccess(s -> {
            session.close(true, "ping");
        }).onFailure(event -> session.close(false, "ping fail")).mapEmpty();
    }

    @Override
    public List<MySQLClientSession> getAllSessions() {
        return new ArrayList<>(allSessions);
    }

    @Override
    public int currentSessionCount() {
        return allSessions.size();
    }

    @Override
    public Future<Void> removeSession(MySQLClientSession session, boolean normal, String reason) {
       if (session.getIOThread() == Thread.currentThread()){
           decrementSessionCounter();
           allSessions.remove(session);
           idleSessions.remove(session);
           session.resetPacket();
           try {
               session.channel().close();
           } catch (Throwable ignore) {
               LOGGER.error("", ignore);
           }
           return Future.succeededFuture();
       }
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        session.getIOThread().addNIOJob(new NIOJob() {
            @Override
            public void run(ReactorEnvThread reactor) throws Exception {
                removeSession(session,normal,reason).onComplete(promise);
            }

            @Override
            public void stop(ReactorEnvThread reactor, Exception hint) {
                removeSession(session,normal,reason).onComplete(promise);
            }

            @Override
            public String message() {
                return "removeSession";
            }
        });

        return promise.future();
    }

    public Future<Void> close() {
        this.provider.get().remove(getName());
        if(this.scheduledFuture!=null&&!this.scheduledFuture.isCancelled()){
            this.scheduledFuture.cancel(false);
        }
        return idleConnectCheck().flatMap(unused -> {
            List<Future> futures = new ArrayList<>();
            for (MySQLClientSession session : allSessions) {
                futures.add(removeSession(session, true, "pool close"));
            }
            return (Future) CompositeFuture.all(futures);
        });
    }

    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        close();
    }
}
