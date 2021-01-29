/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is open software: you can redistribute it and/or modify it under the terms of the
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
package io.mycat.commands;

import io.mycat.MetaClusterCurrent;
import io.mycat.MySQLPacketUtil;
import io.mycat.MycatException;
import io.mycat.proxy.NativeMycatServer;
import io.mycat.beans.MySQLDatasource;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.MySQLPacketExchanger;
import io.mycat.proxy.handler.ResponseType;
import io.mycat.proxy.handler.backend.ResultSetHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.PartialType;
import io.mycat.util.VertxUtil;
import io.vertx.core.impl.future.PromiseInternal;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.function.BiConsumer;

import static io.mycat.proxy.handler.MySQLPacketExchanger.DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK;

/**
 * @author jamie12221 date 2019-05-12 22:41 dataNode执行器 该类本意是从路由获得dataNode名字之后,使用该执行器执行,
 * 解耦结果类和实际执行方法
 **/
public class MySQLTaskUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLTaskUtil.class);

    public static PromiseInternal<Void> proxyBackendByDatasourceName(MycatSession mycat,
                                                                     String datasourceName ,
                                                                     String sql,
                                                                     TransactionSyncType transaction,
                                                                     MySQLIsolation isolation) {
        //todo fix the log
        return proxyBackendByDataSource(mycat,
                MySQLPacketUtil.generateComQueryPacket(sql),
                datasourceName,
                DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK,
                transaction, isolation);
    }

    public static PromiseInternal<Void> proxyBackendByDataSource(MycatSession mycat,
                                                byte[] packetData,
                                                String datasourceName,
                                                MySQLPacketExchanger.PacketExchangerCallback finallyCallBack,
                                                TransactionSyncType transactionType,
                                                MySQLIsolation isolation) {
        Objects.requireNonNull(datasourceName);
        mycat.switchProxyWriteHandler();
        PromiseInternal<Void> promise = VertxUtil.newPromise();
        mycat.getIOThread().addNIOJob(new NIOJob() {
            @Override
            public void run(ReactorEnvThread reactor2) throws Exception {
                MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
                MySQLSessionManager mySQLSessionManager = reactor.getMySQLSessionManager();
                BiConsumer<MySQLDatasource, SessionCallBack<MySQLClientSession>> getSession = (datasource, mySQLClientSessionSessionCallBack) -> {
                    if (mycat.isBindMySQLSession()) {
                        MySQLClientSession mySQLSession = mycat.getMySQLSession();
                        String currentDataSource = mySQLSession.getDatasourceName();
                        if (datasourceName.equals( currentDataSource)&& mycat.getMySQLSession() == mySQLSession && mySQLSession.getMycat() == mycat) {
                            mySQLClientSessionSessionCallBack.onSession(mySQLSession, null, null);
                            return;
                        } else {
                            mySQLClientSessionSessionCallBack.onException(new Exception("is binding"), null, null);
                            return;
                        }
                    } else {
                        mySQLSessionManager.getIdleSessionsOfKey(datasource, mySQLClientSessionSessionCallBack);
                    }
                };
                NativeMycatServer mycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
                MySQLDatasource datasource = mycatServer.getDatasource(datasourceName);
                getSession.accept(datasource, new SessionCallBack<MySQLClientSession>() {
                    @Override
                    public void onSession(MySQLClientSession session, Object sender, Object attr) {
                        MycatMonitor.onRouteResult(mycat, datasource.getName(), datasource.getName(), datasource.getName(), packetData);
                        SessionCallBack<MySQLClientSession> sessionCallBack = new SessionCallBack<MySQLClientSession>() {
                            @Override
                            public void onSession(MySQLClientSession session, Object sender, Object attr) {
                                PromiseInternal<Void> proxyBackend = MySQLPacketExchanger.MySQLProxyNIOHandler.INSTANCE.proxyBackend(session, finallyCallBack, ResponseType.QUERY, mycat, packetData);
                                // todo 异步未实现完全 wangzihaogithub
                                proxyBackend.onComplete(o-> promise.tryComplete());
                            }

                            @Override
                            public void onException(Exception exception, Object sender, Object attr) {
                                promise.tryFail(exception);
                                finallyCallBack.onRequestMySQLException(mycat, exception, null);
                            }
                        };
                        if (transactionType.expect(session.isAutomCommit(), session.isMonopolizedByTransaction())) {
                            sessionCallBack.onSession(session, this, null);
                        } else {
                            syncState(session, transactionType, isolation, sessionCallBack);
                        }
                    }

                    @Override
                    public void onException(Exception exception, Object sender, Object attr) {
                        promise.tryFail(exception);
                        finallyCallBack.onRequestMySQLException(mycat, exception, attr);
                    }
                });
            }

            @Override
            public void stop(ReactorEnvThread reactor, Exception reason) {
                mycat.setLastMessage(reason);
                // todo 异步未实现完全 wangzihaogithub
                PromiseInternal<Void> proxyBackend = mycat.writeErrorEndPacketBySyncInProcessError();
                promise.tryFail(reason);
            }

            @Override
            public String message() {
                return "proxyBackendByDataSource";
            }
        });

        // todo 异步未实现完全 wangzihaogithub
        return promise;
    }

    @ToString
    public enum TransactionSyncType {
        SET_AUTOCOMMIT_ON(MySQLAutoCommit.ON, false, "set autocommit = 1;"),
        SET_AUTOCOMMIT_ON_BEGIN(MySQLAutoCommit.ON, true, "set autocommit = 1;begin;"),
        SET_AUTOCOMMIT_OFF(MySQLAutoCommit.OFF, true, "set autocommit = 0;begin;");
        MySQLAutoCommit automCommit;
        boolean inTransaction;
        String text;

        TransactionSyncType(MySQLAutoCommit automCommit, boolean inTransaction, String text) {
            this.automCommit = automCommit;
            this.inTransaction = inTransaction;
            this.text = text;
        }

        public boolean expect(MySQLAutoCommit automCommit, boolean inTransaction) {
            return this.automCommit == automCommit && this.inTransaction == inTransaction;
        }

        public static TransactionSyncType create(boolean autoCommit, boolean inTransaction) {
            return create(autoCommit ? MySQLAutoCommit.ON : MySQLAutoCommit.OFF, inTransaction);
        }

        public static TransactionSyncType create(MySQLAutoCommit autoCommit, boolean inTransaction) {
            if (autoCommit == MySQLAutoCommit.ON) {
                if (inTransaction) {
                    return SET_AUTOCOMMIT_ON_BEGIN;
                } else {
                    return SET_AUTOCOMMIT_ON;
                }
            } else {
                if (!inTransaction) {
                    return SET_AUTOCOMMIT_OFF;
                } else {
                    return SET_AUTOCOMMIT_ON_BEGIN;
                }
            }
        }
    }

    private static void syncState(MySQLClientSession session, TransactionSyncType transactionType, MySQLIsolation isolation, SessionCallBack<MySQLClientSession> callBack) {
        ResultSetHandler.DEFAULT.request(session, MySQLCommandType.COM_QUERY, isolation.getCmd() + transactionType.text, new ResultSetCallBack<MySQLClientSession>() {
            @Override
            public void onFinishedSendException(Exception exception, Object sender, Object attr) {
                callBack.onException(exception, sender, attr);
            }

            @Override
            public void onFinishedException(Exception exception, Object sender, Object attr) {
                callBack.onException(exception, sender, attr);
            }

            @Override
            public void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender, Object attr) {
                session.setIsolation(isolation);
                if (transactionType.expect(session.isAutomCommit(), session.isMonopolizedByTransaction())) {
                    callBack.onSession(mysql, sender, attr);
                } else {
                    callBack.onException(new MycatException("sync state " + transactionType + " " + isolation), sender, attr);
                }

            }

            @Override
            public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize, MySQLClientSession mysql, Object sender, Object attr) {
                error(mysql, sender, attr, errorPacket.getErrorMessageString());
            }

            private void error(MySQLClientSession mysql, Object sender, Object attr, String message) {
                LOGGER.error(message);
                mysql.close(false, message);
                callBack.onException(new MycatException(message), sender, attr);
            }
        });
    }


    public static void getMySQLSessionForTryConnect(String datasource,
                                                    SessionCallBack<MySQLClientSession> asynTaskCallBack) {
        Objects.requireNonNull(datasource);
        Objects.requireNonNull(asynTaskCallBack);
        Thread thread = Thread.currentThread();
        if (thread instanceof MycatReactorThread) {
            MySQLSessionManager manager = ((MycatReactorThread) thread)
                    .getMySQLSessionManager();
            NativeMycatServer mycatServer = MetaClusterCurrent.wrapper(NativeMycatServer.class);
            manager.getIdleSessionsOfIdsOrPartial(
                    mycatServer.getDatasource(datasource), null, PartialType.SMALL_ID
                    , asynTaskCallBack);
        } else {
            throw new MycatException("Replica must running in MycatReactorThread");
        }
    }


    public static void proxyBackend(MycatSession session, String sql) {
        MySQLClientSession mySQLSession = session.getMySQLSession();
        MySQLPacketExchanger.MySQLProxyNIOHandler.INSTANCE.
                proxyBackend(mySQLSession, DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK, ResponseType.QUERY, session, MySQLPacketUtil.generateComQueryPacket(sql));
    }

}
