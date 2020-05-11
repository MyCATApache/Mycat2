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
package io.mycat;

import io.mycat.beans.MySQLDatasource;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.MySQLPacketExchanger;
import io.mycat.proxy.handler.ResponseType;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.handler.backend.MySQLSynContextImpl;
import io.mycat.proxy.handler.backend.ResultSetHandler;
import io.mycat.proxy.handler.backend.SessionSyncCallback;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.MySQLPacketCallback;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.PartialType;
import io.mycat.proxy.session.SessionManager.SessionIdAble;
import io.mycat.replica.ReplicaSelectorRuntime;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static io.mycat.proxy.handler.MySQLPacketExchanger.DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK;

/**
 * @author jamie12221 date 2019-05-12 22:41 dataNode执行器 该类本意是从路由获得dataNode名字之后,使用该执行器执行,
 * 解耦结果类和实际执行方法
 **/
public class MySQLTaskUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLTaskUtil.class);

    public static void proxyBackendByDatasourceName(MycatSession mycat,
                                                    String datasourceName,
                                                    String sql,
                                                    TransactionSyncType transaction,
                                                    MySQLIsolation isolation) {
        if (ReplicaSelectorRuntime.INSTANCE.isDatasource(datasourceName)) {
            throw new AssertionError("target must be datasource:" + datasourceName);
        }
        proxyBackendByDatasourceName(mycat, sql,
                datasourceName,
                DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK,
                transaction, isolation);
    }

    public static void proxyBackendByDatasourceName(MycatSession mycat,
                                                String datasourceName,
                                                String sql,
                                                MySQLPacketExchanger.PacketExchangerCallback finallyCallBack,
                                                TransactionSyncType transactionType,
                                                MySQLIsolation isolation) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("session id:{} proxy target:{},sql:{},transaction:{},isolation:{}",
                    mycat.sessionId(), datasourceName, sql, transactionType, isolation);
        }

        byte[] packetData = MySQLPacketUtil.generateComQueryPacket(sql);

        Objects.requireNonNull(datasourceName);
        mycat.switchProxyWriteHandler();
        mycat.getIOThread().addNIOJob(new NIOJob() {
            @Override
            public void run(ReactorEnvThread reactor2) throws Exception {
                MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
                MySQLSessionManager mySQLSessionManager = reactor.getMySQLSessionManager();
                BiConsumer<MySQLDatasource, SessionCallBack<MySQLClientSession>> getSession = (datasource, mySQLClientSessionSessionCallBack) -> {
                    if (mycat.isBindMySQLSession()) {
                        MySQLClientSession mySQLSession = mycat.getMySQLSession();
                        String currentDataSource = mySQLSession.getDatasourceName();
                        if (datasourceName.equals(currentDataSource) && mycat.getMySQLSession() == mySQLSession && mySQLSession.getMycat() == mycat) {
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
                MySQLDatasource datasource = MycatCore.INSTANCE.getDatasource(datasourceName);
                getSession.accept(datasource, new SessionCallBack<MySQLClientSession>() {
                    @Override
                    public void onSession(MySQLClientSession session, Object sender, Object attr) {
                        MycatMonitor.onRouteResult(mycat, datasource.getName(), datasource.getName(), datasource.getName(), packetData);
                        SessionCallBack<MySQLClientSession> sessionCallBack = new SessionCallBack<MySQLClientSession>() {
                            @Override
                            public void onSession(MySQLClientSession session, Object sender, Object attr) {
                                MySQLPacketExchanger.MySQLProxyNIOHandler.INSTANCE.proxyBackend(session, finallyCallBack, ResponseType.QUERY, mycat, packetData);
                            }

                            @Override
                            public void onException(Exception exception, Object sender, Object attr) {
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
                        finallyCallBack.onRequestMySQLException(mycat, exception, attr);
                    }
                });
            }

            @Override
            public void stop(ReactorEnvThread reactor, Exception reason) {
                mycat.setLastMessage(reason);
                mycat.writeErrorEndPacketBySyncInProcessError();
            }

            @Override
            public String message() {
                return "proxyBackendByDataSource";
            }
        });

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
            manager.getIdleSessionsOfIdsOrPartial(
                    MycatCore.INSTANCE.getDatasource(datasource), null, PartialType.SMALL_ID
                    , asynTaskCallBack);
        } else {
            throw new MycatException("Replica must running in MycatReactorThread");
        }
    }

    public static void getMySQLSessionForTryConnect(MySQLDatasource datasource,
                                                    List<SessionIdAble> ids,
                                                    PartialType partialType,
                                                    SessionCallBack<MySQLClientSession> asynTaskCallBack) {
        Objects.requireNonNull(datasource);
        Objects.requireNonNull(asynTaskCallBack);
        Thread thread = Thread.currentThread();
        if (thread instanceof MycatReactorThread) {
            MySQLSessionManager manager = ((MycatReactorThread) thread)
                    .getMySQLSessionManager();
            manager.getIdleSessionsOfIdsOrPartial(
                    datasource, ids, partialType
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
