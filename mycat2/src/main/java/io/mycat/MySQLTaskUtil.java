/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat;

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
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.SessionManager.PartialType;
import io.mycat.proxy.session.SessionManager.SessionIdAble;
import io.mycat.beans.MySQLDatasource;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

import static io.mycat.proxy.handler.MySQLPacketExchanger.DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK;

/**
 * @author jamie12221 date 2019-05-12 22:41 dataNode执行器 该类本意是从路由获得dataNode名字之后,使用该执行器执行,
 * 解耦结果类和实际执行方法
 **/
public class MySQLTaskUtil {
    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLTaskUtil.class);
//    public static void proxyBackend(MycatSession mycat, String sql, String targetName,String databaseName,
//                                    MySQLDataSourceQuery query) {
//        MycatMonitor.onRouteSQL(mycat, targetName,databaseName, sql);
//        MySQLPacketExchanger.INSTANCE
//                .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(sql), targetName,databaseName, query, ResponseType.QUERY);
//    }

    //todo
    public static void proxyBackendByTargetName(MycatSession mycat,
                                                String target,
                                                String sql,
                                                boolean transaction,
                                                MySQLIsolation isolation,
                                                boolean master,
                                                String loadBalanceStrategy) {
        //todo fix the log
        if (transaction) {
            master = true;
        }
        String datasourceName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(target, master, loadBalanceStrategy);
        if (datasourceName == null) throw new MycatException("{} is not existed", target);
        LOGGER.debug("session id:{} proxy target:{},sql:{},transaction:{},isolation:{},master:{},balance:{}",
                mycat.sessionId(), target, sql, transaction, isolation, master, loadBalanceStrategy);
        proxyBackendByDatasourceName(mycat, sql, datasourceName, transaction, isolation);
    }

    public static void proxyBackendByDatasourceName(MycatSession mycat,
                                                    String sql,
                                                    String datasourceName,
                                                    boolean transaction,
                                                    MySQLIsolation isolation) {
        //todo fix the log
        proxyBackendByDataSource(mycat,
                MySQLPacketUtil.generateComQueryPacket(sql),
                datasourceName,
                ResponseType.QUERY,
                MySQLPacketExchanger.MySQLProxyNIOHandler.INSTANCE,
                DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK,
                transaction, isolation);
    }

    public static void proxyBackendByDataSource(MycatSession mycat,
                                                byte[] packetData,
                                                String datasourceName,
                                                ResponseType responseType,
                                                MySQLPacketExchanger.MySQLProxyNIOHandler proxyNIOHandler,
                                                MySQLPacketExchanger.PacketExchangerCallback finallyCallBack,
                                                boolean needTransaction,
                                                MySQLIsolation isolation) {
        assert (Thread.currentThread() instanceof MycatReactorThread);
        Objects.requireNonNull(datasourceName);
        MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
        MySQLSessionManager mySQLSessionManager = reactor.getMySQLSessionManager();
        BiConsumer<MySQLDatasource, SessionCallBack<MySQLClientSession>> getSession = (datasource, mySQLClientSessionSessionCallBack) -> {
            if (mycat.isBindMySQLSession()) {
                MySQLClientSession mySQLSession = mycat.getMySQLSession();
                if (datasourceName.equals(mySQLSession.getDatasource().getName()) && mycat.getMySQLSession() == mySQLSession && mySQLSession.getMycat() == mycat) {
                    mySQLClientSessionSessionCallBack.onSession(mySQLSession,null,null);
                } else {
                    mySQLClientSessionSessionCallBack.onException(new Exception("is binding"),null,null);
                }
            }else {
                if (mycat.isBindMySQLSession()){
                    throw new AssertionError();
                }
                mySQLSessionManager.getIdleSessionsOfKey(datasource,mySQLClientSessionSessionCallBack);
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
                if (needTransaction){
                    begin(session, isolation, sessionCallBack);
                }else {
                    sessionCallBack.onSession(session,this,null);
                }
            }

            @Override
            public void onException(Exception exception, Object sender, Object attr) {
                finallyCallBack.onRequestMySQLException(mycat,exception, attr);
            }
        });
    }

    //todo
    public static void proxyBackend(MycatSession mycat, byte[] payload, String dataNodeName,
                                    MySQLDataSourceQuery query, ResponseType responseType) {
//        MySQLPacketExchanger.INSTANCE
//                .proxyBackend(mycat, payload, dataNodeName, query, responseType);
    }

    public static void proxyBackendWithCollector(MycatSession mycat, byte[] payload,
                                                 String dataNodeName,
                                                 MySQLDataSourceQuery query, ResponseType responseType,
                                                 MySQLPacketCallback callback) {
//        MySQLPacketExchanger.INSTANCE
//                .proxyWithCollectorCallback(mycat, payload, dataNodeName, query, responseType, callback);
    }

    public static void withBackend(MycatSession mycat, byte[] payload,
                                   String dataNodeName,
                                   MySQLDataSourceQuery query,
                                   ResponseType responseType,
                                   MySQLPacketCallback callback) {
//        MySQLPacketExchanger.INSTANCE
//                .proxyWithCollectorCallback(mycat, payload, dataNodeName, query, responseType, callback);
    }

    public static void withBackend(MycatSession mycat, String replicaName, String databaseName,
                                   MySQLDataSourceQuery query,
                                   SessionSyncCallback finallyCallBack) {
//        mycat.currentProxyBuffer().reset();
//        if (mycat.getMySQLSession() != null) {
//            if ((mycat.getMySQLSession().getDefaultDatabase().equals(databaseName))) {
//                //只要backend还有值,就说明上次命令因为事务或者遇到预处理,loadata这种跨多次命令的类型导致mysql不能释放
//                finallyCallBack.onSession(mycat.getMySQLSession(), MySQLPacketExchanger.INSTANCE, null);
//                return;
//            } else {
//                finallyCallBack.onException(new MycatException("in bound state"), MySQLPacketExchanger.INSTANCE, null);
//                return;
//            }
//        }
//        MySQLSynContext mycatSynContext = mycat.getRuntime().getProviders()
//                .createMySQLSynContext(mycat);
//        MySQLTaskUtil
//                .getMySQLSession(mycatSynContext, query, finallyCallBack);
    }

    /**
     * 用户在非mycat reactor 线程获取 session
     * <p>
     * 回调执行的函数处于mycat reactor thread 所以不能编写长时间执行的代码
     */
    public static void getMySQLSessionFromUserThread(MySQLSynContextImpl synContext,
                                                     MySQLDataSourceQuery query,
                                                     SessionSyncCallback asynTaskCallBack) {
//        MycatReactorThread[] threads = runtime.getMycatReactorThreads();
//        int i = ThreadLocalRandom.current().nextInt(0, threads.length);
//        threads[i].addNIOJob(new NIOJob() {
//            @Override
//            public void run(ReactorEnvThread reactor) throws Exception {
//                getMySQLSession(synContext, query, asynTaskCallBack);
//            }
//
//            @Override
//            public void stop(ReactorEnvThread reactor, Exception reason) {
//                asynTaskCallBack.onException(reason, this, null);
//            }
//
//            @Override
//            public String message() {
//                return "getMySQLSessionFromUserThread";
//            }
//        });
    }

    private static void setIsolation(MySQLClientSession session, MySQLIsolation isolation, SessionCallBack<MySQLClientSession> callBack) {
        if (session.getIsolation() != isolation) {
            ResultSetHandler.DEFAULT.request(session, MySQLCommandType.COM_QUERY, isolation.getCmd(), new ResultSetCallBack<MySQLClientSession>() {
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
                    callBack.onSession(mysql, sender, attr);
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
        } else {
            callBack.onSession(session, null, null);
        }
    }

    public static void begin(MySQLClientSession session, MySQLIsolation isolation, SessionCallBack<MySQLClientSession> callBack) {
        switch (session.getMonopolizeType()) {
            case TRANSACTION:
                setIsolation(session, isolation, callBack);
                return;
            default:
                break;
        }
        setIsolation(session, isolation, new SessionCallBack<MySQLClientSession>() {
            @Override
            public void onSession(MySQLClientSession session, Object sender, Object attr) {
                ResultSetHandler.DEFAULT.request(session, MySQLCommandType.COM_QUERY, "begin", new ResultSetCallBack<MySQLClientSession>() {
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
                        callBack.onSession(mysql, sender, attr);
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

            @Override
            public void onException(Exception exception, Object sender, Object attr) {
                callBack.onException(exception, sender, attr);
            }
        });

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

    public static void getMySQLSessionForTryConnectFromUserThread(
            MySQLDatasource datasource,
            List<SessionIdAble> ids,
            PartialType partialType,
            SessionCallBack<MySQLClientSession> asynTaskCallBack) {
//        MycatReactorThread[] threads = runtime.getMycatReactorThreads();
//        int i = ThreadLocalRandom.current().nextInt(0, threads.length);
//        threads[i].addNIOJob(new NIOJob() {
//            @Override
//            public void run(ReactorEnvThread reactor) throws Exception {
//                getMySQLSessionForTryConnect(datasource, ids, partialType, asynTaskCallBack);
//            }
//
//            @Override
//            public void stop(ReactorEnvThread reactor, Exception reason) {
//                asynTaskCallBack.onException(reason, this, null);
//            }
//
//            @Override
//            public String message() {
//                return "getMySQLSessionForTryConnectFromUserThread";
//            }
//        });
    }

    public static void getMySQLSessionForTryConnectFromUserThreadByPartialSessionId(
            PartialType partialType,
            MySQLDatasource datasource,
            SessionCallBack<MySQLClientSession> asynTaskCallBack) {
//        MycatReactorThread[] threads = runtime.getMycatReactorThreads();
//        int i = ThreadLocalRandom.current().nextInt(0, threads.length);
//        threads[i].addNIOJob(new NIOJob() {
//            @Override
//            public void run(ReactorEnvThread reactor) throws Exception {
//                getMySQLSessionForTryConnect(datasource, null, partialType, asynTaskCallBack);
//            }
//
//            @Override
//            public void stop(ReactorEnvThread reactor, Exception reason) {
//                asynTaskCallBack.onException(reason, this, null);
//            }
//
//            @Override
//            public String message() {
//                return "getMySQLSessionForTryConnectFromUserThread";
//            }
//        });
    }

    public static void proxyBackend(MycatSession session, String sql) {
        MySQLClientSession mySQLSession = session.getMySQLSession();
        MySQLPacketExchanger.MySQLProxyNIOHandler.INSTANCE.
                proxyBackend(mySQLSession, DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK, ResponseType.QUERY, session, MySQLPacketUtil.generateComQueryPacket(sql));
    }
//    public void proxyBackend(MycatSession mycat, byte[] payload, String targetName,String defaultDatabase,
//                             MySQLDataSourceQuery query, ResponseType responseType) {
//        proxyBackend(mycat, payload, targetName,defaultDatabase, query, responseType,
//                DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK);
//
//    }

    //    public void proxyBackend(MycatSession mycat, byte[] payload, String targetName,String defaultDatabase,
//                             MySQLDataSourceQuery query, ResponseType responseType, MySQLPacketExchanger.PacketExchangerCallback finallyCallBack) {
//        byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
//        MySQLPacketExchanger.MySQLProxyNIOHandler
//                .INSTANCE.proxyBackend(mycat, bytes, targetName,defaultDatabase, query, responseType,
//                MySQLPacketExchanger.MySQLProxyNIOHandler.INSTANCE, finallyCallBack
//        );
//    }
//
//    public void proxyBackendWithRawPacket(MycatSession mycat, byte[] packet, String targetName,String defaultDatabase,
//                                          MySQLDataSourceQuery query, ResponseType responseType) {
//        MySQLPacketExchanger.MySQLProxyNIOHandler
//                .INSTANCE.proxyBackend(mycat, packet, targetName,defaultDatabase, query, responseType,
//                MySQLPacketExchanger.MySQLProxyNIOHandler.INSTANCE, DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK
//        );
//    }
//
//    public void proxyWithCollectorCallback(MycatSession mycat, byte[] payload, String targetName,String defaultDatabase,
//                                           MySQLDataSourceQuery query, ResponseType responseType, MySQLPacketCallback callback) {
//        proxyWithCollectorCallback(mycat, payload, targetName,defaultDatabase, query, responseType, callback,
//                DEFAULT_BACKEND_SESSION_REQUEST_FAILED_CALLBACK);
//    }
//
//    public void proxyWithCollectorCallback(MycatSession mycat, byte[] payload, String targetName,String defaultDatabase,
//                                           MySQLDataSourceQuery query, ResponseType responseType, MySQLPacketCallback callback,
//                                           MySQLPacketExchanger.PacketExchangerCallback finallyCallBack) {
//        byte[] bytes = MySQLPacketUtil.generateMySQLPacket(0, payload);
//        MySQLPacketExchanger.MySQLProxyNIOHandler
//                .INSTANCE.proxyBackend(mycat, bytes, targetName,defaultDatabase, query, responseType,
//                new MySQLPacketExchanger.MySQLCollectorExchanger(callback), finallyCallBack
//        );
//    }
//
    public void proxyBackend(MycatSession mycat, byte[] packetData, String replicaName, String defaultDatabaseName,
                             MySQLDataSourceQuery query, ResponseType responseType, MySQLPacketExchanger.MySQLProxyNIOHandler proxyNIOHandler,
                             MySQLPacketExchanger.PacketExchangerCallback finallyCallBack) {
        MySQLTaskUtil.withBackend(mycat, replicaName, defaultDatabaseName, query, new SessionSyncCallback() {
            @Override
            public void onSession(MySQLClientSession mysql, Object sender, Object attr) {
                MySQLDatasource datasource = mysql.getDatasource();
                MycatMonitor.onRouteResult(mycat, replicaName, defaultDatabaseName, datasource.getName(), packetData);
                proxyNIOHandler.proxyBackend(mysql, finallyCallBack, responseType, mycat, packetData);
            }

            @Override
            public void onException(Exception exception, Object sender, Object attr) {
                MycatMonitor.onGettingBackendException(mycat, replicaName, defaultDatabaseName, exception);
                finallyCallBack.onRequestMySQLException(mycat, exception, attr);
            }

            @Override
            public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                                      MySQLClientSession mysql, Object sender, Object attr) {
                finallyCallBack.onRequestMySQLException(mycat,
                        new MycatException(errorPacket.getErrorMessageString()), attr);
            }
        });
    }

}
