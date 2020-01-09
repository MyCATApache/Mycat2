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
package io.mycat.proxy.session;

import io.mycat.GlobalConst;
import io.mycat.MycatException;
import io.mycat.api.collector.OneResultSetCollector;
import io.mycat.api.collector.TextResultSetTransforCollector;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.packet.ErrorPacketImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.callback.CommandCallBack;
import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.backend.*;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.SessionManager.BackendSessionManager;
import io.mycat.beans.MySQLDatasource;
import io.mycat.util.StringUtil;
import io.mycat.util.nio.NIOUtil;

import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

import static io.mycat.beans.mysql.MySQLCommandType.COM_QUERY;

/**
 * 集中管理MySQL LocalInFileSession 是在mycat proxy中,唯一能够创建mysql session以及关闭mysqlsession的对象
 * 该在一个线程单位里,对象生命周期应该是单例的
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public class MySQLSessionManager implements
        BackendSessionManager<MySQLClientSession, MySQLDatasource> {

    final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLSessionManager.class);
    final HashMap<Integer, MySQLClientSession> allSessions = new HashMap<>();
    final HashMap<MySQLDatasource, LinkedList<MySQLClientSession>> idleDatasourcehMap = new HashMap<>();
    final HashMap<Integer, MySQLPayloadWriter> clearTask = new HashMap<>();

//  private ProxyRuntime runtime;

    public MySQLSessionManager() {
    }

    /**
     * 返回不可变集合,防止外部代码错误操作allSessions导致泄漏MySQLSession
     */
    @Override
    public final List<MySQLClientSession> getAllSessions() {
        return new ArrayList<>(allSessions.values());
    }

    /**
     * 获得mysql proxy中,该线程中所有的mysqlSession的数量
     */
    @Override
    public final int currentSessionCount() {
        return allSessions.size();
    }


    @Override
    public void getIdleSessionsOfIdsOrPartial(MySQLDatasource datasource, List<SessionIdAble> ids,
                                              PartialType partialType,
                                              SessionCallBack<MySQLClientSession> asyncTaskCallBack) {
        Objects.requireNonNull(datasource);

        try {
            for (; ; ) {
                MySQLClientSession mySQLSession = getIdleMySQLClientSessionsByIds(datasource, ids, partialType);
                if (mySQLSession == null ) {
                    createSession(datasource, asyncTaskCallBack);
                    return;
                }
                if (!mySQLSession.checkOpen()) {
                    continue;
                }
                assert mySQLSession.getCurNIOHandler() == IdleHandler.INSTANCE;
                assert mySQLSession.currentProxyBuffer() == null;
                mySQLSession.setIdle(false);
                mySQLSession.switchNioHandler(null);
                MycatMonitor.onGetIdleMysqlSession(mySQLSession);
                if (shouldClear(mySQLSession)) {
                    continue;
                }
                asyncTaskCallBack.onSession(mySQLSession, this, null);
                return;
            }
        } catch (Exception e) {
            LOGGER.error("", e);
            asyncTaskCallBack
                    .onException(e, this,
                            null);
        }
    }

    /**
     * @param ids 如果id失效 设置为-id
     */
    public MySQLClientSession getIdleMySQLClientSessionsByIds(MySQLDatasource datasource,
                                                              List<SessionIdAble> ids, PartialType partialType) {
        MySQLClientSession session = null;
        //dataSource
        if (datasource != null && (ids == null || ids.isEmpty())) {
            LinkedList<MySQLClientSession> group = this.idleDatasourcehMap.get(datasource);
            for (; ; ) {
                if (group == null || group.isEmpty()) {
                    return null;
                }

                if (partialType == PartialType.RANDOM_ID || partialType == null) {
                    boolean random = ThreadLocalRandom.current().nextBoolean();
                    session = random ? group.removeFirst() : group.removeLast();
                } else {
                    group.sort(Comparator.comparing(AbstractSession::sessionId));
                    switch (partialType) {
                        case SMALL_ID:
                            session = group.removeFirst();
                            break;
                        case LARGE_ID:
                            session = group.removeLast();
                            break;
                    }
                }
                return session;
            }
        }
        //dataSource ids
        else if (datasource != null && ids != null) {
            session = searchMap(ids, this.allSessions);
        }
        //ids
        else if (ids != null && ids.size() > 0) {
            session = searchMap(ids, this.allSessions);
        }
        return session;
    }


    private MySQLClientSession searchMap(List<SessionIdAble> ids,
                                         Map<Integer, MySQLClientSession> source) {
        int size = ids.size();
        for (int i = 0; i < size; i++) {
            int id = ids.get(i).getSessionId();
            MySQLClientSession mySQLClientSession = source.get(id);
            if (mySQLClientSession.isIdle()) {
                LinkedList<MySQLClientSession> sessions = this.idleDatasourcehMap
                        .get(mySQLClientSession.getDatasource());
                sessions.remove(mySQLClientSession);
                return mySQLClientSession;
            }
        }
        return null;
    }


    /**
     * 根据dataSource的配置信息获得可用的MySQLSession 0.如果dataSource已经失效,则直接回调异常,异常信息在callback的attr
     * 1.首先从空闲的session集合里面尝试获取 若存在空闲的session则随机从头部或者尾部获得session 2.否则创建新的session
     */

    @Override
    public final void getIdleSessionsOfKey(MySQLDatasource datasource,
                                           SessionCallBack<MySQLClientSession> asyncTaskCallBack) {
        getIdleSessionsOfIdsOrPartial(datasource, null, PartialType.RANDOM_ID, asyncTaskCallBack);
    }

    /**
     * 把使用完毕的mysql session释放到连接池 1.禁止把session多次放入闲置连接池,但是该方法不查重,需要调用方保证
     * 2.在闲置连接池里面,session不对写入事件响应,但对读取事件响应,因为可能收到关闭事件
     */
    @Override
    public final void addIdleSession(MySQLClientSession session) {
        try {
            /**
             * mycat对应透传模式对mysql session的占用
             * niohandler对应透传以及task类对mysql session的占用
             */
            assert session.getMycat() == null;
            assert !session.hasClosed();
            assert session.currentProxyBuffer() == null;
            assert !session.isIdle();
            /////////////////////////////////////////

            if (shouldClear(session)) {
                return;
            }
            //////////////////////////////////////////////////
            session.setCursorStatementId(-1);
            session.resetPacket();
            session.setIdle(true);
            session.switchNioHandler(IdleHandler.INSTANCE);
            session.change2ReadOpts();
            idleDatasourcehMap.computeIfAbsent(session.getDatasource(), (l) -> new LinkedList<>()).add(session);
            MycatMonitor.onAddIdleMysqlSession(session);
        } catch (Exception e) {
            LOGGER.error("{}", e);
            session.close(false, e);
        }
    }

    private boolean shouldClear(MySQLClientSession session) {
        MySQLPayloadWriter clearPayload = clearTask.remove(session.sessionId());
        if (clearPayload != null) {
            RequestHandler.INSTANCE.request(session, clearPayload.toByteArray(), new RequestCallback() {
                @Override
                public void onFinishedSend(MySQLClientSession session, Object sender, Object attr) {
                    addIdleSession(session);
                }

                @Override
                public void onFinishedSendException(Exception e, Object sender, Object attr) {
                }
            });
            return true;
        }
        if (session.isMonopolized()) {
            LOGGER.error("sessionId {} is monopolized", session.sessionId());
            session.close(false, "isMonopolized");
            return true;
        }
        return false;
    }

    public void appendClearRequest(int sessionId, byte[] packet) {
        CheckResult check = check(sessionId);
        switch (check) {
            case NOT_EXIST:
                break;
            case IDLE:
            case BUSY:
                this.clearTask.compute(sessionId,
                        (integer, writer1) -> {
                            if (writer1 == null) {
                                writer1 = new MySQLPayloadWriter();
                            }
                            writer1.writeBytes(packet);
                            return writer1;
                        });
                break;
        }
    }


    /**
     * 1.从闲置池里面移除mysql session 2.该函数不会关闭session 3.该函数可以被子类重写,但是未能遇见这种需要
     */
    private void removeIdleSession(MySQLClientSession session) {
        try {
            assert session != null;
            assert session.getDatasource() != null;
            LinkedList<MySQLClientSession> mySQLSessions = idleDatasourcehMap
                    .get(session.getDatasource());
            if (mySQLSessions != null) {
                mySQLSessions.remove(session);
            }
        } catch (Exception e) {
            LOGGER.error("{}", e);
        }
    }

    /**
     * 1.可能被以下需要调用 a.定时清理某个dataSource的连接 b.强制关闭摸个dataSource的连接
     *//**/
    @Override
    public final void clearAndDestroyDataSource(MySQLDatasource key, String reason) {
        assert key != null;
        assert reason != null;
        Collection<MySQLClientSession> allSessions = Collections
                .unmodifiableCollection(this.allSessions.values());
        for (MySQLClientSession s : allSessions) {
            if (s.getDatasource().equals(key)) {
                this.allSessions.remove(s.sessionId());
            }
        }
        LinkedList<MySQLClientSession> sessions = idleDatasourcehMap.get(key);
        if (sessions != null) {
            for (MySQLClientSession session : sessions) {
                try {
                    session.close(true, reason);
                } catch (Exception e) {
                    LOGGER.error("mysql session is closing but occur error", e);
                }
            }
        }
        idleDatasourcehMap.remove(key);
    }

    /*
      1.给持有的连接发送心跳
      2.关闭事务超时的连接
      3.超过了最小的连接数, 关闭多余的连接
      4.创建连接 保持最小的连接数
     */
    @Override
    public void idleConnectCheck() {
        MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
        idleDatasourcehMap.forEach((mySQLDatasource, v) -> {
            if (v == null) {
                return;
            }
            long idleTimeout = mySQLDatasource.getIdleTimeout();
            long hearBeatTime = System.currentTimeMillis() - idleTimeout;
            long hearBeatTime2 = System.currentTimeMillis() - 2 * idleTimeout;
            int maxConsInOneCheck = Math.min(10, mySQLDatasource.getSessionMinCount());//每次最多检测10个，分多次检测
            LinkedList<MySQLClientSession> group = idleDatasourcehMap.get(mySQLDatasource);
            List<MySQLClientSession> checkList = new ArrayList<>();
            //发送心跳
            if (null != group) {
                checkIfNeedHeartBeat(hearBeatTime, hearBeatTime2, maxConsInOneCheck, group, checkList);
                for (MySQLClientSession mySQLClientSession : checkList) {
                    sendPing(mySQLClientSession);
                }
            }
            int idleCount = group == null ? 0 : group.size();
            int createCount = 0;
            if (mySQLDatasource.getSessionMinCount() > (idleCount + checkList.size())) {
                createCount = (mySQLDatasource.getSessionMinCount() - idleCount) / 3;
            }
            if (createCount > 0 && idleCount < mySQLDatasource.getSessionMinCount()) {
                createByLittle(mySQLDatasource, createCount);
            } else if (idleCount - checkList.size() > mySQLDatasource.getSessionMinCount()
                    && group != null) {
                //关闭多余连接
                closeByMany(mySQLDatasource,
                        idleCount - checkList.size() - mySQLDatasource.getSessionMinCount());
            }

        });

    }

    private void closeByMany(MySQLDatasource mySQLDatasource, int closeCount) {
        LinkedList<MySQLClientSession> group = this.idleDatasourcehMap.get(mySQLDatasource);
        for (int i = 0; i < closeCount; i++) {
            MySQLClientSession mySQLClientSession = group.removeFirst();
            if (mySQLClientSession != null) {
                closeSession(mySQLClientSession, "mysql session  close because of idle");
            }
        }
    }

    private void createByLittle(MySQLDatasource mySQLDatasource, int createCount) {
        for (int i = 0; i < createCount; i++) {
            //创建连接
            createSession(mySQLDatasource, new SessionCallBack<MySQLClientSession>() {
                @Override
                public void onSession(MySQLClientSession session, Object sender, Object attr) {
                    session.getSessionManager().addIdleSession(session);
                }

                @Override
                public void onException(Exception exception, Object sender, Object attr) {
                    //创建异常
                    MycatMonitor.onBackendConCreateException(null, exception);
                }
            });
        }
    }

    private void sendPing(MySQLClientSession session) {
        OneResultSetCollector collector = new OneResultSetCollector();
        TextResultSetTransforCollector transfor = new TextResultSetTransforCollector(collector);
        TextResultSetHandler queryResultSetTask = new TextResultSetHandler(transfor);
        queryResultSetTask
                .request(session, COM_QUERY, GlobalConst.SINGLE_NODE_HEARTBEAT_SQL,
                        new ResultSetCallBack<MySQLClientSession>() {
                            @Override
                            public void onFinishedSendException(Exception exception, Object sender,
                                                                Object attr) {
                                closeSession(session, "send Ping error");
                            }

                            @Override
                            public void onFinishedException(Exception exception, Object sender, Object attr) {
                                closeSession(session, "send Ping error");
                            }

                            @Override
                            public void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender,
                                                   Object attr) {
                                mysql.getSessionManager().addIdleSession(mysql);
                            }

                            @Override
                            public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                                                      MySQLClientSession mysql, Object sender, Object attr) {
                                closeSession(session, "send Ping error ");
                            }
                        });
    }

    private void checkIfNeedHeartBeat(long hearBeatTime, long hearBeatTime2, int maxConsInOneCheck,
                                      LinkedList<MySQLClientSession> group, List<MySQLClientSession> checkList) {
        Iterator<MySQLClientSession> iterator = group.iterator();
        while (iterator.hasNext()) {
            MySQLClientSession mySQLClientSession = iterator.next();
            //移除
            if (!mySQLClientSession.checkOpen()) {
                closeSession(mySQLClientSession, "mysql session  close because of idle");
                iterator.remove();
                continue;
            }
            long lastActiveTime = mySQLClientSession.getLastActiveTime();
            if (lastActiveTime < hearBeatTime
                    && checkList.size() < maxConsInOneCheck) {
                mySQLClientSession.setIdle(false);
                checkList.add(mySQLClientSession); //发送ping命令
                MycatMonitor.onGetIdleMysqlSession(mySQLClientSession);
                iterator.remove();

            } else if (lastActiveTime < hearBeatTime2) {
                closeSession(mySQLClientSession, "mysql session is close in idle");
                iterator.remove();
            }
        }
    }

    private void closeSession(MySQLClientSession mySQLClientSession, String hint) {
        mySQLClientSession.setIdle(false);
        MycatReactorThread mycatReactorThread = mySQLClientSession.getIOThread();
        mycatReactorThread.addNIOJob(new NIOJob() {
            @Override
            public void run(ReactorEnvThread reactor) throws Exception {
                mySQLClientSession.close(false, hint);
            }

            @Override
            public void stop(ReactorEnvThread reactor, Exception reason) {
                mySQLClientSession.close(false, hint);
            }

            @Override
            public String message() {
                return hint;
            }
        });
    }

    final static Exception SESSION_MAX_COUNT_LIMIT = new Exception("session max count limit");

    /**
     * 根据dataSource信息创建MySQL LocalInFileSession 1.这个函数并不会把连接加入到闲置的连接池,因为创建的session就是马上使用的,如果创建之后就加入闲置连接池就会发生挣用问题
     * 2.错误信息放置在attr
     */
    @Override
    public void createSession(MySQLDatasource key, SessionCallBack<MySQLClientSession> callBack) {
        assert key != null;
        assert callBack != null;
        if (!key.tryIncrementSessionCounter()) {
            callBack.onException(SESSION_MAX_COUNT_LIMIT, this, null);
            return;
        }
        int maxRetry = key.gerMaxRetry();
        createCon(key, new SessionCallBack<MySQLClientSession>() {
            int retryCount = 0;
            final long startTime = System.currentTimeMillis();

            @Override
            public void onSession(MySQLClientSession session, Object sender, Object attr) {
                callBack.onSession(session, sender, attr);
            }

            @Override
            public void onException(Exception exception, Object sender, Object attr) {
                long now = System.currentTimeMillis();
                long maxConnectTimeout = key.getMaxConnectTimeout();
                if (retryCount > maxRetry || startTime + maxConnectTimeout > now) {
                    callBack.onException(exception, sender, attr);
                } else {
                    ++retryCount;
                    long waitTime = (maxConnectTimeout + startTime - now) / (maxRetry - retryCount);//剩余时间减去剩余次数为下次重试间隔
                    MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
                    SessionCallBack<MySQLClientSession> sessionCallBack = this;
                    Runnable runnable = (() -> thread.addNIOJob(new NIOJob() {
                        @Override
                        public void run(ReactorEnvThread reactor) throws Exception {
                            createCon(key, sessionCallBack);
                        }

                        @Override
                        public void stop(ReactorEnvThread reactor, Exception reason) {
                            callBack.onException(reason, sender, attr);
                        }

                        @Override
                        public String message() {
                            return "waitTime";
                        }
                    }));
                    runnable.run();
                }
            }
        });
    }

    private void createCon(MySQLDatasource key,
                           SessionCallBack<MySQLClientSession> callBack) {
        new BackendConCreateHandler(key, this,
                (MycatReactorThread) Thread.currentThread(), new CommandCallBack() {
            @Override
            public void onFinishedOk(int serverStatus, MySQLClientSession session, Object sender,
                                     Object attr) {
                assert session.currentProxyBuffer() == null;
                MycatMonitor.onNewMySQLSession(session);
                MySQLDatasource datasource = session.getDatasource();
                String sql = datasource.getInitSQL();
                allSessions.put(session.sessionId(), session);
                if (!StringUtil.isEmpty(sql)) {
                    executeInitSQL(session, sql);
                } else {
                    callBack.onSession(session, sender, attr);
                }
            }

            public void executeInitSQL(MySQLClientSession session, String sql) {
                ResultSetHandler.DEFAULT.request(session, COM_QUERY,
                        sql.getBytes(),
                        new ResultSetCallBack<MySQLClientSession>() {
                            @Override
                            public void onFinishedSendException(Exception exception, Object sender,
                                                                Object attr) {
                                LOGGER.error("{}", exception);
                                callBack.onException(exception, sender, attr);
                            }

                            @Override
                            public void onFinishedException(Exception exception, Object sender, Object attr) {
                                LOGGER.error("{}", exception);
                                callBack.onException(exception, sender, attr);
                            }

                            @Override
                            public void onFinished(boolean monopolize, MySQLClientSession mysql, Object sender,
                                                   Object attr) {
                                if (monopolize) {
                                    String message = "mysql session is monopolized";
                                    mysql.close(false, message);
                                    callBack.onException(new MycatException(message), this, attr);
                                } else {
                                    callBack.onSession(mysql, this, attr);
                                }
                            }

                            @Override
                            public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                                                      MySQLClientSession mysql, Object sender, Object attr) {
                                String message = errorPacket.getErrorMessageString();
                                LOGGER.error(message);
                                mysql.close(false, message);
                                callBack.onException(new MycatException(message), sender, attr);
                            }
                        });
            }


            @Override
            public void onFinishedException(Exception exception, Object sender, Object attr) {
                key.decrementSessionCounter();
                callBack.onException(exception, sender, attr);
            }

            @Override
            public void onFinishedErrorPacket(ErrorPacketImpl errorPacket, int lastServerStatus,
                                              MySQLClientSession session, Object sender, Object attr) {
                key.decrementSessionCounter();
                callBack.onException(toExpection(errorPacket), sender, attr);
            }
        });
    }

    /**
     * 从连接池移除session 1.移除连接 2.关闭连接 3.关闭原因需要写清楚
     */
    @Override
    public void removeSession(MySQLClientSession session, boolean normal, String reason) {
        try {
            assert session != null;
            assert reason != null;
            session.getDatasource().decrementSessionCounter();
            allSessions.remove(session.sessionId());
            MycatMonitor.onCloseMysqlSession(session, normal, reason);
            removeIdleSession(session);
            NIOUtil.close(session.channel());
        } catch (Exception e) {
            LOGGER.error("{}", e);
        }
    }

    public CheckResult check(int sessionId) {
        MySQLClientSession session = allSessions.get(sessionId);
        if (session == null) {
            return CheckResult.NOT_EXIST;
        }
        if (session.isIdle()) {
            return CheckResult.IDLE;
        } else {
            return CheckResult.BUSY;
        }
    }

    /**
     * 根据MySQLDatasource获得MySQL Session 此函数是本类获取MySQL Session中最后一个必经的执行点,检验当前获得Session的线程是否MycatReactorThread
     */
    public void getSessionCallback(MySQLDatasource datasource, List<SessionManager.SessionIdAble> ids, Object sender,
                                   SessionCallBack<MySQLClientSession> asynTaskCallBack) {
        Objects.requireNonNull(datasource);
        Objects.requireNonNull(asynTaskCallBack);
        if (Thread.currentThread() instanceof MycatReactorThread) {
            MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
            reactor.getMySQLSessionManager()
                    .getIdleSessionsOfIdsOrPartial(datasource, ids, SessionManager.PartialType.RANDOM_ID, asynTaskCallBack);
        } else {
            MycatException mycatExpection = new MycatException(
                    "Replica must running in MycatReactorThread");
            asynTaskCallBack.onException(mycatExpection, sender, null);
            return;
        }
    }

}
