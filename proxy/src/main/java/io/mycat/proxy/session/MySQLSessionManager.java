/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.proxy.session;

import io.mycat.MycatExpection;
import io.mycat.annotations.NoExcept;
import io.mycat.beans.mysql.MySQLCommandType;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.collector.OneResultSetCollector;
import io.mycat.collector.TextResultSetTransforCollector;
import io.mycat.config.ConfigEnum;
import io.mycat.config.GlobalConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.logTip.SessionTip;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.CommandCallBack;
import io.mycat.proxy.callback.RequestCallback;
import io.mycat.proxy.callback.ResultSetCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.backend.BackendConCreateHandler;
import io.mycat.proxy.handler.backend.IdleHandler;
import io.mycat.proxy.handler.backend.RequestHandler;
import io.mycat.proxy.handler.backend.ResultSetHandler;
import io.mycat.proxy.handler.backend.TextResultSetHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.ErrorPacketImpl;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.SessionManager.BackendSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import io.mycat.util.nio.NIOUtil;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集中管理MySQL LocalInFileSession 是在mycat proxy中,唯一能够创建mysql session以及关闭mysqlsession的对象
 * 该在一个线程单位里,对象生命周期应该是单例的
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public final class MySQLSessionManager implements
    BackendSessionManager<MySQLClientSession, MySQLDatasource> {

  final static Logger LOGGER = LoggerFactory.getLogger(MySQLSessionManager.class);
  final HashMap<Integer, MySQLClientSession> allSessions = new HashMap<>();
  final HashMap<MySQLDatasource, LinkedList<MySQLClientSession>> idleDatasourcehMap = new HashMap<>();
  final HashMap<Integer, MySQLPayloadWriter> clearTask = new HashMap<>();

  /**
   * 返回不可变集合,防止外部代码错误操作allSessions导致泄漏MySQLSession
   */
  @NoExcept
  @Override
  public final Collection<MySQLClientSession> getAllSessions() {
    return Collections.unmodifiableCollection(allSessions.values());
  }

  /**
   * 获得mysql proxy中,该线程中所有的mysqlSession的数量
   */
  @NoExcept
  @Override
  public final int currentSessionCount() {
    return allSessions.size();
  }


  @Override
  public void getIdleSessionsOfIds(MySQLDatasource datasource, List<SessionIdAble> ids,
      SessionCallBack<MySQLClientSession> asyncTaskCallBack) {
    Objects.requireNonNull(datasource);
    MycatReactorThread thread = (MycatReactorThread) Thread.currentThread();
    try {
      /**
       * 1.如果集群不可用,直接回调不可用
       * 2.如果没有空闲连接,则创建空闲连接
       * 3.如果有空闲连接,则获取空闲连接,然后查看通道是否已经关闭,如果已经关闭,则继续尝试获取
       * 4.session管理不保证session一定可用
       */
      MySQLClientSession mySQLSession = getIdleMySQLClientSessionsByIds(datasource, ids);
      for (; ; ) {
        if (!datasource.isAlive()) {
          asyncTaskCallBack
              .onException(new MycatExpection(datasource.getName() + " is not alive!"), this,
                  null);
          return;
        }
        if (mySQLSession == null) {
          createSession(datasource, asyncTaskCallBack);
          return;
        } else {
          assert mySQLSession.getCurNIOHandler() == IdleHandler.INSTANCE;
          assert mySQLSession.currentProxyBuffer() == null;

          if (!mySQLSession.isOpen()) {
            thread.addNIOJob(() -> {
              mySQLSession.close(false, "mysql session is close in idle");
            });
            continue;
          }
          mySQLSession.setIdle(false);
          mySQLSession.switchNioHandler(null);
          MycatMonitor.onGetIdleMysqlSession(mySQLSession);
          if (shouldClear(mySQLSession)) {
            continue;
          }

          if (mySQLSession.isActivated()) {
            asyncTaskCallBack.onSession(mySQLSession, this, null);
            return;
          } else {
            LOGGER.error("because mysql sessionId:{} is not isActivated,so ping",
                mySQLSession.sessionId());
            ResultSetHandler.DEFAULT.request(mySQLSession, MySQLCommandType.COM_PING, new byte[]{},
                new ResultSetCallBack<MySQLClientSession>() {
                  @Override
                  public void onFinishedSendException(Exception exception, Object sender,
                      Object attr) {
                    LOGGER.error("", exception);
                  }

                  @Override
                  public void onFinishedException(Exception exception, Object sender, Object attr) {
                    LOGGER.error("", exception);
                  }

                  @Override
                  public void onFinished(boolean monopolize, MySQLClientSession mysql,
                      Object sender, Object attr) {
                    asyncTaskCallBack.onSession(mySQLSession, this, null);
                  }

                  @Override
                  public void onErrorPacket(ErrorPacketImpl errorPacket, boolean monopolize,
                      MySQLClientSession mysql, Object sender, Object attr) {
                    String errorMessageString = errorPacket.getErrorMessageString();
                    LOGGER.error(" {}");
                    mysql.close(false, errorMessageString);
                  }
                });
            break;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("",e);
      asyncTaskCallBack
          .onException(e, this,
              null);
    }
  }

  /**
   * @param ids 如果id失效 设置为-id
   */
  public MySQLClientSession getIdleMySQLClientSessionsByIds(MySQLDatasource datasource,
      List<SessionIdAble> ids) {
    boolean random = ThreadLocalRandom.current().nextBoolean();
    //dataSource
    if (datasource != null && (ids == null || ids.size() == 0)) {
      LinkedList<MySQLClientSession> group = this.idleDatasourcehMap.get(datasource);
      if (group==null||group.isEmpty()){
        return null;
      }
      return random ? group.removeFirst() : group.removeLast();
    }
    //dataSource ids
    if (datasource != null) {
      return searchMap(ids, this.allSessions);
    }
    //ids
    if (ids != null && ids.size() > 0) {
      return searchMap(ids, this.allSessions);
    }
    throw new IllegalArgumentException("mysql session search argument");
  }


  private MySQLClientSession searchMap(List<SessionIdAble> ids,
      Map<Integer, MySQLClientSession> source) {
    int size = ids.size();
    for (int i = 0; i < size; i++) {
      int id = ids.get(i).getSessionId();
      if (1 > id) {
        continue;
      }
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
  @NoExcept
  @Override
  public final void getIdleSessionsOfKey(MySQLDatasource datasource,
      SessionCallBack<MySQLClientSession> asyncTaskCallBack) {
    getIdleSessionsOfIds(datasource, null, asyncTaskCallBack);
  }

  /**
   * 把使用完毕的mysql session释放到连接池 1.禁止把session多次放入闲置连接池,但是该方法不查重,需要调用方保证
   * 2.在闲置连接池里面,session不对写入事件响应,但对读取事件响应,因为可能收到关闭事件
   */
  @NoExcept
  @Override
  public final void addIdleSession(MySQLClientSession session) {
    try {
      /**
       * mycat对应透传模式对mysql session的占用
       * niohandler对应透传以及task类对mysql session的占用
       */
      assert session.getMycatSession() == null;
      assert !session.isClosed();
      assert session.isActivated();
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
      idleDatasourcehMap.compute(session.getDatasource(), (k, l) -> {
        if (l == null) {
          l = new LinkedList<>();
        }
        l.addLast(session);
        return l;
      });
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
        MySQLPayloadWriter writer = this.clearTask.get(sessionId);
        if (writer == null) {
          writer = new MySQLPayloadWriter();
        }
        writer.writeBytes(packet);
        break;
    }

  }


  /**
   * 1.从闲置池里面移除mysql session 2.该函数不会关闭session 3.该函数可以被子类重写,但是未能遇见这种需要
   */
  @NoExcept
  protected void removeIdleSession(MySQLClientSession session) {
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
          LOGGER.error("", e);
          SessionTip.UNKNOWN_CLOSE_ERROR.getMessage(e);
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
    Collection<MySQLReplica> mysqlReplicaList = ProxyRuntime.INSTANCE
        .getMySQLReplicaList();
    HeartbeatRootConfig heartbeatRootConfig = ProxyRuntime.INSTANCE
        .getConfig(ConfigEnum.HEARTBEAT);
    long idleTimeout = heartbeatRootConfig.getHeartbeat().getIdleTimeout();
    long hearBeatTime = System.currentTimeMillis() - idleTimeout;
    long hearBeatTime2 = System.currentTimeMillis() - 2 * idleTimeout;

    for (MySQLReplica mySQLReplica : mysqlReplicaList) {
      List<MySQLDatasource> dataSourceList = mySQLReplica
          .getDatasourceList();

      for (MySQLDatasource mySQLDatasource : dataSourceList) {
        int maxConsInOneCheck = Math.min(10, mySQLDatasource.getSessionMinCount());
        LinkedList<MySQLClientSession> group = this.idleDatasourcehMap.get(mySQLDatasource);
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
      }
    }
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
        .request(session, MySQLCommandType.COM_QUERY, GlobalConfig.SINGLE_NODE_HEARTBEAT_SQL,
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
      if (!mySQLClientSession.isOpen()) {
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
    MycatReactorThread mycatReactorThread = mySQLClientSession.getMycatReactorThread();
    mycatReactorThread.addNIOJob(() -> {
      mySQLClientSession.close(false, hint);
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
    new BackendConCreateHandler(key, this,
        (MycatReactorThread) Thread.currentThread(), new CommandCallBack() {
      @Override
      public void onFinishedOk(int serverStatus, MySQLClientSession session, Object sender,
          Object attr) {
        assert session.currentProxyBuffer() == null;
        MycatMonitor.onNewMySQLSession(session);
        allSessions.put(session.sessionId(), session);
        callBack.onSession(session, sender, attr);
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
  @NoExcept
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

}
