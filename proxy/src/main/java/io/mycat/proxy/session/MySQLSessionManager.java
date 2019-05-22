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
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.logTip.SessionTip;
import io.mycat.proxy.callback.CommandCallBack;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.MySQLPacketExchanger.MySQLIdleNIOHandler;
import io.mycat.proxy.handler.backend.BackendConCreateTask;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.SessionManager.BackendSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.util.nio.NIOUtil;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 集中管理MySQL LocalInFileSession 是在mycat proxy中,唯一能够创建mysql session以及关闭mysqlsession的对象
 * 该在一个线程单位里,对象生命周期应该是单例的
 *
 * @author jamie12221
 * @date 2019-05-10 13:21
 **/
public final class MySQLSessionManager implements
    BackendSessionManager<MySQLClientSession, MySQLDatasource> {

  final LinkedList<MySQLClientSession> allSessions = new LinkedList<>();
  final HashMap<MySQLDatasource, LinkedList<MySQLClientSession>> idleDatasourcehMap = new HashMap<>();

  /**
   * 返回不可变集合,防止外部代码错误操作allSessions导致泄漏MySQLSession
   */
  @Override
  public final Collection<MySQLClientSession> getAllSessions() {
    return Collections.unmodifiableCollection(allSessions);
  }

  /**
   * 获得mysql proxy中,该线程中所有的mysqlSession的数量
   */
  @Override
  public final int currentSessionCount() {
    return allSessions.size();
  }

  /**
   * 根据dataSource的配置信息获得可用的MySQLSession 0.如果dataSource已经失效,则直接回调异常,异常信息在callback的attr
   * 1.首先从空闲的session集合里面尝试获取 若存在空闲的session则随机从头部或者尾部获得session 2.否则创建新的session
   */
  @Override
  public final void getIdleSessionsOfKey(MySQLDatasource datasource,
      SessionCallBack<MySQLClientSession> asyncTaskCallBack) {
    assert datasource != null;
    /**
     * 1.如果集群不可用,直接回调不可用
     * 2.如果没有空闲连接,则创建空闲连接
     * 3.如果有空闲连接,则获取空闲连接,然后查看通道是否已经关闭,如果已经关闭,则继续尝试获取
     * 4.session管理不保证session一定可用
     */
    {
      LinkedList<MySQLClientSession> group = this.idleDatasourcehMap.get(datasource);
      for (; ; ) {
        if (!datasource.isAlive()) {
          asyncTaskCallBack
              .onException(new MycatExpection(datasource.getName() + " is not alive!"), this, null);
          return;
        }
        if (group == null || group.isEmpty()) {
          createSession(datasource, asyncTaskCallBack);
          return;
        } else {
          boolean random = ThreadLocalRandom.current().nextBoolean();
          MySQLClientSession mySQLSession = random ? group.removeFirst() : group.removeLast();
          mySQLSession.setIdle(false);
          assert mySQLSession.getCurNIOHandler() == MySQLIdleNIOHandler.INSTANCE;
          assert mySQLSession.currentProxyBuffer() == null;
          if (!mySQLSession.isOpen()) {
            MycatReactorThread mycatReactorThread = mySQLSession.getMycatReactorThread();
            mycatReactorThread.addNIOJob(() -> {
              mySQLSession.close(false, "mysql session is close in idle");
            });
            continue;
          } else if (mySQLSession.isActivated()) {
            mySQLSession.setIdle(false);
            mySQLSession.switchNioHandler(null);
            MycatMonitor.onGetIdleMysqlSession(mySQLSession);
            asyncTaskCallBack.onSession(mySQLSession, this, null);
            return;
          } else {

            asyncTaskCallBack.onSession(mySQLSession, this, null);
            return;
          }
        }
      }
    }
  }

  /**
   * 把使用完毕的mysql session释放到连接池 1.禁止把session多次放入闲置连接池,但是该方法不查重,需要调用方保证
   * 2.在闲置连接池里面,session不对写入事件响应,但对读取事件响应,因为可能收到关闭事件
   */
  @Override
  public final void addIdleSession(MySQLClientSession session) {
    /**
     * mycat对应透传模式对mysql session的占用
     * niohandler对应透传以及task类对mysql session的占用
     */
    assert session.getMycatSession() == null;
    assert !session.isClosed();
    assert session.isActivated();
    assert session.currentProxyBuffer() == null;
    assert !session.isIdle();
    if (session.isMonopolized()) {
      throw new MycatExpection("Monopolized");
    }
    session.resetPacket();
    session.setIdle(true);
    session.switchNioHandler(MySQLIdleNIOHandler.INSTANCE);
    session.change2ReadOpts();
    MycatMonitor.onAddIdleMysqlSession(session);
    idleDatasourcehMap.compute(session.getDatasource(), (k, l) -> {
      if (l == null) {
        l = new LinkedList<>();
      }
      l.addLast(session);
      return l;
    });
  }

  /**
   * 1.从闲置池里面移除mysql session 2.该函数不会关闭session 3.该函数可以被子类重写,但是未能遇见这种需要
   */
  protected void removeIdleSession(MySQLClientSession session) {
    assert session != null;
    assert session.getDatasource() != null;
    LinkedList<MySQLClientSession> mySQLSessions = idleDatasourcehMap.get(session.getDatasource());
    if (mySQLSessions != null) {
      mySQLSessions.remove(session);
    }
  }

  /**
   * 1.可能被以下需要调用 a.定时清理某个dataSource的连接 b.强制关闭摸个dataSource的连接
   */
  @Override
  public final void clearAndDestroyDataSource(MySQLDatasource key, String reason) {
    assert key != null;
    assert reason != null;
    allSessions.removeIf(session -> session.getDatasource().equals(key));
    LinkedList<MySQLClientSession> sessions = idleDatasourcehMap.get(key);
    if (sessions != null) {
      for (MySQLClientSession session : sessions) {
        try {
          session.close(true, reason);
        } catch (Exception e) {
          SessionTip.UNKNOWN_CLOSE_ERROR.getMessage(e);
        }
      }
    }
    idleDatasourcehMap.remove(key);
  }

  /**
   * 根据dataSource信息创建MySQL LocalInFileSession 1.这个函数并不会把连接加入到闲置的连接池,因为创建的session就是马上使用的,如果创建之后就加入闲置连接池就会发生挣用问题
   * 2.错误信息放置在attr
   */
  @Override
  public void createSession(MySQLDatasource key, SessionCallBack<MySQLClientSession> callBack) {
    assert key != null;
    assert callBack != null;
    new BackendConCreateTask(key, this,
        (MycatReactorThread) Thread.currentThread(), new CommandCallBack() {
      @Override
      public void onFinishedOk(int serverStatus, MySQLClientSession session, Object sender,
          Object attr) {
        assert session.currentProxyBuffer() == null;
        MycatMonitor.onNewMySQLSession(session);
        allSessions.add(session);
        callBack.onSession(session, sender, attr);
      }

      @Override
      public void onFinishedException(Exception exception, Object sender, Object attr) {
        callBack.onException(exception, sender, attr);
      }

      @Override
      public void onFinishedErrorPacket(ErrorPacket errorPacket, int lastServerStatus,
          MySQLClientSession session, Object sender, Object attr) {
        callBack.onException(toExpection(errorPacket), sender, attr);
      }
    });
  }

  /**
   * 从连接池移除session 1.移除连接 2.关闭连接 3.关闭原因需要写清楚
   */
  @Override
  public void removeSession(MySQLClientSession session, boolean normal, String reason) {
    assert session != null;
    assert reason != null;
    allSessions.remove(session);
    MycatMonitor.onCloseMysqlSession(session);
    removeIdleSession(session);
    NIOUtil.close(session.channel());
  }
}
