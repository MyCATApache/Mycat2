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
package io.mycat.proxy;

import io.mycat.MycatExpection;
import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.MySQLPacketExchanger;
import io.mycat.proxy.handler.ResponseType;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.handler.backend.MySQLSessionSyncUtil;
import io.mycat.proxy.handler.backend.MySQLSynContext;
import io.mycat.proxy.handler.backend.SessionSyncCallback;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.packet.MySQLPacketCallback;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author jamie12221 date 2019-05-12 22:41 dataNode执行器 该类本意是从路由获得dataNode名字之后,使用该执行器执行,
 * 解耦结果类和实际执行方法
 **/
public class MySQLTaskUtil {


  public static void proxyBackend(MycatSession mycat, byte[] payload, String dataNodeName,
      MySQLDataSourceQuery query, ResponseType responseType) {
    MycatMonitor.onRoute(mycat, dataNodeName, payload);
    MySQLPacketExchanger.INSTANCE
        .proxyBackend(mycat, payload, dataNodeName, query, responseType);
  }

  public static void proxyBackendWithCollector(MycatSession mycat, byte[] payload,
      String dataNodeName,
      MySQLDataSourceQuery query, ResponseType responseType,
      MySQLPacketCallback callback) {
    MycatMonitor.onRoute(mycat, dataNodeName, payload);
    MySQLPacketExchanger.INSTANCE
        .proxyWithCollectorCallback(mycat, payload, dataNodeName, query, responseType, callback);
  }

  public static void withBackend(MycatSession mycat, byte[] payload,
      String dataNodeName,
      MySQLDataSourceQuery query,
      ResponseType responseType,
      MySQLPacketCallback callback) {

    MycatMonitor.onRoute(mycat, dataNodeName, payload);
    MySQLPacketExchanger.INSTANCE
        .proxyWithCollectorCallback(mycat, payload, dataNodeName, query, responseType, callback);
  }

  public static void withBackend(MycatSession mycat, String dataNodeName,
      MySQLDataSourceQuery query,
      SessionSyncCallback finallyCallBack) {
    mycat.currentProxyBuffer().reset();
    mycat.switchDataNode(dataNodeName);
    if (mycat.getMySQLSession() != null) {
      //只要backend还有值,就说明上次命令因为事务或者遇到预处理,loadata这种跨多次命令的类型导致mysql不能释放
      finallyCallBack.onSession(mycat.getMySQLSession(), MySQLPacketExchanger.INSTANCE, null);
      return;
    }
    MySQLSynContext mycatSynContext = mycat.getRuntime().getProviders()
        .createMySQLSynContext(mycat);
    MySQLTaskUtil
        .getMySQLSession(mycatSynContext, query, finallyCallBack);
  }

  /**
   * 用户在非mycat reactor 线程获取 session
   *
   * 回调执行的函数处于mycat reactor thread 所以不能编写长时间执行的代码
   */
  public static void getMySQLSessionFromUserThread(ProxyRuntime runtime, MySQLSynContext synContext,
      MySQLDataSourceQuery query,
      SessionSyncCallback asynTaskCallBack) {
    MycatReactorThread[] threads = runtime.getMycatReactorThreads();
    int i = ThreadLocalRandom.current().nextInt(0, threads.length);
    threads[i].addNIOJob(() -> {
      getMySQLSession(synContext, query, asynTaskCallBack);
    });
  }

  /**
   * dataNode执行器 该类本意是从路由获得dataNode名字之后,使用该执行器执行, 解耦结果类和实际执行方法
   *
   * 该函数实现session状态同步的功能
   */
  public static void getMySQLSession(
      MySQLSynContext synContext,
      MySQLDataSourceQuery query,
      SessionSyncCallback callBack) {

    assert (Thread.currentThread() instanceof MycatReactorThread);
    Objects.requireNonNull(synContext.getDataNode());
    Objects.requireNonNull(synContext.getCharset());
    MySQLDataNode dataNode = synContext.getDataNode();
    MySQLReplica replica = (MySQLReplica) dataNode.getReplica();
    Objects.requireNonNull(replica);
    replica.getMySQLSessionByBalance(query,
        new SessionCallBack<MySQLClientSession>() {
          @Override
          public void onSession(MySQLClientSession mysql, Object sender, Object attr) {
            MySQLSessionSyncUtil.sync(synContext, mysql, this, callBack);
          }

          @Override
          public void onException(Exception exception, Object sender, Object attr) {
            callBack.onException(exception, sender, attr);
          }
        }
    );
  }

  public static void getMySQLSessionForTryConnect(MySQLDatasource datasource,
      SessionCallBack<MySQLClientSession> asynTaskCallBack) {
    Objects.requireNonNull(datasource);
    Objects.requireNonNull(asynTaskCallBack);
    Thread thread = Thread.currentThread();
    if (thread instanceof MycatReactorThread) {
      MySQLSessionManager manager = ((MycatReactorThread) thread)
          .getMySQLSessionManager();
      if (datasource.isAlive()) {
        manager.getIdleSessionsOfKey(
            datasource
            , asynTaskCallBack);
      } else {
        manager.createSession(
            datasource
            , asynTaskCallBack);
      }
    } else {
      throw new MycatExpection("Replica must running in MycatReactorThread");
    }
  }

  public static void getMySQLSessionForTryConnectFromUserThread(ProxyRuntime runtime,
      MySQLDatasource datasource,
      SessionCallBack<MySQLClientSession> asynTaskCallBack) {
    MycatReactorThread[] threads = runtime.getMycatReactorThreads();
    int i = ThreadLocalRandom.current().nextInt(0, threads.length);
    threads[i].addNIOJob(() -> {
      getMySQLSessionForTryConnect(datasource, asynTaskCallBack);
    });
  }
}
