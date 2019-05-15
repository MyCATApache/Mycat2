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
package io.mycat.proxy.task.client;

import io.mycat.MycatExpection;
import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.logTip.ReplicaTip;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MySQLSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author jamie12221
 * @date 2019-05-12 22:41 dataNode执行器 该类本意是从路由获得dataNode名字之后,使用该执行器执行, 解耦结果类和实际执行方法
 **/
public class MySQLTaskUtil {

  /**
   * 用户在非mycat reactor 线程获取 session
   *
   * 回调执行的函数处于mycat reactor thread 所以不能编写长时间执行的代码
   */
  public static void getMySQLSessionFromUserThread(String dataNodeName,
      MySQLIsolation isolation,
      MySQLAutoCommit autoCommit, String charSet,
      boolean runOnSlave, LoadBalanceStrategy strategy,
      AsyncTaskCallBack<MySQLClientSession> asynTaskCallBack) {
    MycatReactorThread[] threads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
    int i = ThreadLocalRandom.current().nextInt(0, threads.length);
    MySQLDataNode dataNode = ProxyRuntime.INSTANCE.getDataNodeByName(dataNodeName);
    threads[i].addNIOJob(() -> {
      getMySQLSession(dataNode, isolation, autoCommit, charSet, runOnSlave, strategy,
          asynTaskCallBack);
    });
  }

  /**
   * dataNode执行器 该类本意是从路由获得dataNode名字之后,使用该执行器执行, 解耦结果类和实际执行方法
   *
   * 该函数实现session状态同步的功能
   */
  public static void getMySQLSession(MySQLDataNode dataNode,
      MySQLIsolation isolation,
      MySQLAutoCommit autoCommit,
      String charset,
      boolean runOnSlave,
      LoadBalanceStrategy strategy,
      AsyncTaskCallBack<MySQLClientSession> asynTaskCallBack) {
    Objects.requireNonNull(charset);
    assert (Thread.currentThread() instanceof MycatReactorThread);
    Objects.requireNonNull(dataNode);
    MySQLReplica replica = (MySQLReplica) dataNode.getReplica();
    if (replica == null) {
      replica = ProxyRuntime.INSTANCE.getMySQLReplicaByReplicaName(dataNode.getReplicaName());
      dataNode.setReplica(replica);
    }
    replica.getMySQLSessionByBalance(runOnSlave, strategy,
        (mysql, sender, success, result, errorMessage) -> {
          if (success) {
            if (dataNode.equals(mysql.getDataNode())) {
              if (autoCommit == MySQLAutoCommit.ON == mysql.isAutomCommit() &&
                      charset.equals(mysql.getCharset()) &&
                      isolation.equals(mysql.getIsolation())
              ) {
                asynTaskCallBack.finished(mysql, sender, true, result, errorMessage);
                return;
              }
            }
            String databaseName = dataNode.getDatabaseName();
            String sql =
                isolation.getCmd() + autoCommit.getCmd() + "USE " + databaseName
                    + ";" + "SET names " + charset + ";";
            QueryUtil.mutilOkResultSet(mysql, 4, sql,
                new AsyncTaskCallBack<MySQLClientSession>() {
                  @Override
                  public void finished(MySQLClientSession session, Object sender,
                      boolean success, Object result, Object attr) {
                    if (success) {
                      session.setCharset(charset);
                      session.setDataNode(dataNode);
                      session.setIsolation(isolation);
                      asynTaskCallBack.finished(session, this, success, result, attr);
                    } else {
                      asynTaskCallBack.finished(session, this, success, result, attr);
                    }
                  }
                });
          } else {
            asynTaskCallBack.finished(mysql, sender, success, result, errorMessage);
          }
        });
  }

  public static void getMySQLSessionForHeatbeat(MySQLDatasource datasource,
      AsyncTaskCallBack<MySQLClientSession> asynTaskCallBack) {
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
      throw new MycatExpection(ReplicaTip.ERROR_EXECUTION_THREAD.getMessage());
    }
  }

  public static void getMySQLSessionForHeartbeatFromUserThread(MySQLDatasource datasource,
      AsyncTaskCallBack<MySQLClientSession> asynTaskCallBack) {
    MycatReactorThread[] threads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
    int i = ThreadLocalRandom.current().nextInt(0, threads.length);
    threads[i].addNIOJob(() -> {
      getMySQLSessionForHeatbeat(datasource, asynTaskCallBack);
    });
  }
}
