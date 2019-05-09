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
package io.mycat.proxy.executer;

import io.mycat.MySQLDataNode;
import io.mycat.MycatExpection;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.MultiOkQueriesCounterTask;
import io.mycat.replica.MySQLReplica;
import java.util.concurrent.ThreadLocalRandom;

public class MySQLDataNodeExecuter {

  public static void getMySQLSessionFromUserThread(String dataNodeName, MySQLIsolation isolation,
      MySQLAutoCommit autoCommit, String charSet,
      boolean runOnSlave, LoadBalanceStrategy strategy,
      AsynTaskCallBack<MySQLClientSession> asynTaskCallBack) {
    MycatReactorThread[] threads = MycatRuntime.INSTANCE.getMycatReactorThreads();
    int i = ThreadLocalRandom.current().nextInt(0, threads.length);
    MySQLDataNode dataNode = (MySQLDataNode) MycatRuntime.INSTANCE.getDataNodeByName(dataNodeName);
    threads[i].addNIOJob(() -> {
      getMySQLSession(dataNode, isolation, autoCommit, charSet, runOnSlave, strategy,
          asynTaskCallBack);
    });
  }

  public static void getMySQLSession(MySQLDataNode dataNode,
      MySQLIsolation isolation,
      MySQLAutoCommit autoCommit,
      String charSet,
      boolean runOnSlave,
      LoadBalanceStrategy strategy,
      AsynTaskCallBack<MySQLClientSession> asynTaskCallBack) {
    if (dataNode != null) {
      MySQLReplica replica = (MySQLReplica) dataNode.getReplica();
      if (replica == null) {
        replica = MycatRuntime.INSTANCE.getMySQLReplicaByReplicaName(dataNode.getReplicaName());
      }
      replica.getMySQLSessionByBalance(runOnSlave, strategy,
          (mysql, sender, success, result, errorMessage) -> {
            if (success) {
              if (dataNode.equals(mysql.getDataNode())) {
                asynTaskCallBack.finished(mysql, sender, true, result, errorMessage);
              } else {
                String databaseName = dataNode.getDatabaseName();
                String sql =
                    isolation.getCmd() + autoCommit.getCmd() + "USE " + databaseName
                        + ";" + "SET names " + charSet + ";";
                new MultiOkQueriesCounterTask(4)
                    .request(mysql, sql, asynTaskCallBack);
              }
            } else {
              asynTaskCallBack.finished(mysql, sender, success, result, errorMessage);
            }
          });
    } else {
      throw new MycatExpection("unsupport dataNode Type");
    }


  }


}
