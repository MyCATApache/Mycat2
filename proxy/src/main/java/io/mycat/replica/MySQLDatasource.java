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
package io.mycat.replica;

import io.mycat.beans.mysql.MySQLCollationIndex;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.QueryUtil;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MySQLDatasource {

  private static final Logger logger = LoggerFactory.getLogger(MySQLDatasource.class);
  private final int index;
  private final DatasourceConfig datasourceConfig;
  private boolean master = false;
  private MySQLReplica replica;
  private MySQLCollationIndex collationIndex = new MySQLCollationIndex();


  public MySQLDatasource(int index, boolean master, DatasourceConfig datasourceConfig,
      MySQLReplica replica) {
    this.index = index;
    this.master = master;
    this.datasourceConfig = datasourceConfig;
    this.replica = replica;
  }

  public void init(BiConsumer<MySQLDatasource, Boolean> successCallback) {
    int minCon = datasourceConfig.getMinCon();
    MycatReactorThread[] threads = MycatRuntime.INSTANCE.getMycatReactorThreads();
    MycatReactorThread firstThread = threads[0 % threads.length];
    firstThread.addNIOJob(
        createMySQLSession(firstThread, (mysql0, sender0, success0, result0, errorMessage0) -> {
          if (success0) {
            logger.info("dataSource create successful!!");
            QueryUtil.collectCollation(mysql0, collationIndex,
                (mysql1, sender1, success1, result1, errorMessage1) -> {
                  if (success1) {
                    mysql1.end();
                    logger.info("dataSource read charset successful!!");
                    for (int index = 1; index < minCon; index++) {
                      MycatReactorThread thread = threads[index % threads.length];
                      Integer finalIndex = index;
                      thread.addNIOJob(createMySQLSession(thread,
                          (mysql2, sender2, success2, result2, errorMessage2) -> {
                            if (success2) {
                              logger.info("dataSource {} create successful!!", finalIndex);
                            } else {
                              logger.error("dataSource {} create fail!!", finalIndex);
                            }
                          }));
                    }
                    successCallback.accept(this, true);
                  } else {
                    logger.error("read charset fail", errorMessage1);
                    successCallback.accept(this, false);
                  }
                });
          } else {
            logger.error("dataSource {} create fail!!", 0);
            successCallback.accept(this, false);
          }
        }));
  }

  private Runnable createMySQLSession(MycatReactorThread thread,
      AsynTaskCallBack<MySQLClientSession> callback) {
    return () -> thread.getMySQLSessionManager()
                     .createSession(this, (mysql, sender, success, result, errorMessage) -> {
                       if (success) {
                         callback.finished(mysql, this, true, null, null);
                       } else {
                         logger.error("create connection fail", errorMessage);
                         callback.finished(null, this, false, null, errorMessage);
                       }
                     });
  }

  public void clearAndDestroyCons(String reason) {
    for (MycatReactorThread thread : MycatRuntime.INSTANCE.getMycatReactorThreads()) {
      thread.addNIOJob(
          () -> thread.getMySQLSessionManager().clearAndDestroyDataSource(this, reason));
    }
  }

  public String getName() {
    return this.datasourceConfig.getHostName();
  }

  public String getIp() {
    return this.datasourceConfig.getIp();
  }

  public int getPort() {
    return this.datasourceConfig.getPort();
  }

  public String getUsername() {
    return this.datasourceConfig.getUser();
  }

  public String getPassword() {
    return this.datasourceConfig.getPassword();
  }

  public boolean isAlive() {
    return true;
  }

  public boolean isActive() {
    return isAlive() && true;
  }

  public boolean canSelectAsReadNode() {
    return canSelectAsReadNode(HeartbeatInfReceiver.identity());
  }

  public boolean canSelectAsReadNode(HeartbeatInfReceiver receiver) {
    return true;
  }

  public boolean isMaster() {
    return master;
  }

  public boolean isSlave() {
    return !isMaster();
  }

  public MySQLReplica getReplica() {
    return replica;
  }

  public MySQLCollationIndex getCollationIndex() {
    return collationIndex;
  }

  public void doHeartbeat() {

  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MySQLDatasource that = (MySQLDatasource) o;
    return index == that.index &&
               master == that.master &&
               Objects.equals(datasourceConfig, that.datasourceConfig) &&
               Objects.equals(replica, that.replica) &&
               Objects.equals(collationIndex, that.collationIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, master, datasourceConfig, replica, collationIndex);
  }
}
