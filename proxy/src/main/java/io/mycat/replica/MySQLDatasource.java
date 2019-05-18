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

import io.mycat.beans.mysql.charset.MySQLCollationIndex;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.logTip.DataSourceTip;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.client.QueryUtil;
import java.util.Objects;
import java.util.function.BiConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL Seesion元信息 对外支持线程修改的属性是alive,其他属性只读
 *
 * @author jamie12221
 * @date 2019-05-10 13:21
 **/
public abstract class MySQLDatasource {

  protected static final Logger logger = LoggerFactory.getLogger(MySQLDatasource.class);
  protected final int index;
  protected final DatasourceConfig datasourceConfig;
  protected final MySQLReplica replica;
  protected final MySQLCollationIndex collationIndex = new MySQLCollationIndex();

  public MySQLDatasource(int index, DatasourceConfig datasourceConfig,
      MySQLReplica replica) {
    this.index = index;
    this.datasourceConfig = datasourceConfig;
    this.replica = replica;
  }

  /**
   * 回调表示获取此数据源的信息成功 信息需要包含字符集内容,如果字符集获取失败,则集群也是启动失败 字符集只有第一个Session获取,此后新建的session就不会获取,因为字符集是集群使用,集群对外应该表现为一个mysql
   */
  public void init(BiConsumer<MySQLDatasource, Boolean> successCallback) {
    Objects.requireNonNull(successCallback);
    int minCon = datasourceConfig.getMinCon();
    MycatReactorThread[] threads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
    Objects.requireNonNull(threads);
    MycatReactorThread firstThread = threads[0 % threads.length];
    firstThread.addNIOJob(
        createMySQLSession(firstThread, (mysql0, sender0, success0, result0, attr) -> {
          if (success0) {
            logger.info(DataSourceTip.CREATE_DATASOURCE_SUCCESS.getMessage(getName()));
            QueryUtil.collectCharset(mysql0, collationIndex,
                (mysql1, sender1, success1, result1, errorMessage1) -> {
                  if (success1) {
                    mysql1.getSessionManager().addIdleSession(mysql1);
                    logger.info(DataSourceTip.READ_CHARSET_SUCCESS.getMessage(getName()));
                    for (int index = 1; index < minCon; index++) {
                      MycatReactorThread thread = threads[index % threads.length];
                      final int finalIndex = index;
                      thread.addNIOJob(createMySQLSession(thread,
                          (mysql2, sender2, success2, result2, attr2) -> {
                            assert mysql2.currentProxyBuffer() == null;
                            if (success2) {
                              mysql2.getSessionManager().addIdleSession(mysql2);
                              logger.info(DataSourceTip.CREATE_DATASOURCE_SUCCESS.getMessage());
                            } else {
                              logger.error(DataSourceTip.CREATE_DATASOURCE_FAIL
                                               .getMessage(getName(), Objects.toString(result2)));
                            }
                          }));
                    }
                    successCallback.accept(this, true);
                  } else {
                    logger.error(
                        DataSourceTip.CREATE_DATASOURCE_FAIL.getMessage(getName(), errorMessage1),
                        errorMessage1);
                    successCallback.accept(this, false);
                  }
                });
          } else {
            logger.error((DataSourceTip.CREATE_DATASOURCE_FAIL
                              .getMessage(getName(), Objects.toString(result0))));
            successCallback.accept(this, false);
          }
        }));
  }

  /**
   * 创建session辅助函数
   */
  protected Runnable createMySQLSession(MycatReactorThread thread,
      AsyncTaskCallBack<MySQLClientSession> callback) {
    Objects.requireNonNull(thread);
    Objects.requireNonNull(callback);
    return () -> thread.getMySQLSessionManager()
                     .createSession(this, (mysql, sender, success, result, attr) -> {
                       if (success) {
                         callback.finished(mysql, this, true, null, null);
                       } else {
                         String message = (String) result;
                         logger
                             .error(DataSourceTip.CREATE_DATASOURCE_FAIL
                                        .getMessage(getName(), message));
                         callback.finished(null, this, false, message, null);
                       }
                     });
  }

  /**
   * 关闭此dataSource创建的连接
   */
  public void clearAndDestroyCons(String reason) {
    Objects.requireNonNull(reason);
    MycatReactorThread[] mycatReactorThreads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
    Objects.requireNonNull(mycatReactorThreads);
    for (MycatReactorThread thread : mycatReactorThreads) {
      thread.addNIOJob(
          () -> thread.getMySQLSessionManager().clearAndDestroyDataSource(this, reason));
    }
  }

  public abstract boolean isAlive();

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

  public MySQLReplica getReplica() {
    return replica;
  }

  public MySQLCollationIndex getCollationIndex() {
    return collationIndex;
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
               Objects.equals(datasourceConfig, that.datasourceConfig) &&
               Objects.equals(replica, that.replica) &&
               Objects.equals(collationIndex, that.collationIndex);
  }

  @Override
  public int hashCode() {
    return Objects.hash(index, datasourceConfig, replica, collationIndex);
  }

  public boolean isMaster() {
    return index == replica.getMasterIndex();
  }

  public boolean isSlave(){
    return index != replica.getMasterIndex();
  }

}
