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
import io.mycat.plug.loadBalance.LoadBalanceDataSource;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.AsyncTaskCallBackCounter;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MySQL Seesion元信息 对外支持线程修改的属性是alive,其他属性只读
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class MySQLDatasource implements LoadBalanceDataSource {

  protected static final Logger logger = LoggerFactory.getLogger(MySQLDatasource.class);
  protected final int index;
  protected final DatasourceConfig datasourceConfig;
  protected final MySQLReplica replica;
  protected final MySQLCollationIndex collationIndex = new MySQLCollationIndex();
  protected final AtomicInteger connectionCounter = new AtomicInteger(0);

  public MySQLDatasource(int index, DatasourceConfig datasourceConfig,
      MySQLReplica replica) {
    this.index = index;
    this.datasourceConfig = datasourceConfig;
    this.replica = replica;
  }

  public int getSessionLimitCount() {
    return datasourceConfig.getMaxCon();
  }


  /**
   * 回调表示获取此数据源的信息成功 信息需要包含字符集内容,如果字符集获取失败,则集群也是启动失败 字符集只有第一个Session获取,此后新建的session就不会获取,因为字符集是集群使用,集群对外应该表现为一个mysql
   *
   * @param callback 回调函数
   */
  public void init(AsyncTaskCallBackCounter callback) {
    Objects.requireNonNull(callback);
    int minCon = datasourceConfig.getMinCon();
    MycatReactorThread[] threads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
    Objects.requireNonNull(threads);
    for (int index = 0; index < minCon; index++) {
      MycatReactorThread thread = threads[index % threads.length];
      thread.addNIOJob(createMySQLSession(thread, new SessionCallBack<MySQLClientSession>() {
        @Override
        public void onSession(MySQLClientSession session, Object sender, Object attr) {
          callback.onCountSuccess();
        }

        @Override
        public void onException(Exception exception, Object sender, Object attr) {
          callback.onCountFail();
        }
      }));
    }
  }


  /**
   * 创建session辅助函数
   *
   * @param thread 执行的线程
   * @param callback 回调函数
   */
  protected Runnable createMySQLSession(MycatReactorThread thread,
      SessionCallBack<MySQLClientSession> callback) {
    Objects.requireNonNull(thread);
    Objects.requireNonNull(callback);
    return () -> thread.getMySQLSessionManager()
        .createSession(this, callback);
  }


  /**
   * 关闭此dataSource创建的连接
   *
   * @param reason 关闭原因
   */
  public void clearAndDestroyCons(String reason) {
    Objects.requireNonNull(reason);
    MycatReactorThread[] mycatReactorThreads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
    Objects.requireNonNull(mycatReactorThreads);
    for (MycatReactorThread thread : mycatReactorThreads) {
      thread.addNIOJob(
          () -> {
            thread.getMySQLSessionManager().clearAndDestroyDataSource(this, reason);
          });
    }
  }

  public abstract boolean isAlive();

  @Override
  public String getName() {
    return this.datasourceConfig.getName();
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

  @Override
  public boolean isMaster() {
    return index == replica.getMasterIndex();
  }

  @Override
  public boolean isSlave() {
    return index != replica.getMasterIndex();
  }

  @Override
  public int getSessionCounter() {
    return connectionCounter.get();
  }

  @Override
  public int getWeight() {
    return this.datasourceConfig.getWeight();
  }

  public int decrementSessionCounter() {
    return connectionCounter.decrementAndGet();
  }

  public boolean tryIncrementSessionCounter() {
    int current = connectionCounter.get();
    return current < connectionCounter.updateAndGet(operand -> {
      if (operand < this.datasourceConfig.getMaxCon()) {
        return ++operand;
      } else {
        return operand;
      }
    });
  }
}
