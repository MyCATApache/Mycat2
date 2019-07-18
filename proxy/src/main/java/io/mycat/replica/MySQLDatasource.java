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

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceElement;
import io.mycat.proxy.callback.AsyncTaskCallBackCounter;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOJob;
import io.mycat.proxy.reactor.ReactorEnvThread;
import io.mycat.proxy.session.MySQLClientSession;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MySQL Seesion元信息 对外支持线程修改的属性是alive,其他属性只读
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class MySQLDatasource implements LoadBalanceElement {

  protected static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLDatasource.class);
  protected final int index;
  protected final DatasourceConfig datasourceConfig;
  protected final MySQLReplica replica;
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

  public int getSessionMinCount() {
    return datasourceConfig.getMinCon();
  }


  /**
   * 回调表示获取此数据源的信息成功 信息需要包含字符集内容,如果字符集获取失败,则集群也是启动失败 字符集只有第一个Session获取,此后新建的session就不会获取,因为字符集是集群使用,集群对外应该表现为一个mysql
   *
   * @param callback 回调函数
   */
  public void init(MycatReactorThread[] threads, AsyncTaskCallBackCounter callback) {
    Objects.requireNonNull(callback);
    int minCon = datasourceConfig.getMinCon();
    Objects.requireNonNull(threads);
    if (minCon < 1) {
      callback.onCountSuccess();
    }
    for (int index = 0; index < minCon; index++) {
      MycatReactorThread thread = threads[index % threads.length];
      thread.addNIOJob(createMySQLSession(thread, new SessionCallBack<MySQLClientSession>() {
        @Override
        public void onSession(MySQLClientSession session, Object sender, Object attr) {
          session.getSessionManager().addIdleSession(session);
          callback.onCountSuccess();
        }

        @Override
        public void onException(Exception exception, Object sender, Object attr) {
          LOGGER.error(exception.getMessage());
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
  protected NIOJob createMySQLSession(MycatReactorThread thread,
      SessionCallBack<MySQLClientSession> callback) {
    Objects.requireNonNull(thread);
    Objects.requireNonNull(callback);
    MySQLDatasource datasource = this;
    return new NIOJob() {
      @Override
      public void run(ReactorEnvThread reactor) throws Exception {
        thread.getMySQLSessionManager()
            .createSession(datasource, callback);
      }

      @Override
      public void stop(ReactorEnvThread reactor, Exception reason) {
        callback.onException(reason, this, null);
      }

      @Override
      public String message() {
        return "createMySQLSession";
      }
    };
  }

//  /**
//   * 关闭此dataSource创建的连接
//   *
//   * @param message 关闭原因
//   */
//  public void clearAndDestroyCons(String message) {
//    Objects.requireNonNull(message);
//    MycatReactorThread[] mycatReactorThreads = ProxyRuntime.INSTANCE.getMycatReactorThreads();
//    Objects.requireNonNull(mycatReactorThreads);
//    for (MycatReactorThread thread : mycatReactorThreads) {
//      thread.addNIOJob(
//          () -> {
//            thread.getMySQLSessionManager().clearAndDestroyDataSource(this, message);
//          });
//    }
//  }

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

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MySQLDatasource that = (MySQLDatasource) o;
    return getName().equals(that.getName());
  }

  @Override
  public int hashCode() {
    return getName().hashCode();
  }

  @Override
  public boolean isMaster() {
    return replica.isMaster(this);
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
    return connectionCounter.updateAndGet(operand -> {
      if (operand > 0) {
        return --operand;
      } else {
        return 0;
      }
    });
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

  public String getInitSQL() {
    return datasourceConfig.getInitSQL();
  }

  public int getIndex() {
    return index;
  }
}
