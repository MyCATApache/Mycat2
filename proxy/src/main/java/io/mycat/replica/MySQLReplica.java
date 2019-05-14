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

import io.mycat.MycatExpection;
import io.mycat.beans.mycat.MycatReplica;
import io.mycat.beans.mysql.charset.MySQLCollationIndex;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.logTip.ReplicaTip;
import io.mycat.plug.loadBalance.BalanceAllRead;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;

/**
 * 集群管理,该类不执行心跳,也不管理jdbc的mysqlsession,只做均衡负载 集群状态在集群相关辅助类实现,辅助类使用定时器分发到执行.辅助类只能更改此类的writeIndex属性,其他属性不是线程安全,初始化之后只读
 *
 * @author jamie12221
 * @date 2019-05-10 13:21
 **/
public abstract class MySQLReplica implements MycatReplica {

  private final ReplicaConfig config;
  private final List<MySQLDatasource> datasourceList = new ArrayList<>();
  private volatile int writeIndex = 0; //主节点默认为0
  private long lastInitTime;  //最后一次初始化时间
  private LoadBalanceStrategy defaultLoadBalanceStrategy = BalanceAllRead.INSTANCE;
  private MySQLCollationIndex collationIndex;

  /**
   * 初始化mycat集群管理
   */
  public MySQLReplica(ReplicaConfig replicaConfig,
      int writeIndex, MySQLDataSourceFactory dataSourceFactory) {
    assert replicaConfig != null;
    assert writeIndex > -1;
    List<DatasourceConfig> mysqls = replicaConfig.getMysqls();
    assert mysqls != null;
    checkIndex(replicaConfig.getName(), writeIndex, mysqls.size());
    this.config = replicaConfig;
    for (int index = 0; index < mysqls.size(); index++) {
      boolean master = index == writeIndex;
      DatasourceConfig datasourceConfig = mysqls.get(index);
      assert datasourceConfig != null;
      if (datasourceConfig.getDbType() == null) {
        datasourceList.add(dataSourceFactory.get(index, datasourceConfig, this));
      }
    }
  }

  /**
   * 获取最后一次初始化时间
   */
  public long getLastInitTime() {
    return lastInitTime;
  }

  /**
   * 对于已经运行的集群,首先把原session都关闭再重新创建
   */
  public void init() {
    Objects.requireNonNull(config);
    Objects.requireNonNull(datasourceList);
    for (MySQLDatasource datasource : datasourceList) {
      datasource.clearAndDestroyCons(ReplicaTip.INIT_REPLICA.getMessage(getName()));
    }
    final BiConsumer<MySQLDatasource, Boolean> defaultCallBack = (datasource, success) -> {
      this.lastInitTime = System.currentTimeMillis();
      this.collationIndex = datasource.getCollationIndex();
    };
    for (MySQLDatasource datasource : datasourceList) {
      datasource.init(defaultCallBack);
    }
  }

  /**
   * 获取字符集
   */
  public MySQLCollationIndex getCollationIndex() {
    return collationIndex;
  }

  /**
   * 根据 1.是否读写分离 2.负载均衡策略 获取MySQL Session
   */
  public void getMySQLSessionByBalance(boolean runOnSlave, LoadBalanceStrategy strategy,
      AsyncTaskCallBack<MySQLClientSession> asynTaskCallBack) {
    MySQLDatasource datasource;
    if (!runOnSlave) {
      getWriteDatasource(asynTaskCallBack);
      return;
    }
    if (strategy == null) {
      strategy = this.defaultLoadBalanceStrategy;
    }
    datasource = strategy.select(this, writeIndex, this.datasourceList);
    if (datasource == null || !datasource.isAlive()) {
      getWriteDatasource(asynTaskCallBack);
    } else {
      getDatasource(datasource, asynTaskCallBack);
    }
  }

  /**
   * 获取写入(主)节点,如果主节点已经失效,则失败
   */
  private void getWriteDatasource(AsyncTaskCallBack<MySQLClientSession> asynTaskCallBack) {
    MySQLDatasource datasource = this.datasourceList.get(writeIndex);
    if (datasource == null || !datasource.isAlive()) {
      asynTaskCallBack.finished(null, this, false,
          ReplicaTip.NO_AVAILABLE_DATA_SOURCE.getMessage(this.getName()), null);
      return;
    }
    getDatasource(datasource, asynTaskCallBack);
    return;
  }

  /**
   * 根据MySQLDatasource获得MySQL Session 此函数是本类获取MySQL Session中最后一个必经的执行点,检验当前获得Session的线程是否MycatReactorThread
   */
  private void getDatasource(MySQLDatasource datasource,
      AsyncTaskCallBack<MySQLClientSession> asynTaskCallBack) {
    Objects.requireNonNull(datasource);
    Objects.requireNonNull(asynTaskCallBack);
    if (Thread.currentThread() instanceof MycatReactorThread) {
      MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
      reactor.getMySQLSessionManager().getIdleSessionsOfKey(datasource, asynTaskCallBack);
    } else {
      throw new MycatExpection(ReplicaTip.ERROR_EXECUTION_THREAD.getMessage());
    }
  }

  /**
   *
   */
  private void checkIndex(String name, int newIndex, int size) {
    if (newIndex < 0 || newIndex >= size) {
      throw new MycatExpection(ReplicaTip.ILLEGAL_REPLICA_INDEX.getMessage(name));
    }
  }

  /**
   * 获取集群名字
   */
  public String getName() {
    return config.getName();
  }

  public List<MySQLDatasource> getDatasourceList() {
    return Collections.unmodifiableList(datasourceList);
  }
}
