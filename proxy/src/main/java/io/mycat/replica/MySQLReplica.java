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

import io.mycat.MycatException;
import io.mycat.ProxyBeanProviders;
import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.MycatConfigUtil;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.handler.backend.MySQLDataSourceQuery;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.SessionManager.PartialType;
import io.mycat.proxy.session.SessionManager.SessionIdAble;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * 集群管理,该类不执行心跳,也不管理jdbc的mysqlsession,只做均衡负载 集群状态在集群相关辅助类实现,辅助类使用定时器分发到执行.辅助类只能更改此类的writeIndex属性,其他属性不是线程安全,初始化之后只读
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class MySQLReplica implements MycatReplica {

  protected static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLReplica.class);
  protected final ReplicaConfig config;
  protected final ProxyRuntime runtime;
  protected final List<MySQLDatasource> mySQLDatasources = new ArrayList<>();
  protected final ReplicaDataSourceSelector selector;

  /**
   * 初始化mycat集群管理
   *
   * @param replicaConfig the config of replica
   * @param dataSourceFactory a factory to create dataSource
   */
  public MySQLReplica(ProxyRuntime runtime, ReplicaConfig replicaConfig,
      ProxyBeanProviders dataSourceFactory) {
    assert replicaConfig != null;
    List<DatasourceConfig> mysqls = replicaConfig.getDatasources();
    assert mysqls != null;
    this.runtime = runtime;
    this.config = replicaConfig;
    this.selector = ReplicaRuntime.INSTCANE
        .getDataSourceSelector(replicaConfig.getName());
    List<DatasourceConfig> datasources = config.getDatasources();
    if (datasources != null) {
      for (int i = 0; i < datasources.size(); i++) {
        MySQLDatasource mySQLDatasource = null;
        DatasourceConfig datasourceConfig = datasources.get(i);
        if (MycatConfigUtil.isMySQLType(datasourceConfig)) {
          mySQLDatasource = dataSourceFactory.createDatasource(runtime, i, datasourceConfig, this);
        }
        mySQLDatasources.add(mySQLDatasource);
      }
    }
  }

  public void getMySQLSessionByBalance(MySQLDataSourceQuery query,
      SessionCallBack<MySQLClientSession> asynTaskCallBack) {
    boolean isRunOnMaster = true;
    LoadBalanceStrategy lbs = null;
    List<SessionIdAble> ids = null;
    if (query != null) {
      isRunOnMaster = query.isRunOnMaster();
      lbs = query.getStrategy();
      ids = query.getIds();
    }
    MySQLDatasource datasource = getMySQLSessionByBalance(isRunOnMaster, lbs);
    getSessionCallback(datasource, ids, asynTaskCallBack);
  }

  public MySQLDatasource getMySQLSessionByBalance(boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    PhysicsInstanceImpl instance =
        runOnMaster ? ReplicaRuntime.INSTCANE.getWriteDatasource(strategy, selector)
            : ReplicaRuntime.INSTCANE.getDatasource(strategy, selector);
    return mySQLDatasources.get(instance.getIndex());
  }

  /**
   * 根据MySQLDatasource获得MySQL Session 此函数是本类获取MySQL Session中最后一个必经的执行点,检验当前获得Session的线程是否MycatReactorThread
   */
  private void getSessionCallback(MySQLDatasource datasource, List<SessionIdAble> ids,
      SessionCallBack<MySQLClientSession> asynTaskCallBack) {
    Objects.requireNonNull(datasource);
    Objects.requireNonNull(asynTaskCallBack);
    if (Thread.currentThread() instanceof MycatReactorThread) {
      MycatReactorThread reactor = (MycatReactorThread) Thread.currentThread();
      reactor.getMySQLSessionManager()
          .getIdleSessionsOfIdsOrPartial(datasource, ids, PartialType.RANDOM_ID, asynTaskCallBack);
    } else {
      MycatException mycatExpection = new MycatException(
          "Replica must running in MycatReactorThread");
      asynTaskCallBack.onException(mycatExpection, this, null);
      return;
    }
  }

  /**
   * 获取集群名字
   */
  @Override
  public String getName() {
    return config.getName();
  }

  /**
   * 注意判断数组中的null元素
   */
  public List<MySQLDatasource> getDatasourceList() {
    return Collections.unmodifiableList(mySQLDatasources);
  }

  public boolean isMaster(MySQLDatasource datasource) {
    return mySQLDatasources.contains(datasource);
  }

  @Override
  public ReplicaConfig getReplicaConfig() {
    return config;
  }

  public boolean switchDataSourceIfNeed() {
    return selector.switchDataSourceIfNeed();
  }

  public ReplicaSwitchType getSwitchType() {
    return selector.getSwitchType();
  }
}
