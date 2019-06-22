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
import io.mycat.ProxyBeanProviders;
import io.mycat.beans.mycat.MycatReplica;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaConfig.BalanceTypeEnum;
import io.mycat.logTip.ReplicaTip;
import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.callback.AsyncTaskCallBackCounter;
import io.mycat.proxy.callback.SessionCallBack;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.SessionManager.SessionIdAble;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 集群管理,该类不执行心跳,也不管理jdbc的mysqlsession,只做均衡负载 集群状态在集群相关辅助类实现,辅助类使用定时器分发到执行.辅助类只能更改此类的writeIndex属性,其他属性不是线程安全,初始化之后只读
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class MySQLReplica implements MycatReplica, LoadBalanceInfo {

  static Logger logger = LoggerFactory.getLogger(MySQLReplica.class);

  private final ReplicaConfig config;
  private final List<MySQLDatasource> datasourceList = new ArrayList<>();
  private final CopyOnWriteArrayList<MySQLDatasource> writeDataSource = new CopyOnWriteArrayList<>(); //主节点默认为0
  private long lastInitTime;  //最后一次初始化时间
  private LoadBalanceStrategy defaultLoadBalanceStrategy;


  /**
   * 初始化mycat集群管理
   *
   * @param replicaConfig the config of replica
   * @param writeIndex master index
   * @param dataSourceFactory a factory to create dataSource
   */
  public MySQLReplica(ReplicaConfig replicaConfig,
      Set<Integer> writeIndex, ProxyBeanProviders dataSourceFactory) {
    assert replicaConfig != null;
    assert writeIndex.size() > 0;
    List<DatasourceConfig> mysqls = replicaConfig.getMysqls();
    defaultLoadBalanceStrategy = ProxyRuntime.INSTANCE
        .getLoadBalanceByBalanceName(replicaConfig.getBalanceName());
    assert mysqls != null;
    this.config = replicaConfig;
    for (int index = 0; index < mysqls.size(); index++) {
      DatasourceConfig datasourceConfig = mysqls.get(index);
      assert datasourceConfig != null;
      if (datasourceConfig.getDbType() == null) {
        MySQLDatasource datasource = dataSourceFactory
            .createDatasource(index, datasourceConfig, this);
        datasourceList.add(datasource);
        if (writeIndex.contains(index)) {
          writeDataSource.add(datasource);
        }
      }
    }
  }

  /**
   * @return 获取最后一次初始化时间
   */
  public long getLastInitTime() {
    return lastInitTime;
  }

  /**
   * 对于已经运行的集群,首先把原session都关闭再重新创建
   *
   * @param callBack callback function
   */
  public void init(AsyncTaskCallBackCounter callBack) {
    Objects.requireNonNull(config);
    Objects.requireNonNull(datasourceList);
    Objects.requireNonNull(callBack);

//    for (MySQLDatasource datasource : datasourceList) {
//      datasource.clearAndDestroyCons(ReplicaTip.INIT_REPLICA.getMessage(getName()));
//    }
    for (MySQLDatasource datasource : datasourceList) {
      datasource.init(callBack);
    }
    this.lastInitTime = System.currentTimeMillis();
  }

  /**
   * 根据 1.是否读写分离 2.负载均衡策略 获取MySQL Session
   *
   * @param runOnMaster is runOnMaster
   * @param strategy balanceStrategy
   * @param asynTaskCallBack callback function
   */
  public void getMySQLSessionByBalance(boolean runOnMaster, LoadBalanceStrategy strategy,
      List<SessionIdAble> ids,
      SessionCallBack<MySQLClientSession> asynTaskCallBack) {
    MySQLDatasource datasource = getMySQLSessionByBalance(runOnMaster, strategy);
    getSessionCallback(datasource, ids, asynTaskCallBack);
  }

  public MySQLDatasource getMySQLSessionByBalance(boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    MySQLDatasource datasource;
    if (runOnMaster) {
      datasource = getWriteDatasource(strategy);
      return datasource;
    }
    if (strategy == null) {
      strategy = this.defaultLoadBalanceStrategy;
    }
    List activeDataSource = getDataSourceByLoadBalacneType();
    datasource = (MySQLDatasource) strategy.select(this, activeDataSource);
    if (datasource == null) {
      datasource = getWriteDatasource(strategy);
      return datasource;
    }
    return datasource;
  }

  private List<MySQLDatasource> getDataSourceByLoadBalacneType() {
    BalanceTypeEnum balanceType = this.getConfig().getBalanceType();
    Objects.requireNonNull(balanceType, "balanceType is null");
    switch (balanceType) {
      case BALANCE_ALL:
        List<MySQLDatasource> list = new ArrayList<>(this.datasourceList.size());
        for (MySQLDatasource datasource : this.datasourceList) {
          if (datasource.isAlive()) {
            list.add(datasource);
          }
        }
        return list;
      case BALANCE_NONE:
        return getMaster();
      case BALANCE_ALL_READ:
        List<MySQLDatasource> result = new ArrayList<>(this.datasourceList.size());
        for (MySQLDatasource mySQLDatasource : this.datasourceList) {
          if (mySQLDatasource.isAlive() && mySQLDatasource.asSelectRead()) {
            result.add(mySQLDatasource);
          }
        }
        return result;
      default:
        return Collections.EMPTY_LIST;
    }

  }

  /**
   * 获取写入(主)节点,如果主节点已经失效,则失败
   */
  private MySQLDatasource getWriteDatasource(LoadBalanceStrategy strategy) {
    if (strategy == null) {
      strategy = this.defaultLoadBalanceStrategy;
    }
    List writeDataSource = this.writeDataSource;
    MySQLDatasource datasource = (MySQLDatasource) strategy
        .select(this, writeDataSource);
    if (datasource == null || !datasource.isAlive()) {
      return null;
    }
    return datasource;
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
      reactor.getMySQLSessionManager().getIdleSessionsOfIds(datasource, ids, asynTaskCallBack);
    } else {
      MycatExpection mycatExpection = new MycatExpection(
          ReplicaTip.ERROR_EXECUTION_THREAD.getMessage());
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

  public List<MySQLDatasource> getDatasourceList() {
    return Collections.unmodifiableList(datasourceList);
  }

  final Map<String, Map<String, Set<String>>> metaData = new HashMap<>();

  public List<MySQLDatasource> getMaster() {
    int size = writeDataSource.size();
    MySQLDatasource datasource = writeDataSource.get(0);
    if (size == 1) {
      return datasource.isAlive() ? Collections.singletonList(datasource) : Collections.emptyList();
    }
    ArrayList<MySQLDatasource> datasources = new ArrayList<>(writeDataSource.size());
    for (int i = 0; i < size; i++) {
      datasource = (writeDataSource.get(i));
      if (datasource.isAlive()) {
        datasources.add(datasource);
      } else {
        //writeDataSource.remove(i);
      }
    }
    return datasources;
  }

  public void addMetaData(String schemaName, String tableName, String columnName) {
    Map<String, Set<String>> schemaMap = metaData.get(schemaName);
    if (schemaMap == null) {
      schemaMap = new HashMap<>();
    }
    Set<String> table = schemaMap.get(tableName);
    if (table == null) {
      table = new HashSet<>();
    }
    table.add(columnName);

    schemaMap.put(tableName, table);

    metaData.put(schemaName, schemaMap);

  }

  public Map<String, Map<String, Set<String>>> getMetaData() {
    return metaData;
  }

  public ReplicaConfig getConfig() {
    return config;
  }

  public boolean isMaster(MySQLDatasource datasource) {
    return writeDataSource.contains(datasource);
  }


  /**
   * 切换写节点
   *
   * @return is switch successful
   */
  public boolean switchDataSourceIfNeed() {
    switch (this.getConfig().getRepType()) {
      case SINGLE_NODE:
      case MASTER_SLAVE: {
        for (int i = 0; i < this.datasourceList.size(); i++) {
          if (datasourceList.get(i).isAlive()) {
            logger.info("{} switch master to {}", this, i);
            return true;
          }
        }
      }
      case GARELA_CLUSTER:
        List<MySQLDatasource> remove = new ArrayList<>();
        for (int i = 0; i < writeDataSource.size(); i++) {
          MySQLDatasource datasource = writeDataSource.get(i);
          if (!datasource.isAlive()) {
            remove.add(datasource);
          }
        }
        writeDataSource.removeAll(remove);
        break;
    }
    return false;
  }
}
