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

import io.mycat.ConfigRuntime;
import io.mycat.config.ConfigFile;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceElement;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.loadBalance.SessionCounter;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum ReplicaSelectorRuntime {
  INSTCANE;

  final ConcurrentMap<String, ReplicaDataSourceSelector> map = new ConcurrentHashMap<>();

  public void load() {
    load(false);
  }

  public void load(boolean force) {
    if (force || map.isEmpty()) {
      map.clear();
      innerThis();
    }
  }

  private void innerThis() {
    PlugRuntime.INSTCANE.load();
    ReplicasRootConfig dsConfig = ConfigRuntime.INSTCANE.getConfig(ConfigFile.DATASOURCE);
    MasterIndexesRootConfig replicaIndexConfig = ConfigRuntime.INSTCANE
        .getConfig(ConfigFile.REPLICA_INDEX);
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(dsConfig, "replica config can not found");
    Objects.requireNonNull(dsConfig.getReplicas(), "replica config can not be empty");
    Objects.requireNonNull(replicaIndexConfig, "master indexes can not found");
    Objects
        .requireNonNull(replicaIndexConfig.getMasterIndexes(), "master indexes can not be empty");
    ////////////////////////////////////check/////////////////////////////////////////////////
    List<ReplicaConfig> replicas = dsConfig.getReplicas();
    Map<String, String> replicaIndexes = replicaIndexConfig.getMasterIndexes();
    int size = replicas.size();
    for (int i = 0; i < size; i++) {
      ReplicaConfig replicaConfig = replicas.get(i);
      ////////////////////////////////////check/////////////////////////////////////////////////
      Objects.requireNonNull(replicaConfig.getName(), "replica name can not be empty");
      Objects.requireNonNull(replicaConfig.getRepType(), "replica message can not be empty");
      Objects
          .requireNonNull(replicaConfig.getSwitchType(), "replica switch message can not be empty");
      Objects
          .requireNonNull(replicaConfig.getReadBalanceName(),
              "replica balance name can not be empty");
      Objects
          .requireNonNull(replicaConfig.getReadbalanceType(),
              "replica balance message can not be empty");
      if (replicaConfig.getDatasources() == null) {
        return;
      }
      ////////////////////////////////////check/////////////////////////////////////////////////

      String name = replicaConfig.getName();
      ReplicaType replicaType = ReplicaType.valueOf(replicaConfig.getRepType());
      BalanceType balanceType = BalanceType.valueOf(replicaConfig.getReadbalanceType());
      ReplicaSwitchType switchType = ReplicaSwitchType.valueOf(replicaConfig.getSwitchType());

      LoadBalanceStrategy readLB = PlugRuntime.INSTCANE
          .getLoadBalanceByBalanceName(replicaConfig.getReadBalanceName());
      LoadBalanceStrategy writeLB
          = PlugRuntime.INSTCANE
          .getLoadBalanceByBalanceName(replicaConfig.getWriteBalanceName());

      ReplicaDataSourceSelector selector = registerReplica(name, balanceType,
          replicaType, switchType, readLB, writeLB);

      List<DatasourceConfig> datasources = replicaConfig.getDatasources();
      if (datasources == null) {
        datasources = Collections.emptyList();
      }
      for (int j = 0; j < datasources.size(); j++) {
        DatasourceConfig datasource = datasources.get(j);
        registerDatasource(selector, datasource, j, null);
      }
      selector.registerFininshed();
    }
  }

  public PhysicsInstanceImpl getWriteDatasourceByReplicaName(String replicaName) {
    return getWriteDatasourceByReplicaName(replicaName, null);
  }

  public PhysicsInstanceImpl getDatasourceByReplicaName(String replicaName) {
    return getDatasourceByReplicaName(replicaName, null);
  }

  public PhysicsInstanceImpl getWriteDatasourceByReplicaName(String replicaName,
      LoadBalanceStrategy balanceStrategy) {
    ReplicaDataSourceSelector selector = map.get(replicaName);
    return getDatasource(balanceStrategy, selector,
        selector.defaultWriteLoadBalanceStrategy, selector.getWriteDataSource());
  }

  public PhysicsInstanceImpl getWriteDatasource(LoadBalanceStrategy balanceStrategy,
      ReplicaDataSourceSelector selector) {
    return getDatasource(balanceStrategy, selector, selector.defaultWriteLoadBalanceStrategy,
        selector.getWriteDataSource());
  }

  public PhysicsInstanceImpl getDatasource(LoadBalanceStrategy balanceStrategy,
      ReplicaDataSourceSelector selector) {
    return getDatasource(balanceStrategy, selector, selector.defaultReadLoadBalanceStrategy,
        selector.getDataSourceByLoadBalacneType());
  }

  public PhysicsInstanceImpl getDatasource(LoadBalanceStrategy balanceStrategy,
      ReplicaDataSourceSelector selector, LoadBalanceStrategy defaultWriteLoadBalanceStrategy,
      List writeDataSource) {
    Objects.requireNonNull(selector);
    if (balanceStrategy == null) {
      balanceStrategy = defaultWriteLoadBalanceStrategy;
    }
    LoadBalanceElement select = balanceStrategy.select(selector, writeDataSource);
    Objects.requireNonNull(select);
    return (PhysicsInstanceImpl) select;
  }

  public PhysicsInstanceImpl getDatasourceByReplicaName(String replicaName,
      LoadBalanceStrategy balanceStrategy) {
    ReplicaDataSourceSelector selector = map.get(replicaName);
    return getDatasource(balanceStrategy, selector,
        selector.defaultReadLoadBalanceStrategy, selector.getDataSourceByLoadBalacneType());
  }

  public boolean notifySwitchReplicaDataSource(String replicaName) {
    ReplicaDataSourceSelector selector = map.get(replicaName);
    Objects.requireNonNull(selector);
    return selector.switchDataSourceIfNeed();
  }

  public ReplicaDataSourceSelector registerReplica(String replicaName, BalanceType balanceType,
      ReplicaType type,
      ReplicaSwitchType switchType, LoadBalanceStrategy readLB,
      LoadBalanceStrategy writeLB) {
    return map.computeIfAbsent(replicaName,
        s -> new ReplicaDataSourceSelector(replicaName, balanceType, type, switchType, readLB,
            writeLB));
  }

  public ReplicaDataSourceSelector getDataSourceSelector(String replicaName) {
    return map.get(replicaName);
  }

  public void updateInstanceStatus(String replicaName, String dataSource, boolean alive,
      boolean selectAsRead) {
    ReplicaDataSourceSelector selector = map.get(replicaName);
    PhysicsInstanceImpl physicsInstance = selector.datasourceMap.get(dataSource);
    physicsInstance.notifyChangeAlive(alive);
    physicsInstance.notifyChangeSelectRead(selectAsRead);
  }

  public PhysicsInstance registerDatasource(String replicaName, DatasourceConfig config,
      int index,
      SessionCounter sessionCounter) {
    return registerDatasource(map.get(replicaName), config, index, sessionCounter);
  }

  public PhysicsInstance registerDatasource(ReplicaDataSourceSelector selector,
      DatasourceConfig config,
      int index,
      SessionCounter sessionCounter) {
    Objects.requireNonNull(selector);
    boolean master = ConfigRuntime.INSTCANE.getReplicaIndexes(selector.getName()).contains(index);
    InstanceType instanceType = InstanceType.READ;
    switch (selector.type) {
      case SINGLE_NODE:
      case MASTER_SLAVE:
        instanceType = master ? InstanceType.READ_WRITE : InstanceType.READ;
        break;
      case GARELA_CLUSTER:
        instanceType = master ? InstanceType.READ_WRITE : InstanceType.READ;
      case NONE:
        break;
    }
    if (config.getInstanceType() != null) {
      instanceType = InstanceType.valueOf(config.getInstanceType());
    }
    return registerDatasource(selector.name, config.getName(), index, instanceType,
        config.getWeight(), sessionCounter);
  }

  public PhysicsInstance registerDatasource(String replicaName, String dataSourceName, int index,
      InstanceType type,
      int weight, SessionCounter sessionCounter) {
    ReplicaDataSourceSelector sourceSelector = map.get(replicaName);
    Objects.requireNonNull(sourceSelector);
    PhysicsInstanceImpl instance = sourceSelector.register(index, dataSourceName, type, weight);
    if (sessionCounter != null) {
      instance.sessionCounters.add(sessionCounter);
    }
    return instance;
  }
}