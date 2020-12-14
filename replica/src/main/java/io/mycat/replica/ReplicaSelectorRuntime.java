/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica;

import io.mycat.MetadataStorageManager;
import io.mycat.MycatConfig;
import io.mycat.config.*;
import io.mycat.plug.loadBalance.LoadBalanceElement;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.loadBalance.SessionCounter;
import io.mycat.replica.heartbeat.DefaultHeartbeatFlow;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;
import io.mycat.replica.heartbeat.strategy.MySQLGaleraHeartBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLMasterSlaveBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import io.mycat.util.CollectionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author : chenjunwen date Date : 2019年05月15日 21:34
 */
public class ReplicaSelectorRuntime implements Closeable {
    private final ConcurrentMap<String, ReplicaDataSourceSelector> replicaMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PhysicsInstance> physicsInstanceMap = new ConcurrentHashMap<>();
    ////////////////////////////////////////heartbeat///////////////////////////////////////////////////////////////////
    private final ConcurrentMap<String, HeartbeatFlow> heartbeatDetectorMap = new ConcurrentHashMap<>();
    private final Map<String, DatasourceConfig> datasources;
    private final LoadBalanceManager loadBalanceManager;
    private final List<ClusterConfig> replicaConfigList;
    private MetadataStorageManager metadataStorageManager;

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaSelectorRuntime.class);


    public ReplicaSelectorRuntime(List<ClusterConfig> clusters,
                                  Map<String, DatasourceConfig> datasources,
                                  LoadBalanceManager loadBalanceManager, MetadataStorageManager storageManager) {
        this.datasources = datasources;
        this.loadBalanceManager = loadBalanceManager;
        this.metadataStorageManager = storageManager;

        this.replicaConfigList = clusters;

        ////////////////////////////////////check/////////////////////////////////////////////////
        Objects.requireNonNull(replicaConfigList, "replica config can not be empty");
        ////////////////////////////////////check/////////////////////////////////////////////////

        for (ClusterConfig replicaConfig : replicaConfigList) {
            addCluster(this.datasources, replicaConfig);
        }


        //移除不必要的配置

        //新配置中的集群名字
        Set<String> clusterNames = clusters.stream().map(i -> i.getName()).collect(Collectors.toSet());
        new HashSet<>(replicaMap.keySet()).stream().filter(name -> !clusterNames.contains(name)).forEach(name -> replicaMap.remove(name));

        //新配置中的数据源名字
        Set<String> datasourceNames = datasources.keySet();
        new HashSet<>(physicsInstanceMap.keySet()).stream().filter(name -> !datasourceNames.contains(name)).forEach(name -> physicsInstanceMap.remove(name));

        List<PhysicsInstanceImpl> collect = replicaMap.values().stream().flatMap(i -> i.datasourceMap.values().stream()).collect(Collectors.toList());
        collect.forEach(c -> {
            c.notifyChangeSelectRead(true);
            c.notifyChangeAlive(true);
        });

        Map<String, PhysicsInstanceImpl> newphysicsInstanceMap = new HashMap<>();
        for (ReplicaDataSourceSelector i : replicaMap.values()) {
            for (PhysicsInstanceImpl k : i.datasourceMap.values()) {
                newphysicsInstanceMap.put(k.getName(), k);
            }
        }
        CollectionUtil.safeUpdateByUpdate(this.physicsInstanceMap, newphysicsInstanceMap);
    }


    /////////////////////////////////////////public manager/////////////////////////////////////////////////////////////

    public void addCluster(MycatConfig config, ClusterConfig replicaConfig) {
        addCluster(config.getDatasource().getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v)), replicaConfig);
    }

    public void removeCluster(String name) {
        replicaMap.remove(name);
    }

    public void addDatasource(ClusterConfig replicaConfig, String clusterName, DatasourceConfig datasource) {
        boolean master = replicaConfig.getMasters().contains(datasource.getName());
        ReplicaDataSourceSelector replicaDataSourceSelector = replicaMap.get(clusterName);
        registerDatasource(master, replicaDataSourceSelector, datasource, null);
    }

    public void removeDatasource(String clusterName, String datasourceName) {
        ReplicaDataSourceSelector selector = replicaMap.get(clusterName);
        if (selector != null) {
            selector.unregister(datasourceName);
        }
    }

    public synchronized boolean notifySwitchReplicaDataSource(String replicaName) {
        ReplicaDataSourceSelector selector = replicaMap.get(replicaName);
        Objects.requireNonNull(selector, replicaName + " 集群不存在");
        return selector.switchDataSourceIfNeed();
    }

    public void updateInstanceStatus(String replicaName, String dataSource, boolean alive,
                                     boolean selectAsRead) {
        ReplicaDataSourceSelector selector = replicaMap.get(replicaName);
        if (selector != null) {
            PhysicsInstanceImpl physicsInstance = selector.datasourceMap.get(dataSource);
            if (physicsInstance != null) {
                physicsInstance.notifyChangeAlive(alive);
                physicsInstance.notifyChangeSelectRead(selectAsRead);
            }
        }
    }

    public void updateInstanceStatus(String dataSource, boolean alive,
                                     boolean selectAsRead) {
        replicaMap.values().stream().flatMap(i -> i.datasourceMap.values().stream()).filter(i -> i.getName().equals(dataSource)).findFirst().ifPresent(physicsInstance -> {
            physicsInstance.notifyChangeAlive(alive);
            physicsInstance.notifyChangeSelectRead(selectAsRead);
        });
    }


    /**
     * @param dataSourceName
     * @param sessionCounter
     * @return 是否注册成功
     */
    public boolean registerDatasource(String dataSourceName, SessionCounter sessionCounter) {
        PhysicsInstance instance = this.physicsInstanceMap.get(dataSourceName);
        if (instance == null) {
            return false;
        }
        PhysicsInstanceImpl physicsInstance = (PhysicsInstanceImpl) instance;
        physicsInstance.addSessionCounter(sessionCounter);
        return true;
    }


///////////////////////////////////////private manager//////////////////////////////////////////////////////////////////////////

    private PhysicsInstance registerDatasource(boolean master, ReplicaDataSourceSelector selector,
                                               DatasourceConfig datasourceConfig,
                                               SessionCounter sessionCounter) {
        Objects.requireNonNull(selector);
        Objects.requireNonNull(datasourceConfig);
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
        if (datasourceConfig.getInstanceType() != null) {
            instanceType = InstanceType.valueOf(datasourceConfig.getInstanceType());
        }
        return registerDatasource(selector.name, datasourceConfig.getName(), instanceType,
                datasourceConfig.getWeight(), sessionCounter);
    }

    private void addCluster(Map<String, DatasourceConfig> datasourceConfigMap, ClusterConfig replicaConfig) {
        String name = replicaConfig.getName();
        ReplicaType replicaType = ReplicaType.valueOf(replicaConfig.getClusterType());
        BalanceType balanceType = BalanceType.valueOf(replicaConfig.getReadBalanceType());
        ReplicaSwitchType switchType = ReplicaSwitchType.valueOf(replicaConfig.getSwitchType());

        LoadBalanceStrategy readLB = loadBalanceManager.getLoadBalanceByBalanceName(replicaConfig.getReadBalanceName());
        LoadBalanceStrategy writeLB = loadBalanceManager.getLoadBalanceByBalanceName(replicaConfig.getWriteBalanceName());
        int maxRequestCount = replicaConfig.getMaxCon() == null ? Integer.MAX_VALUE : replicaConfig.getMaxCon();

        assert datasourceConfigMap.size() != 0;
        DatasourceConfig datasourceConfig = datasourceConfigMap.values().stream().iterator().next();
        String dbType = datasourceConfig.getDbType();

        ReplicaDataSourceSelector selector = registerCluster(
                name,
                dbType,
                balanceType,
                replicaType,
                maxRequestCount,
                switchType,
                readLB,
                writeLB,
                replicaConfig.getTimer());

        if (replicaConfig.getMasters() != null) {
            registerDatasource(datasourceConfigMap, selector, replicaConfig.getMasters(), true);
        }
        if (replicaConfig.getReplicas() != null) {
            registerDatasource(datasourceConfigMap, selector, replicaConfig.getReplicas(), false);
        }
    }

    private void registerDatasource(Map<String, DatasourceConfig> datasourceConfigMap, ReplicaDataSourceSelector selector, List<String> datasourceNameList, boolean master) {
        if (datasourceNameList == null) {
            datasourceNameList = Collections.emptyList();
        }
        for (String datasourceName : datasourceNameList) {
            DatasourceConfig datasource = datasourceConfigMap.get(datasourceName);
            registerDatasource(master, selector, datasource, null);
        }
    }

    private PhysicsInstance registerDatasource(String replicaName, String dataSourceName,
                                               InstanceType type,
                                               int weight, SessionCounter sessionCounter) {
        ReplicaDataSourceSelector sourceSelector = replicaMap.get(replicaName);
        Objects.requireNonNull(sourceSelector);
        PhysicsInstanceImpl instance = sourceSelector.register(dataSourceName, type, weight);
        if (sessionCounter != null) {
            instance.sessionCounters.add(sessionCounter);
        }
        return instance;
    }


    private ReplicaDataSourceSelector registerCluster(String replicaName,
                                                      String dbType,
                                                      BalanceType balanceType,
                                                      ReplicaType type,
                                                      int maxRequestCount,
                                                      ReplicaSwitchType switchType, LoadBalanceStrategy readLB,
                                                      LoadBalanceStrategy writeLB,
                                                      TimerConfig timer) {
        return replicaMap.computeIfAbsent(replicaName,
                s -> new ReplicaDataSourceSelector(replicaName, dbType, balanceType, type, maxRequestCount, switchType, readLB,
                        writeLB, timer, this));
    }

    //////////////////////////////////////////public read///////////////////////////////////////////////////////////////////
    public String getDatasourceNameByRandom() {
        ArrayList<ReplicaDataSourceSelector> values = new ArrayList<>(replicaMap.values());
        int i = ThreadLocalRandom.current().nextInt(0, values.size());
        ReplicaDataSourceSelector replicaDataSourceSelector = values.get(i);
        String name = replicaDataSourceSelector.getName();
        return getDatasourceNameByReplicaName(name, false, null);
    }

    public String getDatasourceNameByReplicaName(String replicaName, boolean master, String loadBalanceStrategy) {
        BiFunction<LoadBalanceStrategy, ReplicaDataSourceSelector, PhysicsInstanceImpl> function = master ? this::getWriteDatasource : this::getDatasource;
        ReplicaDataSourceSelector replicaDataSourceSelector = replicaMap.get(Objects.requireNonNull(replicaName));
        if (replicaDataSourceSelector == null) {
            return replicaName;
        }
        LoadBalanceStrategy loadBalanceByBalance = null;
        if (loadBalanceStrategy != null) {
            loadBalanceByBalance = loadBalanceManager.getLoadBalanceByBalanceName(loadBalanceStrategy);
        }//传null集群配置的负载均衡生效
        PhysicsInstanceImpl writeDatasource = function.apply(loadBalanceByBalance, replicaDataSourceSelector);
        if (writeDatasource == null) {
            return replicaName;
        }
        return writeDatasource.getName();
    }

    public PhysicsInstanceImpl getWriteDatasourceByReplicaName(String replicaName,
                                                               LoadBalanceStrategy balanceStrategy) {
        ReplicaDataSourceSelector selector = replicaMap.get(replicaName);
        if (selector == null) {
            return null;
        }
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
                                             List element) {
        Objects.requireNonNull(element);
        Objects.requireNonNull(selector);
        if (balanceStrategy == null) {
            balanceStrategy = defaultWriteLoadBalanceStrategy;
        }
        LoadBalanceElement select = balanceStrategy.select(selector, element);
        Objects.requireNonNull(select, "No data source available");
        return (PhysicsInstanceImpl) select;
    }

    public PhysicsInstanceImpl getDatasourceByReplicaName(String replicaName, boolean master, LoadBalanceStrategy balanceStrategy) {
        ReplicaDataSourceSelector selector = replicaMap.get(replicaName);
        if (selector == null) {
            return null;
        }
        if (master) {
            return getWriteDatasourceByReplicaName(replicaName, balanceStrategy);
        } else {
            return getDatasource(balanceStrategy, selector,
                    selector.defaultReadLoadBalanceStrategy, selector.getDataSourceByLoadBalacneType());
        }
    }

    public ReplicaDataSourceSelector getDataSourceSelector(String replicaName) {
        return replicaMap.get(replicaName);
    }


    public synchronized void putHeartFlow(String replicaName, String datasourceName, Consumer<HeartBeatStrategy> executer) {
        String name = replicaName + "." + datasourceName;
        if (!heartbeatDetectorMap.containsKey(name)) {
            this.replicaConfigList.stream().filter(i -> replicaName.equals(i.getName())).findFirst().ifPresent(c -> {
                HeartbeatConfig heartbeat = c.getHeartbeat();
                if (heartbeat != null) {
                    ReplicaDataSourceSelector selector = replicaMap.get(replicaName);
                    PhysicsInstanceImpl physicsInstance = selector.datasourceMap.get(datasourceName);
                    DefaultHeartbeatFlow heartbeatFlow = new DefaultHeartbeatFlow(this, physicsInstance, replicaName, datasourceName,
                            heartbeat.getMaxRetry(), heartbeat.getMinSwitchTimeInterval(), heartbeat.getHeartbeatTimeout(),
                            ReplicaSwitchType.valueOf(c.getSwitchType()),
                            heartbeat.getSlaveThreshold(), getStrategyByReplicaType(c.getClusterType()),
                            executer);

                    heartbeatDetectorMap.put(name, heartbeatFlow);
                }
            });
        }
    }

    public void removeHeartFlow(String replicaName, String datasourceName) {
        heartbeatDetectorMap.remove(replicaName + "." + datasourceName);
    }

    private Function<HeartbeatFlow, HeartBeatStrategy> getStrategyByReplicaType(String replicaType) {
        Function<HeartbeatFlow, HeartBeatStrategy> strategyProvider;
        switch (ReplicaType.valueOf(replicaType)) {
            case MASTER_SLAVE:
                strategyProvider = MySQLMasterSlaveBeatStrategy::new;
                break;
            case GARELA_CLUSTER:
                strategyProvider = MySQLGaleraHeartBeatStrategy::new;
                break;
            case NONE:
            case SINGLE_NODE:
            default:
                strategyProvider = MySQLSingleHeartBeatStrategy::new;
                break;
        }
        return strategyProvider;
    }

    public boolean isReplicaName(String targetName) {
        return replicaMap.containsKey(targetName);
    }


    public PhysicsInstance getPhysicsInstanceByName(String name) {
        return physicsInstanceMap.get(name);
    }

    public boolean isDatasource(String targetName) {
        return this.physicsInstanceMap.containsKey(targetName);
    }


    public Map<String, ReplicaDataSourceSelector> getReplicaMap() {
        return Collections.unmodifiableMap(replicaMap);
    }


    public Map<String, PhysicsInstance> getPhysicsInstanceMap() {
        return Collections.unmodifiableMap(physicsInstanceMap);
    }

    public Map<String, HeartbeatFlow> getHeartbeatDetectorMap() {
        return Collections.unmodifiableMap(heartbeatDetectorMap);
    }

    public List<String> getRepliaNameListByInstanceName(String name) {
        List<String> replicaDataSourceSelectorList = new ArrayList<>();
        for (ReplicaDataSourceSelector replicaDataSourceSelector : this.getReplicaMap().values()) {
            for (PhysicsInstance physicsInstance : replicaDataSourceSelector.getRawDataSourceMap().values()) {
                if (name.equals(physicsInstance.getName())) {
                    replicaDataSourceSelectorList.add(replicaDataSourceSelector.getName());
                }
            }
        }
        return replicaDataSourceSelectorList;
    }

    @Override
    public void close() {
        for (ReplicaDataSourceSelector i : replicaMap.values()) {
            try {
                i.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public String getDbTypeByTargetName(String name) {
        ReplicaDataSourceSelector replicaDataSourceSelector = this.getReplicaMap().get(name);
        if (replicaDataSourceSelector != null) {
            return replicaDataSourceSelector.getDbType();
        }
        DatasourceConfig datasourceConfig = datasources.get(name);
        Objects.requireNonNull(datasourceConfig, "unknown dbType:" + name);
        return datasourceConfig.getDbType();
    }
}