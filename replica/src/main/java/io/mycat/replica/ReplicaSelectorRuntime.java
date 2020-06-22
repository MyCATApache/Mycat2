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

import io.mycat.MycatConfig;
import io.mycat.MycatException;
import io.mycat.ScheduleUtil;
import io.mycat.config.ClusterRootConfig;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.config.TimerConfig;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceElement;
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

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author : chenjunwen date Date : 2019年05月15日 21:34
 */
public enum ReplicaSelectorRuntime {
    INSTANCE;
    final ConcurrentMap<String, ReplicaDataSourceSelector> replicaMap = new ConcurrentHashMap<>();
    final ConcurrentMap<String, PhysicsInstance> physicsInstanceMap = new ConcurrentHashMap<>();
    volatile ScheduledFuture<?> schedule;
    volatile MycatConfig config;
    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaSelectorRuntime.class);

    public synchronized void load(MycatConfig config) {
        Objects.requireNonNull(config);
        if (this.config == config) {
            return;
        }
        innerThis(config);
        this.config = config;
    }

    private void innerThis(MycatConfig config) {
        PlugRuntime.INSTCANE.load(config);

        ClusterRootConfig replicasRootConfig = config.getCluster();
        Objects.requireNonNull(replicasRootConfig, "replica config can not found");

        List<ClusterRootConfig.ClusterConfig> replicaConfigList = replicasRootConfig.getClusters();

        List<DatasourceRootConfig.DatasourceConfig> datasources = config.getDatasource().getDatasources();
        Map<String, DatasourceRootConfig.DatasourceConfig> datasourceConfigMap = datasources.stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
        ////////////////////////////////////check/////////////////////////////////////////////////
        Objects.requireNonNull(replicaConfigList, "replica config can not be empty");
        ////////////////////////////////////check/////////////////////////////////////////////////

        for (ClusterRootConfig.ClusterConfig replicaConfig : replicaConfigList) {
            addCluster(datasourceConfigMap, replicaConfig);
        }

        updateTimer(config);

        Map<String, PhysicsInstanceImpl> newphysicsInstanceMap = replicaMap.values().stream().flatMap(i -> i.datasourceMap.values().stream()).collect(Collectors.toMap(k -> k.getName(), v -> v));
        CollectionUtil.safeUpdate(this.physicsInstanceMap, newphysicsInstanceMap);
    }
    public synchronized void restartHeatbeat(){
        if (config == null){
           throw new MycatException("restartHeatbeat fail because config is null");
        }else {
            updateTimer(config);
        }
    }
    public synchronized void updateTimer(MycatConfig config) {
        /////////////////////////////////////////////////////////////////////////////////////////
        stopHeartBeat();
        /////////////////////////////////////////////////////////////////////////////////////////////
        ClusterRootConfig replicas = config.getCluster();
        TimerConfig timerConfig = replicas.getTimer();
        List<PhysicsInstanceImpl> collect = replicaMap.values().stream().flatMap(i -> i.datasourceMap.values().stream()).collect(Collectors.toList());

        if (replicas.isClose()) {
            collect.forEach(c -> {
                c.notifyChangeSelectRead(true);
                c.notifyChangeAlive(true);
            });
        } else {
            collect.forEach(c -> {
                c.notifyChangeSelectRead(true);
                c.notifyChangeAlive(true);
            });
            this.schedule = ScheduleUtil.getTimer().scheduleAtFixedRate(() -> {
                        for (Map.Entry<String, ReplicaDataSourceSelector> stringReplicaDataSourceSelectorEntry : replicaMap.entrySet()) {
                            for (String datasourceName : stringReplicaDataSourceSelectorEntry.getValue().datasourceMap.keySet()) {
                                String replicaName = stringReplicaDataSourceSelectorEntry.getKey();
                                HeartbeatFlow heartbeatFlow = heartbeatDetectorMap.get(replicaName + "." + datasourceName);
                                if (heartbeatFlow != null) {
                                    if (LOGGER.isDebugEnabled()) {
                                        LOGGER.debug("heartbeat");
                                    }
                                    heartbeatFlow.heartbeat();
                                }
                            }
                        }
                    }
                    , timerConfig.getInitialDelay(), timerConfig.getPeriod(), TimeUnit.valueOf(timerConfig.getTimeUnit()));
        }
    }

    public void stopHeartBeat() {
        if (this.schedule != null) {
            schedule.cancel(false);
            schedule = null;
        }
    }


    /////////////////////////////////////////public manager/////////////////////////////////////////////////////////////

    public void addCluster(MycatConfig config, ClusterRootConfig.ClusterConfig replicaConfig) {
        addCluster(config.getDatasource().getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v)), replicaConfig);
    }

    public void removeCluster(String name) {
        replicaMap.remove(name);
    }

    public void addDatasource(ClusterRootConfig.ClusterConfig replicaConfig, String clusterName, DatasourceRootConfig.DatasourceConfig datasource) {
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
        Objects.requireNonNull(selector);
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


    public PhysicsInstance registerDatasource(String dataSourceName, SessionCounter sessionCounter) {
        PhysicsInstance instance = this.physicsInstanceMap.get(dataSourceName);
        if (instance == null) {
            throw new MycatException(dataSourceName + " is not existed");
        }
        PhysicsInstanceImpl physicsInstance = (PhysicsInstanceImpl) instance;
        physicsInstance.addSessionCounter(sessionCounter);
        return physicsInstance;
    }


///////////////////////////////////////private manager//////////////////////////////////////////////////////////////////////////

    private PhysicsInstance registerDatasource(boolean master, ReplicaDataSourceSelector selector,
                                               DatasourceRootConfig.DatasourceConfig datasourceConfig,
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

    private void addCluster(Map<String, DatasourceRootConfig.DatasourceConfig> datasourceConfigMap, ClusterRootConfig.ClusterConfig replicaConfig) {
        String name = replicaConfig.getName();
        ReplicaType replicaType = ReplicaType.valueOf(replicaConfig.getReplicaType());
        BalanceType balanceType = BalanceType.valueOf(replicaConfig.getReadBalanceType());
        ReplicaSwitchType switchType = ReplicaSwitchType.valueOf(replicaConfig.getSwitchType());

        LoadBalanceStrategy readLB = PlugRuntime.INSTCANE
                .getLoadBalanceByBalanceName(replicaConfig.getReadBalanceName());
        LoadBalanceStrategy writeLB
                = PlugRuntime.INSTCANE
                .getLoadBalanceByBalanceName(replicaConfig.getWriteBalanceName());
        int maxRequestCount = replicaConfig.getMaxCon() == null ? Integer.MAX_VALUE : replicaConfig.getMaxCon();
        ReplicaDataSourceSelector selector = registerCluster(name, balanceType,
                replicaType, maxRequestCount, switchType, readLB, writeLB);

        registerDatasource(datasourceConfigMap, selector, replicaConfig.getMasters(), true);
        registerDatasource(datasourceConfigMap, selector, replicaConfig.getReplicas(), false);
    }

    private void registerDatasource(Map<String, DatasourceRootConfig.DatasourceConfig> datasourceConfigMap, ReplicaDataSourceSelector selector, List<String> datasourceNameList, boolean master) {
        if (datasourceNameList == null) {
            datasourceNameList = Collections.emptyList();
        }
        for (String datasourceName : datasourceNameList) {
            DatasourceRootConfig.DatasourceConfig datasource = datasourceConfigMap.get(datasourceName);
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


    private ReplicaDataSourceSelector registerCluster(String replicaName, BalanceType balanceType,
                                                      ReplicaType type,
                                                      int maxRequestCount,
                                                      ReplicaSwitchType switchType, LoadBalanceStrategy readLB,
                                                      LoadBalanceStrategy writeLB) {
        return replicaMap.computeIfAbsent(replicaName,
                s -> new ReplicaDataSourceSelector(replicaName, balanceType, type, maxRequestCount, switchType, readLB,
                        writeLB));
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
        ReplicaDataSourceSelector replicaDataSourceSelector = replicaMap.get(replicaName);
        if (replicaDataSourceSelector == null) {
            return replicaName;
        }
        LoadBalanceStrategy loadBalanceByBalance = null;
        if (loadBalanceStrategy != null) {
            loadBalanceByBalance = PlugRuntime.INSTCANE.getLoadBalanceByBalanceName(loadBalanceStrategy);
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
    ////////////////////////////////////////heartbeat///////////////////////////////////////////////////////////////////

    final ConcurrentMap<String, HeartbeatFlow> heartbeatDetectorMap = new ConcurrentHashMap<>();

    public synchronized void putHeartFlow(String replicaName, String datasourceName, Consumer<HeartBeatStrategy> executer) {
        MycatConfig config = this.config;
        Objects.requireNonNull(config);
        String name = replicaName + "." + datasourceName;
        if (!heartbeatDetectorMap.containsKey(name)) {
            config.getCluster().getClusters().stream().filter(i -> replicaName.equals(i.getName())).findFirst().ifPresent(c -> {
                ClusterRootConfig.HeartbeatConfig heartbeat = c.getHeartbeat();
                ReplicaDataSourceSelector selector = replicaMap.get(replicaName);
                PhysicsInstanceImpl physicsInstance = selector.datasourceMap.get(datasourceName);
                DefaultHeartbeatFlow heartbeatFlow = new DefaultHeartbeatFlow(physicsInstance, replicaName, datasourceName,
                        heartbeat.getMaxRetry(), heartbeat.getMinSwitchTimeInterval(), heartbeat.getHeartbeatTimeout(),
                        ReplicaSwitchType.valueOf(c.getSwitchType()),
                        heartbeat.getSlaveThreshold(), getStrategyByReplicaType(c.getReplicaType()),
                        executer);

                heartbeatDetectorMap.put(name, heartbeatFlow);
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


    public String getPrototypeOrFirstReplicaDataSource() {
        Optional<MycatConfig> config = Optional.ofNullable(this.config);
        Optional<String> prototype = config.map(i -> i.getMetadata()).map(i -> i.getPrototype()).map(i -> i.getTargetName());
        String targetName = prototype.orElseGet(() -> {
            return config.map(c -> c.getCluster())
                    .filter(c -> c.getClusters() != null && c.getClusters().isEmpty())
                    .map(c -> c.getClusters().get(0)).map(c -> getDatasourceNameByReplicaName(c.getName(), false, null))
                    .orElseGet(() -> this.config.getDatasource().getDatasources().get(0).getName());
        });
        return getDatasourceNameByReplicaName(targetName, true, null);
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
        for (ReplicaDataSourceSelector replicaDataSourceSelector : ReplicaSelectorRuntime.INSTANCE.getReplicaMap().values()) {
            for (PhysicsInstance physicsInstance : replicaDataSourceSelector.getRawDataSourceMap().values()) {
                if (name.equals(physicsInstance.getName())) {
                    replicaDataSourceSelectorList.add(replicaDataSourceSelector.getName());
                }
            }
        }
        return replicaDataSourceSelectorList;
    }
}