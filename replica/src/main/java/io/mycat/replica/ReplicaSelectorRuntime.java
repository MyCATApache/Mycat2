/**
 * Copyright (C) <2021>  <chen junwen>
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

import io.mycat.ReplicaBalanceType;
import io.mycat.config.*;
import io.mycat.plug.loadBalance.LoadBalanceElement;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.loadBalance.SessionCounter;
import io.mycat.replica.heartbeat.DefaultHeartbeatFlow;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;
import io.mycat.replica.heartbeat.strategy.*;
import io.mycat.util.CollectionUtil;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.mycat.replica.InstanceType.READ;

/**
 * @author : chenjunwen date Date : 2019年05月15日 21:34
 */
public class ReplicaSelectorRuntime implements ReplicaSelectorManager {
    private final ConcurrentMap<String, ReplicaSelector> replicaMap = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, PhysicsInstance> physicsInstanceMap = new ConcurrentHashMap<>();
    ////////////////////////////////////////heartbeat///////////////////////////////////////////////////////////////////
    private final ConcurrentMap<String, HeartbeatFlow> heartbeatDetectorMap = new ConcurrentHashMap<>();
    private final Map<String, DatasourceConfig> datasources;
    private final LoadBalanceManager loadBalanceManager;
    private final List<ClusterConfig> replicaConfigList;
    private final SessionCounterProvider sessionCounterProvider;
    private final ScheduleProvider scheduleProvider;

    private static final Logger LOGGER = LoggerFactory.getLogger(ReplicaSelectorRuntime.class);


    public static ReplicaSelectorRuntime create(List<ClusterConfig> clusters,
                                                Map<String, DatasourceConfig> datasources,
                                                LoadBalanceManager loadBalanceManager,
                                                SessionCounterProvider sessionCounterProvider,
                                                ScheduleProvider scheduleProvider) {
        return new ReplicaSelectorRuntime(clusters, datasources, loadBalanceManager, sessionCounterProvider, scheduleProvider);
    }

    public ReplicaSelectorRuntime(List<ClusterConfig> clusters,
                                  Map<String, DatasourceConfig> datasources,
                                  LoadBalanceManager loadBalanceManager,
                                  SessionCounterProvider sessionCounterProvider,
                                  ScheduleProvider scheduleProvider) {
        this.datasources = datasources;
        this.loadBalanceManager = loadBalanceManager;

        this.replicaConfigList = clusters;
        this.sessionCounterProvider = sessionCounterProvider;
        this.scheduleProvider = scheduleProvider;

        ////////////////////////////////////check/////////////////////////////////////////////////
        Objects.requireNonNull(replicaConfigList, "replica config can not be empty");
        ////////////////////////////////////check/////////////////////////////////////////////////

        for (ClusterConfig replicaConfig : replicaConfigList) {
            addCluster(this.datasources, replicaConfig, sessionCounterProvider);
        }


        //移除不必要的配置

        //新配置中的集群名字
        Set<String> clusterNames = clusters.stream().map(i -> i.getName()).collect(Collectors.toSet());
        new HashSet<>(replicaMap.keySet()).stream().filter(name -> !clusterNames.contains(name)).forEach(name -> replicaMap.remove(name));

        //新配置中的数据源名字
        Set<String> datasourceNames = datasources.keySet();
        new HashSet<>(physicsInstanceMap.keySet()).stream().filter(name -> !datasourceNames.contains(name)).forEach(name -> physicsInstanceMap.remove(name));

        List<PhysicsInstance> collect = replicaMap.values().stream().flatMap(i -> i.getRawDataSourceMap().values().stream()).collect(Collectors.toList());
        collect.forEach(c -> {
            c.notifyChangeSelectRead(true);
            c.notifyChangeAlive(true);
        });

        Map<String, PhysicsInstance> newphysicsInstanceMap = new HashMap<>();
        for (ReplicaSelector i : replicaMap.values()) {
            for (PhysicsInstance k : i.getRawDataSourceMap().values()) {
                newphysicsInstanceMap.put(k.getName(), k);
            }
        }
        CollectionUtil.safeUpdateByUpdate(this.physicsInstanceMap, newphysicsInstanceMap);
    }

    public List<ClusterConfig> getConfig() {
        return replicaConfigList;
    }
/////////////////////////////////////////public manager/////////////////////////////////////////////////////////////


    public void removeCluster(String name) {
        replicaMap.remove(name);
    }


    public void removeDatasource(String clusterName, String datasourceName) {
        ReplicaSelector selector = replicaMap.get(clusterName);
        if (selector != null) {
            selector.unregister(datasourceName);
        }
    }
    public String getDatasourceNameByReplicaName(String replicaName, boolean master, ReplicaBalanceType replicaBalanceType, String loadBalanceStrategy) {
        BiFunction<LoadBalanceStrategy, ReplicaSelector, PhysicsInstance> function;
        if (master || replicaBalanceType == ReplicaBalanceType.MASTER) {
            function = this::getWriteDatasource;
        } else if (replicaBalanceType == ReplicaBalanceType.SLAVE){
            function = this::getReadDatasource;
        }else {
            function = this::getDatasource;
        }
        ReplicaSelector replicaDataSourceSelector = replicaMap.get(Objects.requireNonNull(replicaName));
        if (replicaDataSourceSelector == null) {
            return replicaName;
        }
        LoadBalanceStrategy loadBalanceByBalance = null;
        if (loadBalanceStrategy != null) {
            loadBalanceByBalance = loadBalanceManager.getLoadBalanceByBalanceName(loadBalanceStrategy);
        }//传null集群配置的负载均衡生效
        if (replicaDataSourceSelector.getWriteDataSourceByReplicaType().isEmpty()
                &&
                replicaDataSourceSelector.getReadDataSourceByReplica().isEmpty()) {
            LOGGER.error("No data sources are available {}", replicaName);
            if (replicaDataSourceSelector.getRawDataSourceMap().size() == 1) {
                return replicaDataSourceSelector.getRawDataSourceMap().keySet().stream().iterator().next();
            }
        }
        try {
            PhysicsInstance physicsInstance = function.apply(loadBalanceByBalance, replicaDataSourceSelector);
            if (physicsInstance == null) {
                return replicaName;
            }
            return physicsInstance.getName();
        } catch (Throwable throwable) {
            LOGGER.error("No data sources are available {}", replicaName, throwable);
        }
        if (!master) {
            LOGGER.error("need abnormal cluster check {}", replicaName);
            return replicaDataSourceSelector.getRawDataSourceMap().values().iterator().next().getName();
        }
        return replicaName;
    }


    public void updateInstanceStatus(String replicaName, String dataSource, boolean alive,
                                     boolean selectAsRead) {
        ReplicaSelector selector = replicaMap.get(replicaName);
        if (selector != null) {
            PhysicsInstance physicsInstance = selector.getRawDataSourceMap().get(dataSource);
            if (physicsInstance != null) {
                physicsInstance.notifyChangeAlive(alive);
                physicsInstance.notifyChangeSelectRead(selectAsRead);
            }
        }
    }

//    public void updateInstanceStatus(String dataSource, boolean alive,
//                                     boolean selectAsRead) {
//        replicaMap.values().stream().flatMap(i -> i.datasourceMap.values().stream()).filter(i -> i.getName().equals(dataSource)).findFirst().ifPresent(physicsInstance -> {
//            physicsInstance.notifyChangeAlive(alive);
//            physicsInstance.notifyChangeSelectRead(selectAsRead);
//        });
//    }


///////////////////////////////////////private manager//////////////////////////////////////////////////////////////////////////

    private PhysicsInstance registerDatasource(boolean master, ReplicaSelector selector,
                                               DatasourceConfig datasourceConfig,
                                               SessionCounter sessionCounter) {
        Objects.requireNonNull(selector);
        Objects.requireNonNull(datasourceConfig);
        InstanceType instanceType;
        if (datasourceConfig.getInstanceType() != null) {
            instanceType = InstanceType.valueOf(datasourceConfig.getInstanceType());
            if (!master) {
                switch (instanceType) {
                    case READ:
                        break;
                    case WRITE:
                        throw new IllegalStateException("Unexpected value: " + datasourceConfig + " is READ ONLY");
                    case READ_WRITE:
                        instanceType = READ;
                        break;
                    default:

                }
            }
        } else {
            instanceType =  master ? InstanceType.READ_WRITE : READ;
        }
        return registerDatasource(selector.getName(), datasourceConfig.getName(), instanceType,
                datasourceConfig.getWeight(), sessionCounter);
    }

    private void addCluster(Map<String, DatasourceConfig> datasourceConfigMap, ClusterConfig replicaConfig, SessionCounterProvider sessionCounterProvider) {
        String name = replicaConfig.getName();
        ReplicaType replicaType = ReplicaType.valueOf(replicaConfig.getClusterType());
        BalanceType balanceType = BalanceType.valueOf(replicaConfig.getReadBalanceType());
        ReplicaSwitchType switchType = ReplicaSwitchType.valueOf(replicaConfig.getSwitchType());

        LoadBalanceStrategy readLB = loadBalanceManager.getLoadBalanceByBalanceName(replicaConfig.getReadBalanceName());
        LoadBalanceStrategy writeLB = loadBalanceManager.getLoadBalanceByBalanceName(replicaConfig.getWriteBalanceName());
        int maxRequestCount = replicaConfig.getMaxCon() == null ? Integer.MAX_VALUE : replicaConfig.getMaxCon();

        assert datasourceConfigMap.size() != 0;

        String dbType = datasourceConfigMap.values().stream().findFirst().map(i->i.getDbType()).orElse("mysql");

        ReplicaSelector selector = registerCluster(
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
            registerDatasource(datasourceConfigMap, selector, replicaConfig.getMasters(), true, sessionCounterProvider);
        }
        if (replicaConfig.getReplicas() != null) {
            registerDatasource(datasourceConfigMap, selector, replicaConfig.getReplicas(), false, sessionCounterProvider);
        }
    }

    private void registerDatasource(Map<String, DatasourceConfig> datasourceConfigMap,
                                    ReplicaSelector selector,
                                    List<String> datasourceNameList,
                                    boolean master,
                                    SessionCounterProvider sessionCounterProvider) {
        if (datasourceNameList == null) {
            datasourceNameList = Collections.emptyList();
        }
        for (String datasourceName : datasourceNameList) {
            DatasourceConfig datasource = datasourceConfigMap.get(datasourceName);
            registerDatasource(master, selector, datasource, () -> sessionCounterProvider.getSessionCounter(datasourceName));
        }
    }

    private PhysicsInstance registerDatasource(String replicaName, String dataSourceName,
                                               InstanceType type,
                                               int weight, SessionCounter sessionCounter) {
        ReplicaSelector sourceSelector = replicaMap.get(replicaName);
        Objects.requireNonNull(sourceSelector);
        return sourceSelector.register(dataSourceName, type, weight, sessionCounter);
    }


    private ReplicaSelector registerCluster(String replicaName,
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
        ArrayList<ReplicaSelector> values = new ArrayList<>(replicaMap.values());
        int i = ThreadLocalRandom.current().nextInt(0, values.size());
        ReplicaSelector replicaDataSourceSelector = values.get(i);
        String name = replicaDataSourceSelector.getName();
        return getDatasourceNameByReplicaName(name, false, null);
    }

    public String getDatasourceNameByReplicaName(String replicaName, boolean master, String loadBalanceStrategy) {
        BiFunction<LoadBalanceStrategy, ReplicaSelector, PhysicsInstance> function =
                master ? this::getWriteDatasource : this::getDatasource;
        ReplicaSelector replicaDataSourceSelector = replicaMap.get(Objects.requireNonNull(replicaName));
        if (replicaDataSourceSelector == null) {
            return replicaName;
        }
        LoadBalanceStrategy loadBalanceByBalance = null;
        if (loadBalanceStrategy != null) {
            loadBalanceByBalance = loadBalanceManager.getLoadBalanceByBalanceName(loadBalanceStrategy);
        }//传null集群配置的负载均衡生效
        if (replicaDataSourceSelector.getWriteDataSourceByReplicaType().isEmpty()
                &&
                replicaDataSourceSelector.getReadDataSourceByReplica().isEmpty()) {
            LOGGER.error("No data sources are available {}", replicaName);
            if (replicaDataSourceSelector.getRawDataSourceMap().size() == 1) {
                return replicaDataSourceSelector.getRawDataSourceMap().keySet().stream().iterator().next();
            }
        }
        try {
            PhysicsInstance physicsInstance = function.apply(loadBalanceByBalance, replicaDataSourceSelector);
            if (physicsInstance == null) {
                return replicaName;
            }
            return physicsInstance.getName();
        } catch (Throwable throwable) {
            LOGGER.error("No data sources are available {}", replicaName, throwable);
        }
        if (!master) {
            LOGGER.error("need abnormal cluster check {}", replicaName);
            return replicaDataSourceSelector.getRawDataSourceMap().values().iterator().next().getName();
        }
        return replicaName;
    }

    public PhysicsInstance getWriteDatasourceByReplicaName(String replicaName,
                                                           LoadBalanceStrategy balanceStrategy) {
        ReplicaSelector selector = replicaMap.get(replicaName);
        if (selector == null) {
            return null;
        }
        return getDatasource(balanceStrategy, selector,
                selector.getDefaultWriteLoadBalanceStrategy(), selector.getWriteDataSourceByReplicaType());
    }

    public PhysicsInstance getWriteDatasource(LoadBalanceStrategy balanceStrategy,
                                              ReplicaSelector selector) {
        LoadBalanceStrategy defaultWriteLoadBalanceStrategy = selector.getDefaultWriteLoadBalanceStrategy();
        List<PhysicsInstance> writeDataSource = selector.getWriteDataSourceByReplicaType();
        return getDatasource(balanceStrategy, selector, defaultWriteLoadBalanceStrategy,
                writeDataSource);
    }

    public PhysicsInstance getDatasource(LoadBalanceStrategy balanceStrategy,
                                         ReplicaSelector selector) {
        LoadBalanceStrategy defaultReadLoadBalanceStrategy = selector.getDefaultReadLoadBalanceStrategy();
        List<PhysicsInstance> dataSourceByLoadBalacneType = selector.getDataSourceByLoadBalacneType();
        return getDatasource(balanceStrategy, selector, defaultReadLoadBalanceStrategy,
                dataSourceByLoadBalacneType);
    }

    public PhysicsInstance getDatasource(LoadBalanceStrategy balanceStrategy,
                                         ReplicaSelector selector, LoadBalanceStrategy defaultWriteLoadBalanceStrategy,
                                         List element) {
        Objects.requireNonNull(element);
        Objects.requireNonNull(selector);
        if (balanceStrategy == null) {
            balanceStrategy = defaultWriteLoadBalanceStrategy;
        }
        LoadBalanceElement select = balanceStrategy.select(selector, element);
        Objects.requireNonNull(select, "No data source available");
        return (PhysicsInstance) select;
    }

    public PhysicsInstance getDatasourceByReplicaName(String replicaName, boolean master, LoadBalanceStrategy balanceStrategy) {
        ReplicaSelector selector = replicaMap.get(replicaName);
        if (selector == null) {
            return null;
        }
        if (master) {
            return getWriteDatasourceByReplicaName(replicaName, balanceStrategy);
        } else {
            return getDatasource(balanceStrategy, selector,
                    selector.getDefaultReadLoadBalanceStrategy(), selector.getDataSourceByLoadBalacneType());
        }
    }


    public synchronized void putHeartFlow(String replicaName, String datasourceName, Consumer<HeartBeatStrategy> executer) {
        String name = replicaName + "." + datasourceName;
        this.replicaConfigList.stream().filter(i -> replicaName.equals(i.getName())).findFirst().ifPresent(c -> {
            HeartbeatConfig heartbeat = c.getHeartbeat();
            if (heartbeat != null) {
                ReplicaSelector selector = replicaMap.get(replicaName);
                if (selector != null) {
                    PhysicsInstance physicsInstance = selector.getRawDataSourceMap().get(datasourceName);
                    DefaultHeartbeatFlow heartbeatFlow = new DefaultHeartbeatFlow(selector, physicsInstance, datasourceName,
                            heartbeat.getMaxRetryCount(), heartbeat.getMinSwitchTimeInterval(), heartbeat.getHeartbeatTimeout(),
                            ReplicaSwitchType.valueOf(c.getSwitchType()),
                            heartbeat.getSlaveThreshold(), getStrategyByReplicaType(c.getClusterType()),
                            executer);

                    heartbeatDetectorMap.put(name, heartbeatFlow);
                    //马上进行心跳,获取集群状态
                    heartbeatFlow.heartbeat();
                }
            }
        });
    }

    public void removeHeartFlow(String replicaName, String datasourceName) {
        HeartbeatFlow remove = heartbeatDetectorMap.remove(replicaName + "." + datasourceName);
    }

    public void clearHeartbeatDetector(){
        heartbeatDetectorMap.clear();
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
            case MGR:
                strategyProvider = MGRHeartBeatStrategy::new;
                break;
            case MHA:
                strategyProvider = MHAHeartBeatStrategy::new;
                break;
            case NONE:
            case SINGLE_NODE:
                strategyProvider = MySQLSingleHeartBeatStrategy::new;
                break;
            default:
               throw new UnsupportedOperationException("unsupported:"+replicaType);
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
        return this.physicsInstanceMap.containsKey(targetName) || this.datasources.containsKey(targetName);
    }


    public Map<String, ReplicaSelector> getReplicaMap() {
        return Collections.unmodifiableMap(replicaMap);
    }

    @Override
    public Collection<PhysicsInstance> getPhysicsInstances() {
        return this.physicsInstanceMap.values();
    }


    public Map<String, PhysicsInstance> getPhysicsInstanceMap() {
        return Collections.unmodifiableMap(physicsInstanceMap);
    }

    public Map<String, HeartbeatFlow> getHeartbeatDetectorMap() {
        return Collections.unmodifiableMap(heartbeatDetectorMap);
    }

    public List<String> getRepliaNameListByInstanceName(String name) {
        List<String> replicaDataSourceSelectorList = new ArrayList<>();
        for (ReplicaSelector replicaDataSourceSelector : this.getReplicaMap().values()) {
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
        for (ReplicaSelector i : replicaMap.values()) {
            i.close();
        }
    }

    public String getDbTypeByTargetName(String name) {
        ReplicaSelector replicaDataSourceSelector = this.getReplicaMap().get(name);
        if (replicaDataSourceSelector != null) {
            return replicaDataSourceSelector.getDbType();
        }
        DatasourceConfig datasourceConfig = datasources.get(name);
        Objects.requireNonNull(datasourceConfig, "unknown dbType of :" + name);
        return datasourceConfig.getDbType();
    }

    public Map<String, List<String>> getState() {
        Map<String, List<String>> map = new HashMap<>();
        for (ReplicaSelector value : replicaMap.values()) {
            ArrayList<PhysicsInstance> objects = new ArrayList<>(value.getWriteDataSourceByReplicaType());
            map.put(value.getName(), objects.stream().map(i -> i.getName()).collect(Collectors.toList()));
        }
        return map;

    }

    public SessionCounterProvider getSessionCounterProvider() {
        return sessionCounterProvider;
    }

    public ScheduleProvider getScheduleProvider() {
        return scheduleProvider;
    }

    public PhysicsInstance getReadDatasource(LoadBalanceStrategy balanceStrategy,
                                             ReplicaSelector selector) {
        LoadBalanceStrategy defaultWriteLoadBalanceStrategy = selector.getDefaultWriteLoadBalanceStrategy();
        List<PhysicsInstance> readDataSourceByReplica = selector.getReadDataSourceByReplica().stream().filter(i->i.getType() == READ).collect(Collectors.toList());
        if (readDataSourceByReplica.isEmpty()){
            readDataSourceByReplica = selector.getWriteDataSourceByReplicaType();
        }
        return getDatasource(balanceStrategy, selector, defaultWriteLoadBalanceStrategy,
                readDataSourceByReplica);
    }
}