package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.replica.heartbeat.DefaultHeartbeatFlow;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;
import io.mycat.replica.heartbeat.strategy.MySQLGaleraHeartBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLMasterSlaveBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;

public enum ReplicaHeartbeatRuntime {
  INSTANCE;
  final ConcurrentMap<String, PhysicsInstance> physicsInstanceMap = new ConcurrentHashMap<>();
  final ConcurrentMap<String, HeartbeatFlow> heartbeatDetectorMap = new ConcurrentHashMap<>();

  public void load(boolean force) {
    if (force || physicsInstanceMap.isEmpty()) {
      physicsInstanceMap.clear();
      for (ReplicaDataSourceSelector value : ReplicaSelectorRuntime.INSTCANE.map.values()) {
        for (PhysicsInstanceImpl physicsInstance : value.datasourceList) {
          physicsInstanceMap.put(physicsInstance.getName(), physicsInstance);
        }
      }
    }
  }

  public void heartbeat() {
    heartbeatDetectorMap
        .forEach((key, value) -> value.heartbeat());
  }

  public void register(ReplicaConfig replicaConfig, DatasourceConfig datasourceConfig,
      HeartbeatConfig heartbeatConfig,
      Consumer<HeartBeatStrategy> executer) {
    register(physicsInstanceMap.get(datasourceConfig.getName()),
        replicaConfig.getName(), datasourceConfig.getName(), datasourceConfig.getMaxRetryCount(),
        heartbeatConfig.getMinSwitchTimeInterval(), heartbeatConfig.getMinHeartbeatChecktime(),
        ReplicaSwitchType.valueOf(replicaConfig.getSwitchType()), replicaConfig.getSlaveThreshold(),
        ReplicaType.valueOf(replicaConfig.getRepType()), executer);
  }

  public void register(PhysicsInstance instance,
      String replica, String datasouceName, int maxRetry,
      long minSwitchTimeInterval, long heartbeatTimeout,
      ReplicaSwitchType switchType, long slaveThreshold, ReplicaType replicaType,
      Consumer<HeartBeatStrategy> executer) {
    Objects.requireNonNull(replicaType);
    Function<HeartbeatFlow, HeartBeatStrategy> strategyProvider;
    switch (replicaType) {
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
    register(instance, replica, datasouceName, maxRetry, minSwitchTimeInterval, heartbeatTimeout,
        switchType, slaveThreshold, strategyProvider, executer);
  }

  public void register(PhysicsInstance instance,
      String replica, String datasouceName, int maxRetry,
      long minSwitchTimeInterval, long heartbeatTimeout,
      ReplicaSwitchType switchType, long slaveThreshold,
      Function<HeartbeatFlow, HeartBeatStrategy> strategy,
      Consumer<HeartBeatStrategy> executer) {
    DefaultHeartbeatFlow heartbeatFlow = new DefaultHeartbeatFlow(instance, replica, datasouceName,
        maxRetry, minSwitchTimeInterval, heartbeatTimeout, switchType, slaveThreshold, strategy,
        executer);
    heartbeatDetectorMap.putIfAbsent(datasouceName, heartbeatFlow);
  }

  public void load() {
    load(false);
  }
}