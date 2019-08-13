package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;
import io.mycat.replica.heartbeat.HeartbeatFlowImpl;
import io.mycat.replica.heartbeat.strategy.MySQLGaleraHeartBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLMasterSlaveBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public enum ReplicaHeartbeatRuntime {
  INSTANCE;
  final ConcurrentMap<String, PhysicsInstance> physicsInstanceMap = new ConcurrentHashMap<>();
  final ConcurrentMap<String, HeartbeatFlow> heartbeatDetectorMap = new ConcurrentHashMap<>();

  public void load(boolean force) {
    if (force || physicsInstanceMap.isEmpty()) {
      physicsInstanceMap.clear();
      for (ReplicaDataSourceSelector value : ReplicaRuntime.INSTCANE.map.values()) {
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
      BiConsumer<HeartbeatFlow, HeartBeatStrategy> executer) {
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
      BiConsumer<HeartbeatFlow, HeartBeatStrategy> executer) {
    Objects.requireNonNull(replicaType);
    HeartBeatStrategy strategy;
    switch (replicaType) {
      case MASTER_SLAVE:
        strategy = new MySQLMasterSlaveBeatStrategy();
        break;
      case GARELA_CLUSTER:
        strategy = new MySQLGaleraHeartBeatStrategy();
        break;
      case NONE:
      case SINGLE_NODE:
      default:
        strategy = new MySQLSingleHeartBeatStrategy();
        break;
    }
    register(instance, replica, datasouceName, maxRetry, minSwitchTimeInterval, heartbeatTimeout,
        switchType, slaveThreshold, strategy, executer);
  }

  public void register(PhysicsInstance instance,
      String replica, String datasouceName, int maxRetry,
      long minSwitchTimeInterval, long heartbeatTimeout,
      ReplicaSwitchType switchType, long slaveThreshold, HeartBeatStrategy strategy,
      BiConsumer<HeartbeatFlow, HeartBeatStrategy> executer) {
    HeartbeatFlowImpl heartbeatFlow = new HeartbeatFlowImpl(instance, replica, datasouceName,
        maxRetry, minSwitchTimeInterval, heartbeatTimeout, switchType, slaveThreshold, strategy,
        executer);
    heartbeatDetectorMap.putIfAbsent(datasouceName, heartbeatFlow);
  }
}