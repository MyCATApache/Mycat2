package io.mycat.replica.heartbeat;

import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import java.util.function.Consumer;
import java.util.function.Function;

public class DefaultHeartbeatFlow extends HeartbeatFlow {

  private final String replicaName;
  private final String datasouceName;
  private final ReplicaSwitchType switchType;
  private final Consumer<HeartBeatStrategy> executer;
  private final Function<HeartbeatFlow, HeartBeatStrategy> strategyProvider;
  private HeartBeatStrategy strategy;

  public DefaultHeartbeatFlow(PhysicsInstance instance, String replica, String datasouceName,
      int maxRetry,
      long minSwitchTimeInterval, long heartbeatTimeout,
      ReplicaSwitchType switchType, long slaveThreshold,
      Function<HeartbeatFlow, HeartBeatStrategy> strategyProvider,
      Consumer<HeartBeatStrategy> executer) {
    super(instance, maxRetry, minSwitchTimeInterval, heartbeatTimeout, slaveThreshold);
    this.replicaName = replica;
    this.datasouceName = datasouceName;
    this.switchType = switchType;
    this.executer = executer;
    this.strategyProvider = strategyProvider;
  }

  @Override
  public void heartbeat() {
    HeartBeatStrategy strategy = strategyProvider.apply(this);
    this.strategy = strategy;
    executer.accept(strategy);
  }

  @Override
  public void sendDataSourceStatus(DatasourceStatus currentDatasourceStatus) {
    //状态不同进行状态的同步
    if (!this.dsStatus.equals(currentDatasourceStatus)) {
      //设置状态给 dataSource
      this.dsStatus = currentDatasourceStatus;
      LOGGER.error("{} heartStatus {}", datasouceName, dsStatus);
    }
    ReplicaSelectorRuntime.INSTCANE
        .updateInstanceStatus(replicaName, datasouceName, isAlive(instance.isMaster()),
            instance.asSelectRead());
    if (switchType.equals(ReplicaSwitchType.SWITCH)
        && instance.isMaster() && dsStatus.isError()
        && canSwitchDataSource()) {
      //replicat 进行选主
      if (ReplicaSelectorRuntime.INSTCANE.notifySwitchReplicaDataSource(replicaName)) {
        //updataSwitchTime
        this.hbStatus.setLastSwitchTime(System.currentTimeMillis());
      }
    }
  }

  @Override
  public void setTaskquitDetector() {
    if (strategyProvider != null) {
      strategy.setQuit(true);
    }
  }

  private boolean isAlive(boolean master) {
    if (master) {
      return this.dsStatus.isAlive();
    } else {
      return this.dsStatus.isAlive() && asSelectRead();
    }
  }

  private boolean asSelectRead() {
    return dsStatus.isAlive() && !dsStatus.isSlaveBehindMaster() && dsStatus.isDbSynStatusNormal();
  }

  private boolean canSwitchDataSource() {
    return this.hbStatus.getLastSwitchTime() + this.hbStatus.getMinSwitchTimeInterval() < System
        .currentTimeMillis();
  }
}