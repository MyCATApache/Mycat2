package io.mycat.replica.heartbeat;

import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaRuntime;
import io.mycat.replica.ReplicaSwitchType;
import java.util.function.BiConsumer;

public class HeartbeatFlowImpl extends HeartbeatFlow {

  final HeartBeatStrategy strategy;
  private final String replicaName;
  private final String datasouceName;
  private final ReplicaSwitchType switchType;
  private final BiConsumer<HeartbeatFlow, HeartBeatStrategy> commonSQLCallbacbProvider;
  protected volatile boolean isQuit = false;

  public HeartbeatFlowImpl(PhysicsInstance instance, String replica, String datasouceName,
      int maxRetry,
      long minSwitchTimeInterval, long heartbeatTimeout,
      ReplicaSwitchType switchType, long slaveThreshold, HeartBeatStrategy strategy,
      BiConsumer<HeartbeatFlow, HeartBeatStrategy> executer) {
    super(instance, maxRetry, minSwitchTimeInterval, heartbeatTimeout, slaveThreshold);
    this.replicaName = replica;
    this.datasouceName = datasouceName;
    this.switchType = switchType;
    this.strategy = strategy;
    this.commonSQLCallbacbProvider = executer;
  }

  @Override
  public void heartbeat() {
    commonSQLCallbacbProvider.accept(this, strategy);
  }

  @Override
  public void sendDataSourceStatus(DatasourceStatus currentDatasourceStatus) {
    //状态不同进行状态的同步
    if (!this.dsStatus.equals(currentDatasourceStatus)) {
      //设置状态给 dataSource
      this.dsStatus = currentDatasourceStatus;
      LOGGER.error("{} heartStatus {}", datasouceName, dsStatus);
    }
    ReplicaRuntime.INSTCANE
        .updateInstanceStatus(replicaName, datasouceName, isAlive(instance.isMaster()),
            instance.asSelectRead());
    if (switchType.equals(ReplicaSwitchType.SWITCH)
        && instance.isMaster() && dsStatus.isError()
        && canSwitchDataSource()) {
      //replicat 进行选主
      if (ReplicaRuntime.INSTCANE.notifySwitchReplicaDataSource(replicaName)) {
        //updataSwitchTime
        this.hbStatus.setLastSwitchTime(System.currentTimeMillis());
      }
    }
  }

  @Override
  public void quitDetector() {
    this.isQuit = true;
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