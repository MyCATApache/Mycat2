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
package io.mycat.replica.heartbeat;

import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
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