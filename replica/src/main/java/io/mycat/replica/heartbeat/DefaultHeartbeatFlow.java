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
package io.mycat.replica.heartbeat;

import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelector;
import io.mycat.replica.ReplicaSwitchType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
public class DefaultHeartbeatFlow extends HeartbeatFlow {
    private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatFlow.class);
    private final String datasouceName;
    private final ReplicaSwitchType switchType;
    private final Consumer<HeartBeatStrategy> executer;
    private final Function<HeartbeatFlow, HeartBeatStrategy> strategyProvider;
    private volatile HeartBeatStrategy strategy;
    private final ReplicaSelector replicaSelector;

    public DefaultHeartbeatFlow(ReplicaSelector replicaSelector, PhysicsInstance instance, String datasouceName,
                                int maxRetry,
                                long minSwitchTimeInterval, long heartbeatTimeout,
                                ReplicaSwitchType switchType, double slaveThreshold,
                                Function<HeartbeatFlow, HeartBeatStrategy> strategyProvider,
                                Consumer<HeartBeatStrategy> executer) {
        super(instance, maxRetry, minSwitchTimeInterval, heartbeatTimeout, slaveThreshold);
        this.replicaSelector = replicaSelector;
        this.datasouceName = datasouceName;
        this.switchType = switchType;
        this.executer = executer;
        this.strategyProvider = strategyProvider;
    }

    @Override
    public void heartbeat() {
        updateLastSendQryTime();
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
            LOGGER.info("{} heartStatus {}", datasouceName, dsStatus);
        }
        switch (replicaSelector.getType()) {
            case SINGLE_NODE:
            case MASTER_SLAVE:
            case GARELA_CLUSTER:
            case NONE:
                replicaSelector
                        .updateInstanceStatus(datasouceName, currentDatasourceStatus.isAlive(),
                                !currentDatasourceStatus.isSlaveBehindMaster(), currentDatasourceStatus.isMaster());
                if (switchType.equals(ReplicaSwitchType.SWITCH) && (dsStatus.isError() ||
                        currentDatasourceStatus.isMaster() != instance.isMaster())
                        && canSwitchDataSource()) {
                    //replicat 进行选主
                    replicaSelector.notifySwitchReplicaDataSource();
                    //updataSwitchTime
                    this.hbStatus.setLastSwitchTime(System.currentTimeMillis());
                }
                break;
            case MHA:
            case MGR:
                if (currentDatasourceStatus.isMaster()){
                    replicaSelector.addWriteDataSource(datasouceName);
                    replicaSelector.removeReadDataSource(datasouceName);
                }else {
                    replicaSelector.addReadDataSource(datasouceName);
                    replicaSelector.removeWriteDataSource(datasouceName);
                }
                replicaSelector
                        .updateInstanceStatus(datasouceName, currentDatasourceStatus.isAlive(),
                                !currentDatasourceStatus.isSlaveBehindMaster(), currentDatasourceStatus.isMaster());
                if (switchType.equals(ReplicaSwitchType.SWITCH) && (dsStatus.isError() ||
                        currentDatasourceStatus.isMaster() != instance.isMaster())
                        && canSwitchDataSource()) {
                    //replicat 进行选主
                    replicaSelector.notifySwitchReplicaDataSource();
                    //updataSwitchTime
                    this.hbStatus.setLastSwitchTime(System.currentTimeMillis());
                }
                break;
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