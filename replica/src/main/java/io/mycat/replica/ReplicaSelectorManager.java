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
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface ReplicaSelectorManager extends Closeable {

    default String getDatasourceNameByReplicaName(String name, boolean master,  String loadBalanceStrategy){
        return getDatasourceNameByReplicaName(name,master,ReplicaBalanceType.NONE,loadBalanceStrategy);
    }

    String getDatasourceNameByReplicaName(String name, boolean master, ReplicaBalanceType replicaBalanceType, String loadBalanceStrategy);

    void putHeartFlow(String replicaName, String datasourceName, Consumer<HeartBeatStrategy> executer);

    String getDbTypeByTargetName(String name);

    String getDatasourceNameByRandom();

    boolean isDatasource(String targetName);

    boolean isReplicaName(String targetName);

    Map<String, List<String>> getState();

    PhysicsInstance getPhysicsInstanceByName(String name);

    Map<String, ReplicaSelector> getReplicaMap();

    Collection<PhysicsInstance> getPhysicsInstances();

    List<String> getRepliaNameListByInstanceName(String name);

    Map<String, HeartbeatFlow> getHeartbeatDetectorMap();
}
