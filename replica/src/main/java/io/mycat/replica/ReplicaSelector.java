/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat.replica;

import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.loadBalance.SessionCounter;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public interface ReplicaSelector extends LoadBalanceInfo, Closeable {
    String getDbType();

    List<PhysicsInstance> getWriteDataSourceByReplicaType();

    String getName();

    void close();

    Map<String,PhysicsInstance> getRawDataSourceMap();

    PhysicsInstance register(String dataSourceName, InstanceType type, int weight, SessionCounter sessionCounter);

    void unregister(String datasourceName);

    ReplicaSwitchType getSwitchType();

    int maxRequestCount();

    BalanceType getBalanceType();

    List<PhysicsInstance> getReadDataSourceByReplica();

    LoadBalanceStrategy getDefaultWriteLoadBalanceStrategy();

    LoadBalanceStrategy getDefaultReadLoadBalanceStrategy();

    List<PhysicsInstance> getDataSourceByLoadBalacneType();


    public void updateInstanceStatus(String dataSource, boolean alive, boolean selectAsRead,boolean master);

    ReplicaType getType();

    void notifySwitchReplicaDataSource();


    public void addWriteDataSource(String dataSource);


    public void removeWriteDataSource(String dataSource);


    public void addReadDataSource(String dataSource);

    public void removeReadDataSource(String dataSource);
}
