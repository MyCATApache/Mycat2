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
