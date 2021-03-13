package io.mycat.replica;

import io.mycat.plug.loadBalance.LoadBalanceInfo;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.plug.loadBalance.SessionCounter;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

public interface ReplicaSelector extends LoadBalanceInfo, Closeable {
    String getDbType();

    List<PhysicsInstance> getWriteDataSource();

    String getName();

    void close();

    Map<String,PhysicsInstance> getRawDataSourceMap();

    PhysicsInstance register(String dataSourceName, InstanceType type, int weight, SessionCounter sessionCounter);

    void unregister(String datasourceName);

    void switchDataSourceIfNeed();

    ReplicaSwitchType getSwitchType();

    int maxRequestCount();

    BalanceType getBalanceType();

    List<PhysicsInstance> getReadDataSource();

    LoadBalanceStrategy getDefaultWriteLoadBalanceStrategy();

    LoadBalanceStrategy getDefaultReadLoadBalanceStrategy();

    List<PhysicsInstance> getDataSourceByLoadBalacneType();


    public void updateInstanceStatus(String dataSource, boolean alive, boolean selectAsRead);

    ReplicaType getType();
}
