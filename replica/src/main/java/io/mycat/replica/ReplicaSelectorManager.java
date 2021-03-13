package io.mycat.replica;

import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;

import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public interface ReplicaSelectorManager extends Closeable {


    String getDatasourceNameByReplicaName(String name, boolean master, String loadBalanceStrategy);

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

    Map<String, HeartbeatFlow>  getHeartbeatDetectorMap();
}
