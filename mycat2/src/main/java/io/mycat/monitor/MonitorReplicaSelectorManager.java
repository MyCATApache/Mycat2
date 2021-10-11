package io.mycat.monitor;

import io.mycat.ReplicaBalanceType;
import io.mycat.config.ClusterConfig;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelector;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.replica.heartbeat.HeartBeatStrategy;
import io.mycat.replica.heartbeat.HeartbeatFlow;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class MonitorReplicaSelectorManager implements ReplicaSelectorManager {

    final ReplicaSelectorManager replicaSelectorManager;

    public MonitorReplicaSelectorManager(ReplicaSelectorManager replicaSelectorManager) {
        this.replicaSelectorManager = replicaSelectorManager;
    }

    @Override
    public String getDatasourceNameByReplicaName(String name, boolean master, ReplicaBalanceType replicaBalanceType, String loadBalanceStrategy) {
        String dsName = replicaSelectorManager.getDatasourceNameByReplicaName(name, master, replicaBalanceType, loadBalanceStrategy);
        if (dsName.equals(name)) {
            return dsName;
        }
        RWEntry.stat(name,master);
        return dsName;
    }

    @Override
    public void putHeartFlow(String replicaName, String datasourceName, Consumer<HeartBeatStrategy> executer) {
        replicaSelectorManager.putHeartFlow(replicaName,datasourceName,executer);
    }

    @Override
    public String getDbTypeByTargetName(String name) {
        return replicaSelectorManager.getDbTypeByTargetName(name);
    }

    @Override
    public String getDatasourceNameByRandom() {
        return replicaSelectorManager.getDatasourceNameByRandom();
    }

    @Override
    public boolean isDatasource(String targetName) {
        return replicaSelectorManager.isDatasource(targetName);
    }

    @Override
    public boolean isReplicaName(String targetName) {
        return replicaSelectorManager.isReplicaName(targetName);
    }

    @Override
    public Map<String, List<String>> getState() {
        return replicaSelectorManager.getState();
    }

    @Override
    public PhysicsInstance getPhysicsInstanceByName(String name) {
        return replicaSelectorManager.getPhysicsInstanceByName(name);
    }

    @Override
    public Map<String, ReplicaSelector> getReplicaMap() {
        return replicaSelectorManager.getReplicaMap();
    }

    @Override
    public Collection<PhysicsInstance> getPhysicsInstances() {
        return replicaSelectorManager.getPhysicsInstances();
    }

    @Override
    public List<String> getRepliaNameListByInstanceName(String name) {
        return replicaSelectorManager.getRepliaNameListByInstanceName(name);
    }

    @Override
    public Map<String, HeartbeatFlow> getHeartbeatDetectorMap() {
        return replicaSelectorManager.getHeartbeatDetectorMap();
    }

    @Override
    public List<ClusterConfig> getConfig() {
        return replicaSelectorManager.getConfig();
    }

    @Override
    public void close() throws IOException {
        replicaSelectorManager.close();
    }
}
