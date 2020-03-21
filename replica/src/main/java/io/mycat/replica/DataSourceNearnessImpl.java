package io.mycat.replica;

import io.mycat.DataSourceNearness;

import java.util.HashMap;
import java.util.Objects;

/**
 * 使集群选择数据源具有亲近性
 *
 * @junwen12221
 */
public class DataSourceNearnessImpl implements DataSourceNearness {
    HashMap<String, String> map = new HashMap<>();
    String loadBalanceStrategy;
    Boolean replicaMode;
    boolean update;

    public DataSourceNearnessImpl() {
    }

    public String getDataSourceByTargetName(final String targetName) {
        Objects.requireNonNull(targetName);
        ReplicaSelectorRuntime instance = ReplicaSelectorRuntime.INSTANCE;
        if (replicaMode == null) {
            replicaMode = instance.isReplicaName(targetName);
        }
        String res;
        if (replicaMode) {
            res  =  map.computeIfAbsent(targetName, (s) -> {
                String datasourceNameByReplicaName = instance.getDatasourceNameByReplicaName(targetName, false, loadBalanceStrategy);
                return Objects.requireNonNull(datasourceNameByReplicaName);
            });
        }else {
            res = targetName;
        }
        return Objects.requireNonNull( res);
    }

    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }

    public void setUpdate(boolean update) {
        this.update = update;
    }

    public void clear(){
        map.clear();
        replicaMode = null;
        loadBalanceStrategy = null;
    }
}