package io.mycat.replica;

import io.mycat.DataSourceNearness;
import io.mycat.MetaCluster;
import io.mycat.MetaClusterCurrent;
import io.mycat.TransactionSession;

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
    private TransactionSession transactionSession;

    public DataSourceNearnessImpl(TransactionSession transactionSession) {
        this.transactionSession = transactionSession;
    }

    public String getDataSourceByTargetName(final String targetName) {
        Objects.requireNonNull(targetName);
        ReplicaSelectorRuntime instance = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
        if (replicaMode == null) {
            replicaMode = instance.isReplicaName(targetName);
        }
        String res;
        if (replicaMode) {
            res  =  map.computeIfAbsent(targetName, (s) -> {
                String datasourceNameByReplicaName = instance.getDatasourceNameByReplicaName(targetName, transactionSession.isInTransaction(), loadBalanceStrategy);
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


    public void clear(){
        map.clear();
        replicaMode = null;
        loadBalanceStrategy = null;
    }
}