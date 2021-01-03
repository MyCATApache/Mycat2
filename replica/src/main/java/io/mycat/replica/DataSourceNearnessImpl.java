package io.mycat.replica;

import io.mycat.DataSourceNearness;
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

    private TransactionSession transactionSession;

    public DataSourceNearnessImpl(TransactionSession transactionSession) {
        this.transactionSession = transactionSession;
    }

    public String getDataSourceByTargetName(final String targetName, boolean masterArg) {
        Objects.requireNonNull(targetName);
        boolean master = masterArg || transactionSession.isInTransaction();
        ReplicaSelectorRuntime selector = MetaClusterCurrent.wrapper(ReplicaSelectorRuntime.class);
        boolean replicaMode = selector.isReplicaName(targetName);
        String datasource;
        if (replicaMode) {
            datasource = map.computeIfAbsent(targetName, (s) -> {
                String datasourceNameByReplicaName = selector.getDatasourceNameByReplicaName(targetName, master, loadBalanceStrategy);
                return Objects.requireNonNull(datasourceNameByReplicaName);
            });
        } else {
            datasource = targetName;
        }
        return Objects.requireNonNull(datasource);
    }

    @Override
    public String getDataSourceByTargetName(String targetName) {
        return getDataSourceByTargetName(targetName, false);
    }

    public void setLoadBalanceStrategy(String loadBalanceStrategy) {
        this.loadBalanceStrategy = loadBalanceStrategy;
    }


    public void clear() {
        map.clear();
        loadBalanceStrategy = null;
    }
}