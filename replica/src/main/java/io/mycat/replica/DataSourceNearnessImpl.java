package io.mycat.replica;

import io.mycat.DataSourceNearness;
import io.mycat.MetaClusterCurrent;
import io.mycat.TransactionSession;

import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 使集群选择数据源具有亲近性
 *
 * @junwen12221
 */
public class DataSourceNearnessImpl implements DataSourceNearness {
    ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();
    String loadBalanceStrategy;

    private TransactionSession transactionSession;

    public DataSourceNearnessImpl(TransactionSession transactionSession) {
        this.transactionSession = transactionSession;
    }

    public synchronized String getDataSourceByTargetName(final String targetName, boolean masterArg) {
        Objects.requireNonNull(targetName);
        boolean master = masterArg || transactionSession.isInTransaction();
        ReplicaSelectorManager selector = MetaClusterCurrent.wrapper(ReplicaSelectorManager.class);
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