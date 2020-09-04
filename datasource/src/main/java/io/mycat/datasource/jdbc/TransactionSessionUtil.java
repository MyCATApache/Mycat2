package io.mycat.datasource.jdbc;

import io.mycat.MycatConnection;
import io.mycat.TransactionSession;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.replica.PhysicsInstanceImpl;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class TransactionSessionUtil {

    public static MycatConnection getConnectionByReplicaName(TransactionSession transactionSession, String replicaName, boolean update, String strategy) {
        return getDefaultConnection(replicaName, update, strategy, transactionSession);
    }

    public static MycatConnection getDefaultConnection(String replicaName, boolean update, String strategy, TransactionSession transactionSession) {
        LoadBalanceStrategy loadBalanceByBalanceName = PlugRuntime.INSTANCE.getLoadBalanceByBalanceName(strategy);
        PhysicsInstanceImpl datasource = ReplicaSelectorRuntime.INSTANCE.getDatasourceByReplicaName(replicaName, update, loadBalanceByBalanceName);
        String name;
        if (datasource == null) {
            name = replicaName;
        } else {
            name = datasource.getName();
        }
        return transactionSession.getConnection(Objects.requireNonNull(name));
    }

}