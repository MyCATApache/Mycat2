package io.mycat.runtime;

import io.mycat.TransactionSession;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.replica.PhysicsInstanceImpl;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.resultset.MysqlSingleDataNodeResultSetResponse;
import io.mycat.resultset.TextResultSetResponse;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class TransactionSessionUtil {

    public static MycatResultSetResponse executeQuery(TransactionSession transactionSession, String replicaName, String sql, boolean update, String strategy) {
        DefaultConnection connection = getConnectionByReplicaName(transactionSession,replicaName, update, strategy);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static MycatResultSetResponse executeQuery(TransactionSession transactionSession, String datasource, String sql) {
        Objects.requireNonNull(datasource);
        DefaultConnection connection = transactionSession.getConnection(datasource);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static DefaultConnection getConnectionByReplicaName(TransactionSession transactionSession,String replicaName, boolean update, String strategy) {
        return getDefaultConnection(replicaName, update, strategy, transactionSession);
    }

    public static DefaultConnection getDefaultConnection(String replicaName, boolean update, String strategy, TransactionSession transactionSession) {
        LoadBalanceStrategy loadBalanceByBalanceName = PlugRuntime.INSTCANE.getLoadBalanceByBalanceName(strategy);
        PhysicsInstanceImpl datasource = ReplicaSelectorRuntime.INSTANCE.getDatasourceByReplicaName(replicaName, update, loadBalanceByBalanceName);
        String name;
        if (datasource == null) {
            name = replicaName;
        } else {
            name = datasource.getName();
        }
        return transactionSession.getConnection(Objects.requireNonNull(name));
    }

    public static MycatUpdateResponse executeUpdateByReplicaName(TransactionSession transactionSession,String replicaName,
                                                                 String sql,
                                                                 boolean needGeneratedKeys,
                                                                 String strategy) {
        DefaultConnection connection = getConnectionByReplicaName(transactionSession,replicaName, true, strategy);
        return connection.executeUpdate(sql, needGeneratedKeys,transactionSession.getServerStatus());
    }

    public static MycatUpdateResponse executeUpdate(TransactionSession transactionSession,String datasource, String sql, boolean needGeneratedKeys) {
        DefaultConnection connection = transactionSession.getConnection(datasource);
        return connection.executeUpdate(sql, needGeneratedKeys,transactionSession.getServerStatus());
    }

    public static UpdateRowIteratorResponse executeUpdateByDatasouce(TransactionSession transactionSession,Map<String, List<String>> map, boolean needGeneratedKeys) {
        int lastId = 0;
        int count = 0;
        int serverStatus = 0;
        for (Map.Entry<String, List<String>> backendTableInfoStringEntry : map.entrySet()) {
            for (String s : backendTableInfoStringEntry.getValue()) {
                MycatUpdateResponse mycatUpdateResponse = executeUpdate(transactionSession,backendTableInfoStringEntry.getKey(), s, needGeneratedKeys);
                long lastInsertId = mycatUpdateResponse.getLastInsertId();
                long updateCount = mycatUpdateResponse.getUpdateCount();
                lastId = Math.max((int) lastInsertId, lastId);
                count += updateCount;
                serverStatus = mycatUpdateResponse.serverStatus();
            }
        }
        return new UpdateRowIteratorResponse(count, lastId, serverStatus);
    }
}