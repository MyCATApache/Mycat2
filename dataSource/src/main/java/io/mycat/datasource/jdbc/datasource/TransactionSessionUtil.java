package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.resultset.MysqlSingleDataNodeResultSetResponse;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.datasource.jdbc.thread.GThread;
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
    static final ThreadLocal<TransactionSession> transactionSessionThreadLocal = ThreadLocal.withInitial(JdbcRuntime.INSTANCE::createTransactionSession);

    public static TransactionSession currentTransactionSession() {
        return transactionSessionThreadLocal.get();
    }

    public static MycatResultSetResponse executeQuery(String replicaName, String sql, boolean update, String strategy) {
        DefaultConnection connection = getConnectionByReplicaName(replicaName, update, strategy);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static MycatResultSetResponse executeQuery(String datasource, String sql) {
        Objects.requireNonNull(datasource);
        DefaultConnection connection = getConnectionByDataSource(datasource);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static DefaultConnection getConnectionByReplicaName(String replicaName, boolean update, String strategy) {
        LoadBalanceStrategy loadBalanceByBalanceName = PlugRuntime.INSTCANE.getLoadBalanceByBalanceName(strategy);
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.beforeDoAction();
        PhysicsInstanceImpl datasource = ReplicaSelectorRuntime.INSTANCE.getDatasourceByReplicaName(replicaName, update, loadBalanceByBalanceName);
        String name;
        if (datasource == null) {
            name = replicaName;
        } else {
            name = datasource.getName();
        }
        return transactionSession.getConnection(Objects.requireNonNull(name));
    }

    public static DefaultConnection getConnectionByDataSource(String datasource) {
        Objects.requireNonNull(datasource);
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.beforeDoAction();
        return transactionSession.getConnection(datasource);
    }

    public static MycatUpdateResponse executeUpdateByReplicaName(String replicaName,
                                                    String sql,
                                                    boolean needGeneratedKeys,
                                                    String strategy) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        try {
            transactionSession.beforeDoAction();
            DefaultConnection connection = getConnectionByReplicaName(replicaName, true, strategy);

            return connection.executeUpdate(sql, needGeneratedKeys);
        } finally {
            transactionSession.afterDoAction();
        }
    }

    public static MycatUpdateResponse executeUpdate(String datasource, String sql, boolean needGeneratedKeys) {
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        try {
            transactionSession.beforeDoAction();
            DefaultConnection connection = getConnectionByDataSource(datasource);
            return connection.executeUpdate(sql, needGeneratedKeys);
        } finally {
            transactionSession.afterDoAction();
        }
    }

    public static MycatUpdateResponse executeUpdateByDatasouce(Map<String, List<String>> map, boolean needGeneratedKeys) {
        int lastId = 0;
        int count = 0;
        int serverStatus = 0;
        for (Map.Entry<String, List<String>> backendTableInfoStringEntry : map.entrySet()) {
            for (String s : backendTableInfoStringEntry.getValue()) {
                MycatUpdateResponse mycatUpdateResponse = executeUpdate(backendTableInfoStringEntry.getKey(), s, needGeneratedKeys);
                long lastInsertId = mycatUpdateResponse.getLastInsertId();
                int updateCount = mycatUpdateResponse.getUpdateCount();
                lastId = Math.max((int) lastInsertId, lastId);
                count += updateCount;
                serverStatus = mycatUpdateResponse.serverStatus();
            }
        }
        return new MycatUpdateResponseImpl(count, lastId, serverStatus);
    }

    public static void commit() {
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.commit();
    }

    public static void setAutocommitOff() {
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.setAutocommit(false);
    }

    public static void rollback() {
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.rollback();
    }

    public static void setIsolation(int transactionIsolation) {
        beforeDoAction();
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.setTransactionIsolation(transactionIsolation);
    }

    public static void reset() {
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.reset();
    }

    public static void afterDoAction() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.afterDoAction();
    }

    public static void beforeDoAction() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.beforeDoAction();
    }

    public static void begin() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = TransactionSessionUtil.currentTransactionSession();
        transactionSession.begin();
    }
}