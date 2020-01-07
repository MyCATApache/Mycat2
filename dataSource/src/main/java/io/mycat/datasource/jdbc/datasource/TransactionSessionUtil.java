package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.datasource.jdbc.resultset.MysqlSingleDataNodeResultSetResponse;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.plug.PlugRuntime;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.PhysicsInstanceImpl;
import io.mycat.replica.ReplicaSelectorRuntime;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author Junwen Chen
 **/
public class TransactionSessionUtil {

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
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
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
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.beforeDoAction();
        return transactionSession.getConnection(datasource);
    }

    public static MycatUpdateResponse executeUpdate(String replicaName,
                                                    String sql,
                                                    boolean needGeneratedKeys,
                                                    String strategy) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        try {
            DefaultConnection connection = getConnectionByReplicaName(replicaName,true, strategy);
            transactionSession.beforeDoAction();
            return connection.executeUpdate(sql, needGeneratedKeys);
        } finally {
            transactionSession.afterDoAction();
        }
    }

    public static MycatUpdateResponse executeUpdate(String datasource, String sql, boolean needGeneratedKeys) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        try {
            DefaultConnection connection = getConnectionByDataSource(datasource);
            transactionSession.beforeDoAction();
            return connection.executeUpdate(sql, needGeneratedKeys);
        } finally {
            transactionSession.afterDoAction();
        }
    }

    public static MycatUpdateResponse executeUpdateByDatasouceList(String sql, Collection<String> datasourceList, boolean needGeneratedKeys) {
        return executeUpdateByDatasouce(datasourceList.stream().collect(Collectors.toMap(k -> k, v -> sql)), needGeneratedKeys);
    }

    public static MycatUpdateResponse executeUpdateByDatasouce(Map<String, String> map, boolean needGeneratedKeys) {
        int lastId = 0;
        int count = 0;
        int serverStatus = 0;
        for (Map.Entry<String, String> backendTableInfoStringEntry : map.entrySet()) {
            MycatUpdateResponse mycatUpdateResponse = executeUpdate(backendTableInfoStringEntry.getValue(), backendTableInfoStringEntry.getKey(), needGeneratedKeys);
            long lastInsertId = mycatUpdateResponse.getLastInsertId();
            int updateCount = mycatUpdateResponse.getUpdateCount();
            lastId = Math.max((int) lastInsertId, lastId);
            count += updateCount;
            serverStatus = mycatUpdateResponse.serverStatus();
        }
        return new MycatUpdateResponseImpl(lastId, count, serverStatus);
    }

    public static String getDataSourceByBalance(String replicaName, JdbcDataSourceQuery query) {
        boolean runOnMaster = false;
        LoadBalanceStrategy strategy = null;
        if (query != null) {
            runOnMaster = query.isRunOnMaster();
            strategy = query.getStrategy();
        }
        PhysicsInstance dataSource =
                ReplicaSelectorRuntime.INSTANCE.getDataSourceSelector(replicaName).getDataSource(runOnMaster, strategy);
        return dataSource.getName();
    }

    public static void commit() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.commit();
    }

    public static void setAutocommitOff() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.setAutocommit(false);
    }

    public static void rollback() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.rollback();
    }

    public static void setIsolation(int transactionIsolation) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.setTransactionIsolation(transactionIsolation);
    }

    public static void reset() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.reset();
    }

    public static void afterDoAction() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.afterDoAction();
    }

    public static void beforeDoAction() {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.beforeDoAction();
    }
}