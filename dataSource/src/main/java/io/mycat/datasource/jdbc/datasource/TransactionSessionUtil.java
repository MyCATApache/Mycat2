package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.datasource.jdbc.resultset.MysqlSingleDataNodeResultSetResponse;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.datasource.jdbc.thread.GThread;
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

    public static MycatResultSetResponse executeQuery(String replicaName, String sql, LoadBalanceStrategy strategy) {
        DefaultConnection connection = getConnectionByReplicaName(replicaName, strategy);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static MycatResultSetResponse executeQuery(String datasource, String sql) {
        DefaultConnection connection = getConnectionByDataSource(datasource);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static DefaultConnection getConnectionByReplicaName(String replicaName, LoadBalanceStrategy strategy) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.beforeDoAction();
        PhysicsInstanceImpl datasource = ReplicaSelectorRuntime.INSTANCE.getDatasourceByReplicaName(replicaName, strategy);
        return transactionSession.getConnection(Objects.requireNonNull(datasource.getName()));
    }

    public static DefaultConnection getConnectionByDataSource(String datasource) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.beforeDoAction();
        return transactionSession.getConnection(datasource);
    }

    public static MycatUpdateResponse executeUpdate(String replicaName,
                                                    String sql,
                                                    boolean insert,
                                                    LoadBalanceStrategy strategy) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        try {
            DefaultConnection connection = getConnectionByReplicaName(replicaName, strategy);
            transactionSession.beforeDoAction();
            return connection.executeUpdate(sql, insert);
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

    public static MycatUpdateResponse executeUpdateByDatasouce(String sql, Collection<String> datasourceList) {
       return executeUpdateByDatasouce(datasourceList.stream().collect(Collectors.toMap(k->k,v->sql)));
    }

    public static MycatUpdateResponse executeUpdateByDatasouce(Map<String, String> map) {
        int lastId = 0;
        int count = 0;
        int serverStatus = 0;
        for (Map.Entry<String, String> backendTableInfoStringEntry : map.entrySet()) {
            MycatUpdateResponse mycatUpdateResponse = executeUpdate(backendTableInfoStringEntry.getValue(), backendTableInfoStringEntry.getKey(),true);
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
}