package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.beans.resultset.MycatUpdateResponseImpl;
import io.mycat.calcite.BackendTableInfo;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.resultset.MysqlSingleDataNodeResultSetResponse;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author Junwen Chen
 **/
public class TransactionSessionUtil {
    public static MycatResultSetResponse executeQuery(String sql, BackendTableInfo backendTableInfo) {
        return executeQuery(sql, backendTableInfo, true, null);
    }

    public static MycatResultSetResponse executeQuery(String sql, BackendTableInfo backendTableInfo, boolean runOnMaster, LoadBalanceStrategy strategy) {
        DsConnection connection = backendTableInfo.getSession(runOnMaster, strategy);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static MycatResultSetResponse executeQuery(String dataNode, String sql,
                                                      boolean runOnMaster,
                                                      LoadBalanceStrategy strategy) {
        DsConnection connection = getConnectionByDataNode(dataNode,
                runOnMaster, strategy);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static MycatResultSetResponse executeQuery(String datasource, String sql) {
        DsConnection connection = getConnectionByDataSource(datasource);
        if (connection.getDataSource().isMySQLType()) {
            return new MysqlSingleDataNodeResultSetResponse(connection.executeQuery(sql));
        } else {
            return new TextResultSetResponse(connection.executeQuery(sql));
        }
    }

    public static DsConnection getConnectionByDataNode(String dataNode,
                                                       boolean runOnMaster,
                                                       LoadBalanceStrategy strategy) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.beforeDoAction();
        GRuntime runtime = GRuntime.INSTACNE;
        Objects.requireNonNull(dataNode);
        JdbcDataSource dataSource = runtime
                .getJdbcDatasourceByDataNodeName(dataNode,
                        new JdbcDataSourceQuery()
                                .setRunOnMaster(runOnMaster)
                                .setStrategy(strategy));
        return transactionSession.getConnection(dataSource);
    }

    public static DsConnection getConnectionByDataSource(String datasource) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        transactionSession.beforeDoAction();
        GRuntime runtime = GRuntime.INSTACNE;
        JdbcDataSource dataSource = runtime.getJdbcDatasourceByName(datasource);
        Objects.requireNonNull(dataSource);
        return transactionSession.getConnection(dataSource);
    }

    public static MycatUpdateResponse executeUpdate(String dataNode,
                                                    String sql,
                                                    boolean insert,
                                                    boolean runOnMaster,
                                                    LoadBalanceStrategy strategy) {
        GThread processUnit = (GThread) Thread.currentThread();
        TransactionSession transactionSession = processUnit.getTransactionSession();
        try {
            DsConnection connection = getConnectionByDataNode(dataNode,
                    runOnMaster, strategy);
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
            DsConnection connection = getConnectionByDataSource(datasource);
            transactionSession.beforeDoAction();
            return connection.executeUpdate(sql, needGeneratedKeys);
        } finally {
            transactionSession.afterDoAction();
        }
    }

    public static MycatUpdateResponse executeUpdate(String sql, BackendTableInfo backendTableInfo) {
        return executeUpdate(sql, backendTableInfo.getSession(true, null), true);
    }

    public static MycatUpdateResponse executeUpdate(String sql, List<BackendTableInfo> backendTableInfo) {
        MycatUpdateResponse mycatUpdateResponse = null;
        for (BackendTableInfo tableInfo : backendTableInfo) {
            mycatUpdateResponse = executeUpdate(sql, tableInfo);
        }
        return mycatUpdateResponse;
    }

    public static MycatUpdateResponse executeUpdate(Map<BackendTableInfo, String> map) {
        int lastId = 0;
        int count = 0;
        int serverStatus=0;
        for (Map.Entry<BackendTableInfo, String> backendTableInfoStringEntry : map.entrySet()) {
            MycatUpdateResponse mycatUpdateResponse = executeUpdate(backendTableInfoStringEntry.getValue(), backendTableInfoStringEntry.getKey());
            long lastInsertId = mycatUpdateResponse.getLastInsertId();
            int updateCount = mycatUpdateResponse.getUpdateCount();
            lastId = Math.max((int)lastInsertId, lastId);
            count += updateCount;
            serverStatus = mycatUpdateResponse.serverStatus();
        }
        return new  MycatUpdateResponseImpl(lastId,count,serverStatus);
    }

}
}