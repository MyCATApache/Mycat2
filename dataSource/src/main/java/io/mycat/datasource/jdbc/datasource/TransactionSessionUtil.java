package io.mycat.datasource.jdbc.datasource;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.resultset.MysqlSingleDataNodeResultSetResponse;
import io.mycat.datasource.jdbc.resultset.TextResultSetResponse;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import java.util.Objects;
/**
 * @author Junwen Chen
 **/
public class TransactionSessionUtil {

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
    JdbcDataSource dataSource =runtime.getJdbcDatasourceByName(datasource);
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
}