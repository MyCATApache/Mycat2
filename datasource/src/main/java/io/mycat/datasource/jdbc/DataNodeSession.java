package io.mycat.datasource.jdbc;

import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.datasource.jdbc.transaction.TransactionStatus;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;

public interface DataNodeSession extends TransactionStatus {

  void setAutomcommit(boolean on);

  void setTransactionIsolation(MySQLIsolation isolation);

  MycatResultSetResponse executeQuery(String dataNode, String sql,
      boolean runOnMaster,
      LoadBalanceStrategy strategy);

  MycatUpdateResponse executeUpdate(String dataNode, String sql,
      boolean insert,
      boolean runOnMaster,
      LoadBalanceStrategy strategy);

  void startTransaction();

  void commit();

  void rollback();
}