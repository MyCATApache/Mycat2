package io.mycat.datasource.jdbc;

import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.session.MycatSession;

public interface DataNodeSession extends ClearableSession {

  void setAutomcommit(boolean on);

  void setTransactionIsolation(MySQLIsolation isolation);

  MycatResultSetResponse executeQuery(MycatSession mycat, String dataNode, String sql,
      boolean runOnMaster,
      LoadBalanceStrategy strategy);

  MycatUpdateResponse executeUpdate(MycatSession mycat, String dataNode, String sql,
      boolean insert,
      boolean runOnMaster,
      LoadBalanceStrategy strategy);

  void startTransaction();

  void commit();

  void rollback();
}