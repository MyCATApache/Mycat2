package io.mycat.datasource.jdbc;

import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;

public abstract class AbsractDataNodeSession implements DataNodeSession {

  @Override
  public MycatResultSetResponse executeQuery(String dataNode, String sql, boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    return null;
  }

  @Override
  public MycatUpdateResponse executeUpdate(String dataNode, String sql, boolean insert,
      boolean runOnMaster, LoadBalanceStrategy strategy) {
    return null;
  }


  @Override
  public void close() {

  }
}