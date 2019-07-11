package io.mycat.datasource.jdbc;

import io.mycat.plug.loadBalance.LoadBalanceStrategy;

public class JdbcDataSourceQuery {

  boolean runOnMaster = false;
  LoadBalanceStrategy strategy = null;

  public boolean isRunOnMaster() {
    return runOnMaster;
  }

  public void setRunOnMaster(boolean runOnMaster) {
    this.runOnMaster = runOnMaster;
  }

  public LoadBalanceStrategy getStrategy() {
    return strategy;
  }

  public void setStrategy(LoadBalanceStrategy strategy) {
    this.strategy = strategy;
  }
}