package io.mycat.datasource.jdbc;

import io.mycat.plug.loadBalance.LoadBalanceStrategy;

public class JdbcDataSourceQuery {

  boolean runOnMaster = true;
  LoadBalanceStrategy strategy = null;

  public boolean isRunOnMaster() {
    return runOnMaster;
  }

  public JdbcDataSourceQuery setRunOnMaster(boolean runOnMaster) {
    this.runOnMaster = runOnMaster;
    return this;
  }

  public LoadBalanceStrategy getStrategy() {
    return strategy;
  }

  public JdbcDataSourceQuery setStrategy(LoadBalanceStrategy strategy) {
    this.strategy = strategy;
    return this;
  }
}