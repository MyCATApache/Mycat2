package io.mycat.datasource.jdbc;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import java.util.ArrayList;
import java.util.List;

public class DataNodeSession {

  final List<JdbcSession> backends = new ArrayList<>();
  final GridRuntime jdbcRuntime;

  MySQLAutoCommit autocommit = MySQLAutoCommit.ON;
  MySQLIsolation isolation = MySQLIsolation.REPEATED_READ;

  public DataNodeSession(GridRuntime jdbcRuntime) {
    this.jdbcRuntime = jdbcRuntime;
  }


  public void setAutomcommit(boolean on) {
    this.autocommit = on ? MySQLAutoCommit.ON : MySQLAutoCommit.OFF;
    for (JdbcSession backend : backends) {
      backend.setAutomcommit(on);
    }
  }

  public void setTransactionIsolation(MySQLIsolation isolation) {
    this.isolation = isolation;
    for (JdbcSession backend : backends) {
      backend.setTransactionIsolation(isolation);
    }
  }

  public MycatResultSetResponse executeQuery(String dataNode, String sql, boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    JdbcSession session = this.jdbcRuntime
        .getJdbcSessionByDataNodeName(dataNode, isolation, autocommit,
            new JdbcDataSourceQuery().setRunOnMaster(runOnMaster).setStrategy(strategy));
    backends.add(session);
    return new MycatResultSetResponseImpl(session, session.executeQuery(sql));
  }

  public  MycatUpdateResponse executeUpdate(String dataNode, String sql,boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    JdbcSession session = this.jdbcRuntime
        .getJdbcSessionByDataNodeName(dataNode, isolation, autocommit,
            new JdbcDataSourceQuery().setRunOnMaster(runOnMaster).setStrategy(strategy));
    backends.add(session);
    return session.executeUpdate(sql,true);
  }

  public void startTransaction() {
    this.autocommit = MySQLAutoCommit.OFF;
  }

  public void commit() {
    this.autocommit = MySQLAutoCommit.ON;
    for (JdbcSession backend : backends) {
      backend.commit();
      backend.close(true, "commit");
    }
    backends.clear();
  }

  public void rollback() {
    this.autocommit = MySQLAutoCommit.ON;
    for (JdbcSession backend : backends) {
      backend.rollback();
      backend.close(true, "rollback");
    }
    backends.clear();
  }

  public void finish() {
    if (autocommit == MySQLAutoCommit.ON){
      for (JdbcSession backend : backends) {
        backend.close(true,"finish");
      }
    }
  }
}