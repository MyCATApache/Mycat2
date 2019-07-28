package io.mycat.datasource.jdbc;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.resultset.MycatResultSetResponse;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import java.util.HashMap;
import java.util.Map;

public class DataNodeSession implements ClearableSession {

  final Map<String, JdbcSession> backends = new HashMap<>();
  final GridRuntime jdbcRuntime;

  MySQLAutoCommit autocommit = MySQLAutoCommit.ON;
  MySQLIsolation isolation = MySQLIsolation.REPEATED_READ;

  public DataNodeSession(GridRuntime jdbcRuntime) {
    this.jdbcRuntime = jdbcRuntime;
  }


  public void setAutomcommit(boolean on) {
    this.autocommit = on ? MySQLAutoCommit.ON : MySQLAutoCommit.OFF;
    for (JdbcSession backend : backends.values()) {
      backend.setAutomcommit(on);
    }
  }

  public void setTransactionIsolation(MySQLIsolation isolation) {
    this.isolation = isolation;
    for (JdbcSession backend : backends.values()) {
      backend.setTransactionIsolation(isolation);
    }
  }

  public MycatResultSetResponse executeQuery(MycatSession mycat, String dataNode, String sql,
      boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    JdbcSession session = getBackendSession(dataNode, runOnMaster, strategy);
    JdbcDataSource datasource = session.getDatasource();
    MycatMonitor
        .onRouteResult(mycat, dataNode, datasource.getReplica().getName(), datasource.getName(),
            sql);
    if (session.getDatasource().getDbType() == null) {
      return new SingleDataNodeResultSetResponse(session.executeQuery(this, sql));
    } else {
      return new TextResultSetResponse(session.executeQuery(this, sql));
    }
  }

  private JdbcSession getBackendSession(String dataNode, boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    JdbcSession session = backends
        .compute(dataNode, (s, session1) -> {
          if (session1 == null) {
            session1 = jdbcRuntime
                .getJdbcSessionByDataNodeName(dataNode, isolation, autocommit,
                    new JdbcDataSourceQuery().setRunOnMaster(runOnMaster).setStrategy(strategy));
          }
          return session1;
        });

    backends.put(dataNode, session);
    return session;
  }

  public MycatUpdateResponse executeUpdate(MycatSession mycat, String dataNode, String sql,
      boolean runOnMaster,
      LoadBalanceStrategy strategy) {
    try {
      JdbcSession session = getBackendSession(dataNode, runOnMaster, strategy);
      JdbcDataSource datasource = session.getDatasource();
      MycatMonitor
          .onRouteResult(mycat, dataNode, datasource.getReplica().getName(), datasource.getName(),
              sql);
      return session.executeUpdate(sql, true);
    } finally {
      clear();
    }
  }

  public void startTransaction() {
    this.autocommit = MySQLAutoCommit.OFF;
  }

  public void commit() {
    try {
      this.autocommit = MySQLAutoCommit.ON;
      for (JdbcSession backend : backends.values()) {
        backend.commit();
        backend.close(true, "commit");
      }
      backends.clear();
    } finally {
      clear();
    }

  }

  public void rollback() {
    try {
      this.autocommit = MySQLAutoCommit.ON;
      for (JdbcSession backend : backends.values()) {
        backend.rollback();
        backend.close(true, "rollback");
      }
      backends.clear();
    } finally {
      clear();
    }
  }

  @Override
  public void clear() {
    if (autocommit == MySQLAutoCommit.ON) {
      for (JdbcSession backend : backends.values()) {
        backend.close(true, "finish");
      }
      backends.clear();
    }
  }
}