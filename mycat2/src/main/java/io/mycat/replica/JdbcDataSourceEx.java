package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.datasource.jdbc.JdbcReplica;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.reactor.SessionThread;
import io.mycat.replica.heartbeat.jdbcDetector.JdbcHeartbeatManager;

public class JdbcDataSourceEx extends JdbcDataSource {

  private final static MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(JdbcDataSourceEx.class);
  final JdbcHeartbeatManager jdbcHeartbeatManager;

  public JdbcDataSourceEx(GridRuntime runtime, int index, DatasourceConfig datasourceConfig,
      JdbcReplica mycatReplica) {
    super(index, datasourceConfig, mycatReplica);
    jdbcHeartbeatManager = new JdbcHeartbeatManager(this, runtime);
  }

  public void heartBeat() {
    SessionThread thread = (SessionThread) Thread.currentThread();
    try {
      jdbcHeartbeatManager.heartBeat();
    } catch (Exception e) {
      LOGGER.error("", e);
      thread.onExceptionClose();
    } finally {
      thread.close();
    }
  }

  @Override
  public boolean isAlive() {
    if (isMaster()) {
      return jdbcHeartbeatManager.getDsStatus().isAlive();
    } else {
      return jdbcHeartbeatManager.getDsStatus().isAlive() && asSelectRead();
    }
  }

  @Override
  public boolean asSelectRead() {
    return jdbcHeartbeatManager.getDsStatus().isAlive()
        && jdbcHeartbeatManager.getDsStatus().isSlaveBehindMaster() == false
        && jdbcHeartbeatManager.getDsStatus().isDbSynStatusNormal();
  }
}