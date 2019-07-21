package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.datasource.jdbc.JdbcReplica;
import io.mycat.replica.heartbeat.jdbcDetector.JdbcHeartbeatManager;

public class JdbcDataSourceEx extends JdbcDataSource {
  final JdbcHeartbeatManager jdbcHeartbeatManager;
  public JdbcDataSourceEx(GridRuntime runtime,int index, DatasourceConfig datasourceConfig,
      JdbcReplica mycatReplica) {
    super(index, datasourceConfig, mycatReplica);
    jdbcHeartbeatManager = new JdbcHeartbeatManager(this,runtime);
  }
  public void heartBeat() {
    jdbcHeartbeatManager.heartBeat();
  }

  @Override
  public boolean isAlive() {
    if(isMaster()) {
      return jdbcHeartbeatManager.getDsStatus().isAlive();
    } else {
      return jdbcHeartbeatManager.getDsStatus().isAlive()&& asSelectRead();
    }
  }

  @Override
  public boolean asSelectRead() {
    return jdbcHeartbeatManager.getDsStatus().isAlive()
        && jdbcHeartbeatManager.getDsStatus().isSlaveBehindMaster() == false
        && jdbcHeartbeatManager.getDsStatus().isDbSynStatusNormal();
  }
}