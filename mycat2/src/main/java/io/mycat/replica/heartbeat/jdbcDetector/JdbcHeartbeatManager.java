package io.mycat.replica.heartbeat.jdbcDetector;

import io.mycat.config.ConfigEnum;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaConfig.RepSwitchTypeEnum;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.datasource.jdbc.JdbcDataSource;
import io.mycat.datasource.jdbc.JdbcReplica;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatStatus;
import io.mycat.replica.heartbeat.HeartbeatManager;
import io.mycat.replica.heartbeat.NoneHeartbeatDetector;
import io.mycat.replica.heartbeat.strategy.MySQLMasterSlaveBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import java.util.Objects;

public class JdbcHeartbeatManager extends HeartbeatManager {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcHeartbeatManager.class);

  final JdbcDataSource dataSource;
  final GridRuntime runtime;

  public JdbcHeartbeatManager(JdbcDataSource jdbcDataSource,
      GridRuntime runtime) {
    HeartbeatRootConfig heartbeatRootConfig = runtime.getConfig(ConfigEnum.HEARTBEAT);
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(heartbeatRootConfig, "heartbeat config can not found");
    Objects
        .requireNonNull(heartbeatRootConfig.getHeartbeat(), "heartbeat config can not be empty");
    ////////////////////////////////////check/////////////////////////////////////////////////
    this.dataSource = jdbcDataSource;
    this.runtime = runtime;
    this.dsStatus = new DatasourceStatus();
    long lastSwitchTime = System.currentTimeMillis();
    HeartbeatConfig heartbeatConfig = heartbeatRootConfig
        .getHeartbeat();
    int maxRetry = heartbeatConfig.getMaxRetry();
    long minSwitchTimeInterval = heartbeatConfig.getMinSwitchTimeInterval();
    this.hbStatus = new HeartBeatStatus(maxRetry, minSwitchTimeInterval, false, lastSwitchTime);

    JdbcReplica replica = jdbcDataSource.getReplica();
    ReplicaConfig replicaConfig = replica.getReplicaConfig();

    if (ReplicaConfig.RepTypeEnum.SINGLE_NODE.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultJdbcHeartbeatDetector(runtime, replica, jdbcDataSource,
          this, MySQLSingleHeartBeatStrategy::new);
    } else if (ReplicaConfig.RepTypeEnum.MASTER_SLAVE.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultJdbcHeartbeatDetector(runtime, replica, jdbcDataSource,
          this,
          MySQLMasterSlaveBeatStrategy::new);
    } else if (ReplicaConfig.RepTypeEnum.GARELA_CLUSTER.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultJdbcHeartbeatDetector(runtime, replica, jdbcDataSource,
          this,
          MySQLSingleHeartBeatStrategy::new);
    } else {
      this.heartbeatDetector = new NoneHeartbeatDetector();
    }
  }

  @Override
  protected void sendDataSourceStatus(DatasourceStatus currentDatasourceStatus) {
    //状态不同进行状态的同步
    if (!this.dsStatus.equals(currentDatasourceStatus)) {
      //设置状态给 dataSource
      this.dsStatus = currentDatasourceStatus;
      LOGGER.error("{} heartStatus {}", dataSource.getName(), dsStatus);
    }
    ReplicaConfig conf = this.dataSource.getReplica().getConfig();
    if (conf.getSwitchType().equals(RepSwitchTypeEnum.SWITCH)
        && dataSource.isMaster() && dsStatus.isError()
        && canSwitchDataSource()) {
      //replicat 进行选主
      if (dataSource.getReplica().switchDataSourceIfNeed()) {
        //updataSwitchTime
        this.hbStatus.setLastSwitchTime(System.currentTimeMillis());
      }
    }
  }
}