package io.mycat.replica.heartbeat.jdbcDetector;

import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.ConfigFile;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.datasource.jdbc.datasource.JdbcReplica;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaRuntime;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.replica.ReplicaType;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatStatus;
import io.mycat.replica.heartbeat.HeartbeatManager;
import io.mycat.replica.heartbeat.NoneHeartbeatDetector;
import io.mycat.replica.heartbeat.strategy.MySQLMasterSlaveBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import java.util.Objects;

public class JdbcHeartbeatManager extends HeartbeatManager {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcHeartbeatManager.class);

  final MycatDataSource dataSource;
  final GRuntime runtime;

  public JdbcHeartbeatManager(JdbcDataSource jdbcDataSource,
      GRuntime runtime) {
    HeartbeatRootConfig heartbeatRootConfig = runtime.getConfig(ConfigFile.HEARTBEAT);
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
    ReplicaType replicaType = ReplicaType.valueOf(replicaConfig.getRepType());
    switch (replicaType) {
      case SINGLE_NODE:
        this.heartbeatDetector = new DefaultJdbcHeartbeatDetector(runtime, replica, jdbcDataSource,
            this, MySQLSingleHeartBeatStrategy::new);
        break;
      case MASTER_SLAVE:
        this.heartbeatDetector = new DefaultJdbcHeartbeatDetector(runtime, replica, jdbcDataSource,
            this,
            MySQLMasterSlaveBeatStrategy::new);
        break;
      case GARELA_CLUSTER:
        this.heartbeatDetector = new DefaultJdbcHeartbeatDetector(runtime, replica, jdbcDataSource,
            this,
            MySQLSingleHeartBeatStrategy::new);
        break;
      case NONE:
        this.heartbeatDetector = new NoneHeartbeatDetector();
        break;
    }
  }

  @Override
  public void sendDataSourceStatus(DatasourceStatus currentDatasourceStatus) {
    PhysicsInstance instance = dataSource.instance();
    ReplicaRuntime.INSTCANE
        .updateInstanceStatus(dataSource.getReplica().getName(), dataSource.getName(),
            isAlive(instance.isMaster()), instance.asSelectRead());
    //状态不同进行状态的同步
    if (!this.dsStatus.equals(currentDatasourceStatus)) {
      //设置状态给 dataSource
      this.dsStatus = currentDatasourceStatus;
      LOGGER.error("{} heartStatus {}", dataSource.getName(), dsStatus);
    }
    ReplicaSwitchType replicaSwitchType = this.dataSource.getReplica().getSwitchType();
    if (replicaSwitchType.equals(ReplicaSwitchType.SWITCH)
        && dataSource.instance().isMaster() && dsStatus.isError()
        && canSwitchDataSource()) {
      //replicat 进行选主
      if (dataSource.getReplica().switchDataSourceIfNeed()) {
        //updataSwitchTime
        this.hbStatus.setLastSwitchTime(System.currentTimeMillis());
      }
    }
  }
}