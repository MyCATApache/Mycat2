package io.mycat.replica.heartbeat.proxyDetector;

import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.ConfigFile;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.replica.ReplicaType;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartBeatStatus;
import io.mycat.replica.heartbeat.HeartbeatManager;
import io.mycat.replica.heartbeat.NoneHeartbeatDetector;
import io.mycat.replica.heartbeat.strategy.MySQLMasterSlaveBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import java.util.Objects;

/**
 * @author : zhangwy
 * @version V1.0 date Date : 2019年05月14日 22:21
 */
public class MySQLProxyHeartBeatManager extends HeartbeatManager {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLProxyHeartBeatManager.class);
  private final MycatDataSource dataSource;

  public MySQLProxyHeartBeatManager(ProxyRuntime runtime, ReplicaConfig replicaConfig,
      MySQLDataSourceEx dataSource) {
    HeartbeatRootConfig heartbeatRootConfig = runtime.getConfig(ConfigFile.HEARTBEAT);
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(heartbeatRootConfig, "heartbeat config can not found");
    Objects
        .requireNonNull(heartbeatRootConfig.getHeartbeat(), "heartbeat config can not be empty");
    ////////////////////////////////////check/////////////////////////////////////////////////
    this.dataSource = dataSource;
    this.dsStatus = new DatasourceStatus();
    long lastSwitchTime = System.currentTimeMillis();
    HeartbeatConfig heartbeatConfig = heartbeatRootConfig
        .getHeartbeat();
    int maxRetry = heartbeatConfig.getMaxRetry();
    long minSwitchTimeInterval = heartbeatConfig.getMinSwitchTimeInterval();
    this.hbStatus = new HeartBeatStatus(maxRetry, minSwitchTimeInterval, false, lastSwitchTime);
    ReplicaType replicaType = ReplicaType.valueOf(replicaConfig.getRepType());
    switch (replicaType) {
      case SINGLE_NODE:
        this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig,
            dataSource,
            this,
            MySQLSingleHeartBeatStrategy::new);
        break;
      case MASTER_SLAVE:
        this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig,
            dataSource,
            this,
            MySQLMasterSlaveBeatStrategy::new);
        break;
      case GARELA_CLUSTER:
        this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig,
            dataSource,
            this,
            MySQLSingleHeartBeatStrategy::new);
        break;
      case NONE:
        this.heartbeatDetector = new NoneHeartbeatDetector();
        break;
    }
  }


  //给所有的mycatThread发送dataSourceStatus
  @Override
  public void sendDataSourceStatus(DatasourceStatus currentDatasourceStatus) {
    updateInstance(currentDatasourceStatus);
  }

  private void updateInstance(DatasourceStatus currentDatasourceStatus) {
    PhysicsInstance instance = dataSource.instance();
    ReplicaSelectorRuntime.INSTCANE
        .updateInstanceStatus(dataSource.getReplica().getName(), dataSource.getName(),
            isAlive(instance.isMaster()), instance.asSelectRead());
    //状态不同进行状态的同步
    if (!this.dsStatus.equals(currentDatasourceStatus)) {
      //设置状态给 dataSource
      this.dsStatus = currentDatasourceStatus;
      LOGGER.error("{} heartStatus {}", dataSource.getName(), dsStatus);
    }
    if (dataSource.getReplica().getSwitchType().equals(ReplicaSwitchType.SWITCH)
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
