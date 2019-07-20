package io.mycat.replica.heartbeat.proxyDetector;

import io.mycat.config.ConfigEnum;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaConfig.RepSwitchTypeEnum;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heartbeat.DatasourceStatus;
import io.mycat.replica.heartbeat.HeartbeatDetector;
import io.mycat.replica.heartbeat.HeartbeatManager;
import io.mycat.replica.heartbeat.NoneHeartbeatDetector;
import io.mycat.replica.heartbeat.strategy.MySQLMasterSlaveBeatStrategy;
import io.mycat.replica.heartbeat.strategy.MySQLSingleHeartBeatStrategy;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author : zhangwy
 * @version V1.0 date Date : 2019年05月14日 22:21
 */
public class MySQLProxyHeartBeatManager implements HeartbeatManager {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLProxyHeartBeatManager.class);

  private final HeartbeatDetector heartbeatDetector;

  private final MySQLDatasource dataSource;
  private volatile DatasourceStatus heartBeatStatus;

  protected int maxRetry = 3; //错误maxRetry设置为错误
  private final long minSwitchTimeInterval; //配置最小切换时间

  protected volatile boolean isChecking = false; //是否正在检查
  protected AtomicInteger errorCount = new AtomicInteger(0); //错误计数


  private long lastSwitchTime;//上次主从切换时间

  public MySQLProxyHeartBeatManager(ProxyRuntime runtime, ReplicaConfig replicaConfig,
      MySQLDataSourceEx dataSource) {
    HeartbeatRootConfig heartbeatRootConfig = runtime.getConfig(ConfigEnum.HEARTBEAT);
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(heartbeatRootConfig, "heartbeat config can not found");
    Objects
        .requireNonNull(heartbeatRootConfig.getHeartbeat(), "heartbeat config can not be empty");
    ////////////////////////////////////check/////////////////////////////////////////////////

    this.dataSource = dataSource;
    this.heartBeatStatus = new DatasourceStatus();
    this.lastSwitchTime = System.currentTimeMillis();

    HeartbeatConfig heartbeatConfig = heartbeatRootConfig
        .getHeartbeat();
    this.maxRetry = heartbeatConfig.getMaxRetry();
    this.minSwitchTimeInterval = heartbeatConfig.getMinSwitchTimeInterval();
    if (ReplicaConfig.RepTypeEnum.SINGLE_NODE.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig, dataSource,
          this,
          MySQLSingleHeartBeatStrategy::new);
    } else if (ReplicaConfig.RepTypeEnum.MASTER_SLAVE.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig, dataSource,
          this,
          MySQLMasterSlaveBeatStrategy::new);
    } else if (ReplicaConfig.RepTypeEnum.GARELA_CLUSTER.equals(replicaConfig.getRepType())) {
      this.heartbeatDetector = new DefaultProxyHeartbeatDetector(runtime, replicaConfig, dataSource,
          this,
          MySQLSingleHeartBeatStrategy::new);
    } else {
      this.heartbeatDetector = new NoneHeartbeatDetector();
    }

  }

  /**
   * 1: 发送sql 判断是否已经发送 如果还未发送 则发送sql  设置为发送中。。 如果为发送中。。 判断是否已经超时, 超时则设置超时状态  设置为未发送 如果发生错误, 则设置错误状态.
   * 设置为未发送 2. 检查结果集 --&gt; 设置结果集状态 设置为未发送 设置失败集状态 设置为未发送 设置超时状态
   */
  public void heartBeat() {
    if (isChecking == false) {
      isChecking = true;
      this.heartbeatDetector.heartBeat();
    } else if (this.heartbeatDetector.isHeartbeatTimeout()) {
      DatasourceStatus datasourceStatus = new DatasourceStatus();
      datasourceStatus.setStatus(DatasourceStatus.TIMEOUT_STATUS);
      this.setStatus(datasourceStatus, DatasourceStatus.TIMEOUT_STATUS);
    }
  }

  public void setStatus(DatasourceStatus datasourceStatus, int status) {
    //对应的status 状态进行设置
    switch (status) {
      case DatasourceStatus.OK_STATUS:
        setOk(datasourceStatus);
        break;
      case DatasourceStatus.ERROR_STATUS:
        setError(datasourceStatus);
        break;
      case DatasourceStatus.TIMEOUT_STATUS:
        setTimeout(datasourceStatus);
        break;
    }

    this.heartbeatDetector.updateLastReceivedQryTime();
    isChecking = false;
  }

  private void setTimeout(DatasourceStatus datasourceStatus) {
    errorCount.incrementAndGet();
    heartbeatDetector.quitDetector();
    if (errorCount.get() == maxRetry) {
      datasourceStatus.setStatus(DatasourceStatus.TIMEOUT_STATUS);
      sendDataSourceStatus(datasourceStatus);
      errorCount.set(0);
    }
  }

  private void setError(DatasourceStatus datasourceStatus) {
    errorCount.incrementAndGet();
    heartbeatDetector.quitDetector();
    if (errorCount.get() == maxRetry) {
      datasourceStatus.setStatus(DatasourceStatus.ERROR_STATUS);
      sendDataSourceStatus(datasourceStatus);
      errorCount.set(0);
    }
  }

  private void setOk(DatasourceStatus datasourceStatus) {
    //对应的status 状态进行设置
    switch (this.heartBeatStatus.getStatus()) {
      case DatasourceStatus.INIT_STATUS:
      case DatasourceStatus.OK_STATUS:
        datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);
        errorCount.set(0);
        break;
      case DatasourceStatus.ERROR_STATUS:
        datasourceStatus.setStatus(DatasourceStatus.INIT_STATUS);
        errorCount.set(0);
        break;
      case DatasourceStatus.TIMEOUT_STATUS:
        datasourceStatus.setStatus(DatasourceStatus.INIT_STATUS);
        errorCount.set(0);
        break;
      default:
        datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);
    }
    sendDataSourceStatus(datasourceStatus);
  }

  //给所有的mycatThread发送dataSourceStatus
  public void sendDataSourceStatus(DatasourceStatus currentDatasourceStatus) {
    //状态不同进行状态的同步
    if (!this.heartBeatStatus.equals(currentDatasourceStatus)) {
      //设置状态给 dataSource
      this.heartBeatStatus = currentDatasourceStatus;
      LOGGER.error("{} heartStatus {}", dataSource.getName(), heartBeatStatus);
    }
    ReplicaConfig conf = this.dataSource.getReplica().getConfig();
    if (conf.getSwitchType().equals(RepSwitchTypeEnum.SWITCH)
        && dataSource.isMaster() && heartBeatStatus.isError()
        && canSwitchDataSource()) {
      //replicat 进行选主
      if (dataSource.getReplica().switchDataSourceIfNeed()) {
        //updataSwitchTime
        this.lastSwitchTime = System.currentTimeMillis();
      }
    }
  }

  private boolean canSwitchDataSource() {
    return this.lastSwitchTime + this.minSwitchTimeInterval < System.currentTimeMillis();
  }

  public DatasourceStatus getHeartBeatStatus() {
    return heartBeatStatus;
  }
}
