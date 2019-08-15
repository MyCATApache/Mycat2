/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.replica.heartbeat;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.replica.PhysicsInstance;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
public abstract class HeartbeatFlow {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(HeartbeatFlow.class);
  protected final HeartBeatStatus hbStatus;
  protected final long heartbeatTimeout;
  protected final long slaveThreshold;
  protected final PhysicsInstance instance;
  protected volatile DatasourceStatus dsStatus;
  protected volatile long lastSendQryTime;
  protected volatile long lastReceivedQryTime;//    private isCheck


  public HeartbeatFlow(PhysicsInstance instance, int maxRetry,
      long minSwitchTimeInterval, long heartbeatTimeout,
      long slaveThreshold) {
    this.instance = instance;
    this.slaveThreshold = slaveThreshold;
    this.dsStatus = new DatasourceStatus();
    this.hbStatus = new HeartBeatStatus(maxRetry, minSwitchTimeInterval, false,
        System.currentTimeMillis());
    this.heartbeatTimeout = heartbeatTimeout;
  }

  public boolean isHeartbeatTimeout() {
    return System.currentTimeMillis() > Math.max(lastSendQryTime,
        lastReceivedQryTime) + heartbeatTimeout;
  }

  public void updateLastReceivedQryTime() {
    this.lastReceivedQryTime = System.currentTimeMillis();
  }

  public void updateLastSendQryTime() {
    this.lastSendQryTime = System.currentTimeMillis();
  }

  public void setStatus(int status) {
    DatasourceStatus datasourceStatus = new DatasourceStatus();
    setStatus(datasourceStatus, status);
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
    updateLastReceivedQryTime();
    this.hbStatus.setChecking(false);
  }

  protected void setError(DatasourceStatus datasourceStatus) {
    this.hbStatus.incrementErrorCount();
    setTaskquitDetector();
    if (this.hbStatus.getErrorCount() >= this.hbStatus.getMaxRetry()) {
      datasourceStatus.setStatus(DatasourceStatus.ERROR_STATUS);
      sendDataSourceStatus(datasourceStatus);
      this.hbStatus.setErrorCount(0);
    }
  }

  protected void setOk(DatasourceStatus datasourceStatus) {
    //对应的status 状态进行设置
    switch (this.dsStatus.getStatus()) {
      case DatasourceStatus.INIT_STATUS:
      case DatasourceStatus.OK_STATUS:
        datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);
        this.hbStatus.setErrorCount(0);
        break;
      case DatasourceStatus.ERROR_STATUS:
        datasourceStatus.setStatus(DatasourceStatus.INIT_STATUS);
        this.hbStatus.setErrorCount(0);
        break;
      case DatasourceStatus.TIMEOUT_STATUS:
        datasourceStatus.setStatus(DatasourceStatus.INIT_STATUS);
        this.hbStatus.setErrorCount(0);
        break;
      default:
        datasourceStatus.setStatus(DatasourceStatus.OK_STATUS);
    }
    sendDataSourceStatus(datasourceStatus);
  }

  protected void setTimeout(DatasourceStatus datasourceStatus) {
    this.hbStatus.incrementErrorCount();
    setTaskquitDetector();
    if (this.hbStatus.getErrorCount() >= this.hbStatus.getMaxRetry()) {
      datasourceStatus.setStatus(DatasourceStatus.TIMEOUT_STATUS);
      sendDataSourceStatus(datasourceStatus);
      this.hbStatus.setErrorCount(0);
    }
  }

  public abstract void heartbeat();

  public abstract void sendDataSourceStatus(DatasourceStatus status);

  public abstract void setTaskquitDetector();

  public long getSlaveThreshold() {
    return slaveThreshold;
  }

  public PhysicsInstance instance() {
    return instance;
  }
}