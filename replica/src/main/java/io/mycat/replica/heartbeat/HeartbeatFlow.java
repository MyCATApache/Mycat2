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

import io.mycat.replica.PhysicsInstance;
import lombok.Getter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.mycat.replica.heartbeat.DatasourceEnum.*;

/**
 * @author : zhangwy date Date : 2019年05月15日 21:34
 */
@Getter
public abstract class HeartbeatFlow {

  private static final Logger LOGGER = LoggerFactory.getLogger(HeartbeatFlow.class);
  protected final HeartBeatStatus hbStatus;
  protected final long heartbeatTimeout;
  protected final double slaveThreshold;
  protected final PhysicsInstance instance;
  protected volatile DatasourceStatus dsStatus;
  protected volatile long lastSendQryTime;
  protected volatile long lastReceivedQryTime;//    private isCheck


  public HeartbeatFlow(PhysicsInstance instance, int maxRetry,
      long minSwitchTimeInterval, long heartbeatTimeout,
                       double slaveThreshold) {
    this.instance = instance;
    this.slaveThreshold = slaveThreshold;
    this.dsStatus = new DatasourceStatus();
    this.hbStatus = new HeartBeatStatus(maxRetry, minSwitchTimeInterval, false, 0);
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

//  public void setStatus(DatasourceEnum status) {
//    DatasourceStatus datasourceStatus = new DatasourceStatus();
//    setStatus(datasourceStatus, status);
//  }

  public void setStatus(DatasourceStatus datasourceStatus, DatasourceEnum status) {
    //对应的status 状态进行设置
    switch (status) {
      case OK_STATUS:
        setOk(datasourceStatus);
        break;
      case ERROR_STATUS:
        setError(datasourceStatus);
        break;
      case TIMEOUT_STATUS:
        setTimeout(datasourceStatus);
        break;
      case INIT_STATUS:
        break;
    }
    updateLastReceivedQryTime();
    this.hbStatus.setChecking(false);
  }

  protected void setError(DatasourceStatus datasourceStatus) {
    this.hbStatus.incrementErrorCount();
    setTaskquitDetector();
    if (this.hbStatus.getErrorCount() >= this.hbStatus.getMaxRetry()) {
      datasourceStatus.setStatus(ERROR_STATUS);
      sendDataSourceStatus(datasourceStatus);
      this.hbStatus.setErrorCount(0);
    }
  }

  protected void setOk(DatasourceStatus datasourceStatus) {
    //对应的status 状态进行设置
    switch (this.dsStatus.getStatus()) {
      case INIT_STATUS:
      case OK_STATUS:
        datasourceStatus.setStatus(OK_STATUS);
        this.hbStatus.setErrorCount(0);
        break;
      case ERROR_STATUS:
        datasourceStatus.setStatus(INIT_STATUS);
        this.hbStatus.setErrorCount(0);
        break;
      case TIMEOUT_STATUS:
        datasourceStatus.setStatus(INIT_STATUS);
        this.hbStatus.setErrorCount(0);
        break;
      default:
        datasourceStatus.setStatus(OK_STATUS);
    }
    sendDataSourceStatus(datasourceStatus);
  }

  protected void setTimeout(DatasourceStatus datasourceStatus) {
    this.hbStatus.incrementErrorCount();
    setTaskquitDetector();
    if (this.hbStatus.getErrorCount() >= this.hbStatus.getMaxRetry()) {
      datasourceStatus.setStatus(DatasourceEnum.TIMEOUT_STATUS);
      sendDataSourceStatus(datasourceStatus);
      this.hbStatus.setErrorCount(0);
    }
  }

  public abstract void heartbeat();

  public abstract void sendDataSourceStatus(DatasourceStatus status);

  public abstract void setTaskquitDetector();

  public double getSlaveThreshold() {
    return slaveThreshold;
  }

  public PhysicsInstance instance() {
    return instance;
  }
}