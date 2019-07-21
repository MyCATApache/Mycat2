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

/**
 * @author : zhangwy
 * @author : chenjunwen
 * @version V1.1 心跳流程控制
 */
public abstract class HeartbeatManager {

  protected volatile DatasourceStatus dsStatus;
  protected HeartBeatStatus hbStatus;
  protected HeartbeatDetector heartbeatDetector;


  /**
   * 1: 发送sql 判断是否已经发送 如果还未发送 则发送sql  设置为发送中。。 如果为发送中。。 判断是否已经超时, 超时则设置超时状态  设置为未发送 如果发生错误, 则设置错误状态.
   * 设置为未发送 2. 检查结果集 --&gt; 设置结果集状态 设置为未发送 设置失败集状态 设置为未发送 设置超时状态
   */
  public void heartBeat() {
    if (hbStatus.tryChecking()) {
      this.heartbeatDetector.heartBeat();
    } else if (this.heartbeatDetector.isHeartbeatTimeout()) {
      DatasourceStatus datasourceStatus = new DatasourceStatus();
      datasourceStatus.setStatus(DatasourceStatus.TIMEOUT_STATUS);
      this.setStatus(datasourceStatus, DatasourceStatus.TIMEOUT_STATUS);
    }
  }


  protected abstract void sendDataSourceStatus(DatasourceStatus datasourceStatus);

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
    this.heartbeatDetector.updateLastReceivedQryTime();
    this.hbStatus.setChecking(false);
  }

  protected void setTimeout(DatasourceStatus datasourceStatus) {
    this.hbStatus.incrementErrorCount();
    this.heartbeatDetector.quitDetector();
    if (this.hbStatus.getErrorCount() == this.hbStatus.getMaxRetry()) {
      datasourceStatus.setStatus(DatasourceStatus.TIMEOUT_STATUS);
      sendDataSourceStatus(datasourceStatus);
      this.hbStatus.setErrorCount(0);
    }
  }


  protected void setError(DatasourceStatus datasourceStatus) {
    this.hbStatus.incrementErrorCount();
    this.heartbeatDetector.quitDetector();
    if (this.hbStatus.getErrorCount() == this.hbStatus.getMaxRetry()) {
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


  protected boolean canSwitchDataSource() {
    return this.hbStatus.getLastSwitchTime() + this.hbStatus.getMinSwitchTimeInterval() < System
        .currentTimeMillis();
  }

  public DatasourceStatus getDsStatus() {
    return dsStatus;
  }


  protected void setDsStatus(DatasourceStatus dsStatus) {
    this.dsStatus = dsStatus;
  }


  protected void setHbStatus(HeartBeatStatus hbStatus) {
    this.hbStatus = hbStatus;
  }


  public void setHeartbeatDetector(HeartbeatDetector heartbeatDetector) {
    this.heartbeatDetector = heartbeatDetector;
  }
}
