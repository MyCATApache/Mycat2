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
package io.mycat.replica.heartbeat.proxyDetector;


import io.mycat.api.collector.CommonSQLCallback;
import io.mycat.config.ConfigFile;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.heartbeat.HeartbeatConfig;
import io.mycat.config.heartbeat.HeartbeatRootConfig;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.SessionManager.PartialType;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heartbeat.HeartbeatDetector;
import io.mycat.replica.heartbeat.HeartbeatManager;
import java.util.function.Function;

/**
 * @author : zhangwy
 * @version V1.0
 *
 *  date Date : 2019年05月06日 23:20
 */
public final class DefaultProxyHeartbeatDetector implements
    HeartbeatDetector<MySQLDatasource, ProxyHeartBeatAsyncTaskCallBack> {

  protected final ReplicaConfig replicaConfig;
  protected final MySQLDatasource dataSource;
  protected final HeartbeatManager heartbeatManager;
  protected final ProxyRuntime runtime;
  private final Function<HeartbeatDetector, CommonSQLCallback> commonSQLCallbacbProvider;
  protected volatile long lastSendQryTime;
  protected volatile long lastReceivedQryTime;//    private isCheck
  protected long heartbeatTimeout;
  protected volatile ProxyHeartBeatAsyncTaskCallBack heartBeatAsyncTaskCallBack;

  public DefaultProxyHeartbeatDetector(ProxyRuntime runtime,
      ReplicaConfig replicaConfig, MySQLDataSourceEx dataSource,
      HeartbeatManager heartbeatManager,
      Function<HeartbeatDetector, CommonSQLCallback> commonSQLCallbacbProvider) {
    this.replicaConfig = replicaConfig;
    this.dataSource = dataSource;
    this.heartbeatManager = heartbeatManager;
    this.runtime = runtime;
    HeartbeatRootConfig heartbeatRootConfig = runtime.getConfig(
        ConfigFile.HEARTBEAT);
    HeartbeatConfig heartbeatConfig = heartbeatRootConfig
        .getHeartbeat();
    this.heartbeatTimeout = heartbeatConfig.getMinHeartbeatChecktime();
    this.commonSQLCallbacbProvider = commonSQLCallbacbProvider;
  }

  @Override
  public ProxyHeartBeatAsyncTaskCallBack getCallback() {
    return new ProxyHeartBeatAsyncTaskCallBack(this, commonSQLCallbacbProvider.apply(this));
  }


  public void heartBeat() {
    heartBeatAsyncTaskCallBack = getCallback();
    MySQLTaskUtil
        .getMySQLSessionForTryConnectFromUserThread(runtime, dataSource, null,
            PartialType.SMALL_ID, heartBeatAsyncTaskCallBack);
  }


  @Override
  public boolean isHeartbeatTimeout() {
    return System.currentTimeMillis() > Math.max(lastSendQryTime,
        lastReceivedQryTime) + heartbeatTimeout;
  }

  @Override
  public void updateLastReceivedQryTime() {
    this.lastReceivedQryTime = System.currentTimeMillis();
  }

  @Override
  public void updateLastSendQryTime() {
    this.lastSendQryTime = System.currentTimeMillis();

  }

  public boolean quitDetector() {
    heartBeatAsyncTaskCallBack.setQuit(true);
    return true;
  }


  @Override
  public ReplicaConfig getReplicaConfig() {
    return replicaConfig;
  }

  @Override
  public MySQLDatasource getDataSource() {
    return dataSource;
  }

  @Override
  public HeartbeatManager getHeartbeatManager() {
    return heartbeatManager;
  }
}
