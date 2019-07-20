package io.mycat.replica.heartbeat;

import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.heartbeat.proxyDetector.ProxyHeartBeatAsyncTaskCallBack;

public class NoneHeartbeatDetector implements HeartbeatDetector {

  @Override
  public ReplicaConfig getReplica() {
    return null;
  }

  @Override
  public MySQLDatasource getDataSource() {
    return null;
  }

  @Override
  public HeartbeatManager getHeartbeatManager() {
    return null;
  }

  @Override
  public void heartBeat() {

  }

  @Override
  public ProxyHeartBeatAsyncTaskCallBack getCallback() {
    return null;
  }

  @Override
  public boolean isHeartbeatTimeout() {
    return false;
  }

  @Override
  public void updateLastReceivedQryTime() {

  }

  @Override
  public void updateLastSendQryTime() {

  }

  @Override
  public boolean quitDetector() {
    return true;
  }
}