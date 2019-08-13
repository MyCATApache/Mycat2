package io.mycat.replica.heartbeat;

import io.mycat.api.collector.CommonSQLCallback;

public abstract class HeartBeatStrategy implements CommonSQLCallback {

  protected HeartbeatFlow heartbeatFlow;

  public void setHeartbeatFlow(HeartbeatFlow heartbeatFlow) {
    this.heartbeatFlow = heartbeatFlow;
  }
}