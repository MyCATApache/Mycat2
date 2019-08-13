package io.mycat.replica.heartbeat;

import io.mycat.api.collector.CommonSQLCallback;

public abstract class HeartBeatStrategy implements CommonSQLCallback {

  protected HeartbeatFlow heartbeatFlow;
  protected volatile boolean quit = false;

  public HeartBeatStrategy() {
  }

  public HeartBeatStrategy(HeartbeatFlow heartbeatFlow) {
    this.heartbeatFlow = heartbeatFlow;
  }

  public void setHeartbeatFlow(HeartbeatFlow heartbeatFlow) {
    this.heartbeatFlow = heartbeatFlow;
  }

  public boolean isQuit() {
    return quit;
  }

  public void setQuit(boolean quit) {
    this.quit = quit;
  }

  public void onStatus(int status) {
    if (heartbeatFlow != null && !quit) {
      heartbeatFlow.setStatus(status);
    }
  }
}