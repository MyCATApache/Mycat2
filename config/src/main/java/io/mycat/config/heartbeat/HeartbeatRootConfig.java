package io.mycat.config.heartbeat;

import io.mycat.config.ConfigurableRoot;

/**
 * @author jamie12221
 *  date 2019-05-23 16:24
 **/
public class HeartbeatRootConfig implements ConfigurableRoot {

  HeartbeatConfig heartbeat;

  public HeartbeatConfig getHeartbeat() {
    return heartbeat;
  }

  public void setHeartbeat(HeartbeatConfig heartbeat) {
    this.heartbeat = heartbeat;
  }
}
