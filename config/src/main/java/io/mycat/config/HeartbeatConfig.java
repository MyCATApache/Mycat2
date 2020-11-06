package io.mycat.config;

import lombok.Data;

@Data
public class HeartbeatConfig {
    private int maxRetry;
    private long minSwitchTimeInterval;
    private long heartbeatTimeout;
    private long slaveThreshold;
}