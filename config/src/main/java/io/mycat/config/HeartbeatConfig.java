package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class HeartbeatConfig {
    private int maxRetry;
    private long minSwitchTimeInterval;
    private long heartbeatTimeout;
    private double slaveThreshold;

    public HeartbeatConfig() {
    }
}