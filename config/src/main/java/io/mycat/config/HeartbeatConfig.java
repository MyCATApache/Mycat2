package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

@Data
@Builder
@AllArgsConstructor
public class HeartbeatConfig {
    private int maxRetryCount;
    private long minSwitchTimeInterval;
    private long heartbeatTimeout;
    private double slaveThreshold;
    private boolean showLog = true;

    public HeartbeatConfig() {
    }
}