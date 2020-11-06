package io.mycat.config;

import lombok.Builder;
import lombok.Data;
import lombok.ToString;

import java.util.concurrent.TimeUnit;

/**
 * int corePoolSize,
 * int maximumPoolSize,
 * long keepAliveTime,
 * TimeUnit unit
 */
@Data
@ToString
@Builder
public class ThreadPoolExecutorConfig {
    private int corePoolSize = 0;
    private int maxPoolSize = 1024;
    private long keepAliveTime = 60;
    private long taskTimeout = 600;
    private String timeUnit = TimeUnit.SECONDS.toString();
    private int maxPendingLimit = 65535;

    public ThreadPoolExecutorConfig() {
    }

    public ThreadPoolExecutorConfig(int corePoolSize, int maxPoolSize, long keepAliveTime, long taskTimeout, String timeUnit, int maxPendingLimit) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.keepAliveTime = keepAliveTime;
        this.taskTimeout = taskTimeout;
        this.timeUnit = timeUnit;
        this.maxPendingLimit = maxPendingLimit;
    }
}