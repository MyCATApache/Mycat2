package io.mycat.config;

import lombok.Data;

import java.util.concurrent.TimeUnit;

@Data
public class TimerConfig {
    private long initialDelay = 1 * 1000L;
    private long period = 10 * 1000L;
    private String timeUnit = TimeUnit.MILLISECONDS.name();
}

