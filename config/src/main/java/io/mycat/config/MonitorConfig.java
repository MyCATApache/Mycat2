package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.concurrent.TimeUnit;

@Data
@EqualsAndHashCode
public class MonitorConfig {
    String ip;
    int port;
    SqlLogConfig sqlLogConfig = new SqlLogConfig();
    TimerConfig instanceMonitorConfig = createOneSecondTimeConfig();
    TimerConfig readWriteRatioConfig = createOneSecondTimeConfig();

    public static TimerConfig createOneSecondTimeConfig() {
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setInitialDelay(1);
        timerConfig.setPeriod(1);
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        return timerConfig;
    }
}
