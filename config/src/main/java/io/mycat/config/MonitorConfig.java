package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.concurrent.TimeUnit;

@Data
@EqualsAndHashCode
public class MonitorConfig {
    String ip = "localhost";
    int port = 9066;
    boolean open = true;
    SqlLogConfig sqlLog = new SqlLogConfig();
    TimerConfig instanceMonitor = createOneSecondTimeConfig();
    TimerConfig clusterMonitor = createOneSecondTimeConfig();
    TimerConfig databaseInstanceMonitor = createOneSecondTimeConfig();

    public static TimerConfig createOneSecondTimeConfig() {
        TimerConfig timerConfig = new TimerConfig();
        timerConfig.setInitialDelay(30);
        timerConfig.setPeriod(30);
        timerConfig.setTimeUnit(TimeUnit.SECONDS.name());
        return timerConfig;
    }
}
