package io.mycat.config;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import java.util.concurrent.TimeUnit;

@Data
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class TimerConfig {
    private long initialDelay = 30;
    private long period = 5;
    private String timeUnit = TimeUnit.SECONDS.name();
}

