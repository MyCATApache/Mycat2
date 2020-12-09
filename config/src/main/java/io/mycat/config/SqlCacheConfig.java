package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.concurrent.TimeUnit;

@EqualsAndHashCode
@Data
@ToString
public class SqlCacheConfig {
    String name = "ping";
    String sql = "select 'X' ";
    long refreshInterval =  TimeUnit.MINUTES.toSeconds(1);
    long initialDelay =  TimeUnit.MINUTES.toSeconds(0);
    String timeUnit = TimeUnit.SECONDS.name();
}
