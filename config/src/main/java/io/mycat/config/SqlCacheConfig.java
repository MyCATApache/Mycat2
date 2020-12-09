package io.mycat.config;

import lombok.EqualsAndHashCode;
import lombok.Getter;

@EqualsAndHashCode
@Getter
public class SqlCacheConfig {
    String name;
    String sql;
    int refreshInterval;
    String refreshIntervalUnit;
    int initialDelay;
    String initialDelayUnit;
}
