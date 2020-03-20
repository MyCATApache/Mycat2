package io.mycat.boost;

import lombok.Data;
import lombok.EqualsAndHashCode;

@Data
@EqualsAndHashCode
public class CacheConfig {
    private Duration refreshInterval;
    private Duration initialDelay;
    private String text;
    private String command;
}