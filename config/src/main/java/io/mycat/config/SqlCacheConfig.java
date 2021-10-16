package io.mycat.config;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.concurrent.TimeUnit;

@EqualsAndHashCode
@Data
@ToString
public class SqlCacheConfig implements KVObject{
    String name = "ping";
    String sql = "select 'X' ";
    long refreshInterval = TimeUnit.MINUTES.toSeconds(1);
    long initialDelay = TimeUnit.MINUTES.toSeconds(0);
    String timeUnit = TimeUnit.SECONDS.name();

    @Override
    public String keyName() {
        return name;
    }
    @Override
    public String path() {
        return "sqlcaches";
    }

    @Override
    public String fileName() {
        return "sqlcache";
    }
}
