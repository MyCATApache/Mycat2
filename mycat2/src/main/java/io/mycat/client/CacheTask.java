package io.mycat.client;

import io.mycat.boost.CacheConfig;
import lombok.Getter;

@Getter
public class CacheTask {
    final String name;
    final String text;
    final Type type;
    final CacheConfig cacheConfig;

    public CacheTask(String name, String text, Type type, CacheConfig cacheConfig) {
        this.name = name;
        this.text = text;
        this.type = type;
        this.cacheConfig = cacheConfig;
    }
}