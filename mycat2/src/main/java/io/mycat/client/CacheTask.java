package io.mycat.client;

import io.mycat.boost.CacheConfig;
import lombok.Getter;

import java.util.Objects;
/**
 * @author Junwen Chen
 **/
@Getter
public class CacheTask {
    final String name;
    final String text;
    final Type type;
    final CacheConfig cacheConfig;

    public CacheTask(String name, String text, Type type, CacheConfig cacheConfig) {
        this.name = Objects.requireNonNull(name);
        this.text = Objects.requireNonNull(text);
        this.type = Objects.requireNonNull(type);
        this.cacheConfig =Objects.requireNonNull(cacheConfig);
    }
}