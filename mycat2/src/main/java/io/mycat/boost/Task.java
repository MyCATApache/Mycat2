package io.mycat.boost;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public abstract class Task {
    final CacheConfig cacheConfig;

    public CacheConfig config() {
        return cacheConfig;
    }

    public  void start() {
        start(cacheConfig);
    }

    public abstract void start(CacheConfig cacheConfig);

    public  void cache() {
        cache(cacheConfig);
    }

    public abstract void cache(CacheConfig cacheConfig);

    public <T> T get() {
        return (T) get(cacheConfig);
    }

    public  abstract <T> T get(CacheConfig cacheConfig);
}
