package io.mycat.optimizer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public enum PlanCache {
    INSTANCE;
    Cache<String, Plan> cache = CacheBuilder.newBuilder()
            .maximumSize(65535)
            .build();

    public Plan get(String sql) {
        return cache.getIfPresent(sql);
    }

    public void put(String sql,Plan plan){
        cache.put(sql,plan);
    }

}