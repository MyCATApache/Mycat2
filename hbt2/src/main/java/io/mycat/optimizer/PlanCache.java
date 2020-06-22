package io.mycat.optimizer;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.SneakyThrows;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public enum PlanCache {
    INSTANCE;
    Cache<String, Plan> cache = CacheBuilder.newBuilder()
            .maximumSize(65535)
            .build();

    public Plan get(String sql) {
        return cache.getIfPresent(sql);
    }
    @SneakyThrows
    public Plan get(String sql, Callable<Plan> callable) {
        return cache.get(sql,callable);
    }
    public void put(String sql,Plan plan){
        cache.put(sql,plan);
    }

}