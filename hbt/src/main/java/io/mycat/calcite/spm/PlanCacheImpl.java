package io.mycat.calcite.spm;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.sql.type.SqlTypeName;
import org.jetbrains.annotations.NotNull;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class PlanCacheImpl implements PlanCache{

    private LoadingCache<PlanCacheImpl.Key, AtomicReference<Plan>> cache;

   public PlanCacheImpl() {
        this.cache = newCache();
    }

    @NotNull
    private LoadingCache<PlanCacheImpl.Key, AtomicReference<Plan>> newCache() {
        LoadingCache<PlanCacheImpl.Key, AtomicReference<Plan>> cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<PlanCacheImpl.Key, AtomicReference<Plan>>() {
                            public AtomicReference<Plan> load(PlanCacheImpl.Key key) {
                                return new AtomicReference<Plan>();
                            }
                        });
        return cache;
    }
    public Plan getMinCostPlan(String sql, List<SqlTypeName> types) {
        AtomicReference<Plan> plans = computeIfAbsent(new PlanCacheImpl.Key(sql,types));
        return plans.get();
    }

    public Plan getMinCostPlan(PlanCacheImpl.Key sql) {
        AtomicReference<Plan> plans = computeIfAbsent(sql);
        return plans.get();
    }

    @SneakyThrows
    private AtomicReference<Plan> computeIfAbsent(PlanCacheImpl.Key sql) {
        return this.cache.get(sql, () -> new AtomicReference<Plan>());
    }

    @EqualsAndHashCode
    @Getter
    public static class Key {
        private String sql;
        private List<SqlTypeName> types;

        public Key(String sql, List<SqlTypeName> types) {
            this.sql = sql;
            this.types = types;
        }
    }

    public void put(String sql, List<SqlTypeName> types, Plan update) {
        AtomicReference<Plan> plans = computeIfAbsent(new PlanCacheImpl.Key(sql,types));
        plans.updateAndGet(plan -> {
            if (plan == null) {
                return update;
            }
            if (plan.compareTo(update) <= 0) {
                return plan;
            }
            return update;
        });
    }

    public void clear() {
        if (this.cache != null) {
            this.cache.cleanUp();
            this.cache = newCache();
        }
    }



}
