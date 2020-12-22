/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.hbt4;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public enum PlanCache {
    INSTANCE;

    private LoadingCache<String, AtomicReference<Plan>> cache;

    PlanCache() {
        this.cache = newCache();
    }

    @NotNull
    private LoadingCache<String, AtomicReference<Plan>> newCache() {
        LoadingCache<String, AtomicReference<Plan>> cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, AtomicReference<Plan>>() {
                            public AtomicReference<Plan> load(String key) {
                                return new AtomicReference<Plan>();
                            }
                        });
        return cache;
    }


    public Plan getMinCostPlan(String sql) {
        AtomicReference<Plan> plans = computeIfAbsent(sql);
        return plans.get();
    }

    @SneakyThrows
    private AtomicReference<Plan> computeIfAbsent(String sql) {
        return this.cache.get(sql, () -> new AtomicReference<Plan>());
    }

    public void put(String sql, Plan update) {
        AtomicReference<Plan> plans = computeIfAbsent(sql);
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