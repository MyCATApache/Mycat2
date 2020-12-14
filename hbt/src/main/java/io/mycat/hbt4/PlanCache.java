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

import java.util.PriorityQueue;
import java.util.concurrent.TimeUnit;

public enum PlanCache {
    INSTANCE;

    private LoadingCache<String, PriorityQueue<Plan>> cache;

    PlanCache() {
        this.cache = newCache();
    }

    @NotNull
    private LoadingCache<String, PriorityQueue<Plan>> newCache() {
        LoadingCache<String, PriorityQueue<Plan>> cache = CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(10, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, PriorityQueue<Plan>>() {
                            public PriorityQueue<Plan> load(String key) {
                                return new PriorityQueue<Plan>(Comparable::compareTo);
                            }
                        });
        return cache;
    }


    public Plan getMinCostPlan(String sql) {
        PriorityQueue<Plan> plans = computeIfAbsent(sql);
        if (plans == null) return null;
        synchronized (plans) {
            if (plans.isEmpty()) {
                return null;
            } else {
                return plans.peek();
            }
        }
    }

    @SneakyThrows
    private PriorityQueue<Plan> computeIfAbsent(String sql) {
        return this.cache.get(sql, () -> new PriorityQueue<Plan>(Comparable::compareTo));
    }

    public void put(String sql, Plan plan) {
        PriorityQueue<Plan> plans = computeIfAbsent(sql);
        synchronized (plans) {
            plans.add(plan);
        }
    }

    public void clear() {
        if (this.cache != null) {
            this.cache.cleanUp();
            this.cache = newCache();
        }
    }
}