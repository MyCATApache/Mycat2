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

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import lombok.SneakyThrows;
import org.jetbrains.annotations.NotNull;

import java.util.Collections;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;

public enum PlanCache {
    INSTANCE;

    private Cache<String, PriorityQueue<Plan>> cache;

    PlanCache() {
        this.cache = newCache();
    }

    @NotNull
    private Cache<String, PriorityQueue<Plan>> newCache() {
        return CacheBuilder.newBuilder().maximumSize(65535)
                .build();
    }


    public Plan getMinCostPlan(String sql) {
        PriorityQueue<Plan> plans = computeIfAbsent(sql, () -> new PriorityQueue<>(Comparable::compareTo));
        if (plans.isEmpty()) {
            return null;
        } else {
            return plans.poll();
        }
    }

    @SneakyThrows
    private PriorityQueue<Plan> computeIfAbsent(String sql, Supplier<PriorityQueue<Plan>> o) {
        return this.cache.get(sql, new Callable<PriorityQueue<Plan>>() {
            @Override
            public PriorityQueue<Plan> call() throws Exception {
                return o.get();
            }
        });

    }

    public void put(String sql, Plan plan) {
        PriorityQueue<Plan> plans = computeIfAbsent(sql, () -> new PriorityQueue<>(Comparable::compareTo));
        plans.add(plan);
    }

    public void clear() {
        if (this.cache != null) {
            this.cache.cleanUp();
            this.cache = newCache();
        }
    }
}