package io.mycat.hbt4;

import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

public enum PlanCache {
    INSTANCE;
    ConcurrentHashMap<String, PriorityQueue<Plan>> cache = new ConcurrentHashMap<>();

    public Plan getMinCostPlan(String sql) {
        PriorityQueue<Plan> plans = cache.computeIfAbsent(sql, s -> new PriorityQueue<>(Comparable::compareTo));
        if (plans.isEmpty()){
            return null;
        }else {
            return plans.poll();
        }
    }
    public void put(String sql,Plan plan){
        PriorityQueue<Plan> plans = cache.computeIfAbsent(sql, s -> new PriorityQueue<>(Comparable::compareTo));
        plans.add(plan);
    }

}