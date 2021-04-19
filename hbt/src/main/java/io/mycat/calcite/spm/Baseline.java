package io.mycat.calcite.spm;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

@Getter
@ToString
@EqualsAndHashCode
public class Baseline {
    long baselineId;
    String sql;
    Constraint constraint;

    Set<BaselinePlan> planList;
    BaselinePlan fixPlan;
    ExtraConstraint extraConstraint;

    public Baseline(long baselineId, String sql, Constraint constraint, Set<BaselinePlan> planList, BaselinePlan fixPlan, ExtraConstraint extraConstraint) {
        this.baselineId = baselineId;
        this.sql = sql;
        this.planList = new CopyOnWriteArraySet<>(planList);
        this.fixPlan = fixPlan;
        this.constraint = constraint;
        this.extraConstraint = extraConstraint;
    }

    public Baseline(long baselineId, String sql, Constraint constraint, BaselinePlan fixPlan, ExtraConstraint extraConstraint) {
        this(baselineId, sql, constraint, Collections.emptySet(), fixPlan,extraConstraint);
    }

    public void removePlanById(long planId) {
        planList.removeIf(baselinePlan -> baselinePlan.id == planId);
    }
}
