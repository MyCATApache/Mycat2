package io.mycat.hbt3;

import io.mycat.hbt4.MycatRel;
import io.mycat.hbt4.Plan;
import io.mycat.hbt4.PlanCache;
import io.mycat.hbt4.PlanImpl;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

import java.util.List;

public class OptimizationContext {
    boolean predicateOnPhyView = false;
    boolean complex = false;
    boolean predicateOnView = false;
    public final List<Object> params;

    public OptimizationContext(List<Object> params, PlanCache planCache) {
        this.params = params;
        this.planCache = planCache;
    }

    final PlanCache planCache;

    public void saveAlways(String p, MycatRel mycatRel) {
        RelOptCost relOptCost = getDefaultRelOptCost(mycatRel);
        planCache.put(p, new PlanImpl(Plan.Type.FINAL, relOptCost, mycatRel));
    }

    private RelOptCost getDefaultRelOptCost(RelNode mycatRel) {
        RelOptCluster cluster = mycatRel.getCluster();
        RelOptPlanner planner = cluster.getPlanner();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        return mycatRel.computeSelfCost(planner, mq);
    }

    public void setPredicateOnPhyView(boolean b) {
        this.predicateOnPhyView = b;
    }

    public void setComplex(boolean complex) {
        this.complex = complex;
    }

    public void setPredicateOnView(boolean b) {
        this.predicateOnView = b;
    }

    public void saveParameterized(String drdsSql, MycatRel mycatRel) {
        saveAlways(drdsSql, mycatRel);
    }

    public void saveParse(String drdsSql, RelNode o) {
        planCache.put(drdsSql, new PlanImpl(Plan.Type.PARSE, getDefaultRelOptCost(o), o));
    }
}