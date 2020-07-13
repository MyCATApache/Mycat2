package io.mycat.hbt4;

import org.apache.calcite.plan.RelOptCost;

public interface Plan extends Comparable<Plan> {
    RelOptCost getRelOptCost();
    public MycatRel getRelNode();
}