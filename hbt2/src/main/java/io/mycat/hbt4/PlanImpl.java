package io.mycat.hbt4;

import org.apache.calcite.plan.RelOptCost;
import org.jetbrains.annotations.NotNull;

public class PlanImpl implements Plan {
    private final RelOptCost relOptCost;
    private final MycatRel relNode1;

    public PlanImpl(RelOptCost relOptCost, MycatRel relNode1) {

        this.relOptCost = relOptCost;
        this.relNode1 = relNode1;
    }

    @Override
    public int compareTo(@NotNull Plan o) {
        return this.relOptCost.isLt(o.getRelOptCost())?1:-1;
    }

    @Override
    public RelOptCost getRelOptCost() {
        return relOptCost;
    }
    @Override
    public MycatRel getRelNode() {
        return relNode1;
    }
}