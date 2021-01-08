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
package io.mycat.calcite.spm;

import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.CodeExecuterContext;
import org.jetbrains.annotations.NotNull;

public class PlanImpl implements Plan {
    private  boolean forUpdate;
    private final Type type;
    private final RelOptCost relOptCost;
    private final RelNode logical;
    private final RelNode physical;
    private final CodeExecuterContext executerContext;


    public static PlanImpl of(RelNode relNode,
                              CodeExecuterContext executerContext,boolean forUpdate) {
        return new PlanImpl(relNode, executerContext,forUpdate);
    }

    public static PlanImpl of(RelNode relNode1) {
        return new PlanImpl(relNode1);
    }

    public PlanImpl(RelNode relNode,
                    CodeExecuterContext executerContext,boolean forUpdate) {
        this.forUpdate = forUpdate;
        this.type = Type.PHYSICAL;
        RelOptCluster cluster = relNode.getCluster();
        this.relOptCost = relNode.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery());
        this.logical = null;
        this.physical = relNode;
        this.executerContext = executerContext;
    }

    public PlanImpl(MycatInsertRel relNode) {
        this.type = Type.INSERT;
        RelOptCluster cluster = relNode.getCluster();
        this.relOptCost = cluster.getPlanner().getCostFactory().makeZeroCost();
        this.logical = null;
        this.physical = relNode;
        this.executerContext = null;
    }

    public PlanImpl(MycatUpdateRel relNode) {
        this.type = Type.UPDATE;
        RelOptCluster cluster = relNode.getCluster();
        this.relOptCost = cluster.getPlanner().getCostFactory().makeZeroCost();
        this.logical = null;
        this.physical = relNode;
        this.executerContext = null;
    }

    public PlanImpl(
            RelNode relNode1) {
        this.type = Type.LOGICAL;
        RelOptCluster cluster = relNode1.getCluster();
        this.relOptCost = relNode1.computeSelfCost(cluster.getPlanner(), cluster.getMetadataQuery());
        ;
        this.logical = relNode1;
        this.executerContext = null;
        this.physical = null;
    }

    @Override
    public int compareTo(@NotNull Plan o) {
        return this.relOptCost.isLt(o.getRelOptCost()) ? 1 : -1;
    }

    @Override
    public boolean forUpdate() {
        return forUpdate;
    }

    @Override
    public RelOptCost getRelOptCost() {
        return relOptCost;
    }

    @Override
    public Type getType() {
        return type;
    }

    public RelNode getLogical() {
        return logical;
    }

    @Override
    public CodeExecuterContext getCodeExecuterContext() {
        return executerContext;
    }

    public RelNode getPhysical() {
        return physical;
    }
}