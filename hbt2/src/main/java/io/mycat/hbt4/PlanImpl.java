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