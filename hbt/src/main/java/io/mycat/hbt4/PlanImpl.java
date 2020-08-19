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

import com.google.common.collect.ImmutableList;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.rel.RelNode;
import org.jetbrains.annotations.NotNull;

import java.util.Objects;

public class PlanImpl implements Plan {
    private final Type type;
    private final RelOptCost relOptCost;
    private final RelNode relNode1;

    public PlanImpl(Type type, RelOptCost relOptCost, RelNode relNode1) {
        this.type = type;
        this.relOptCost = relOptCost;
        this.relNode1 = Objects.requireNonNull(relNode1);
    }

    @Override
    public int compareTo(@NotNull Plan o) {
        return this.relOptCost.isLt(o.getRelOptCost()) ? 1 : -1;
    }

    @Override
    public RelOptCost getRelOptCost() {
        return relOptCost;
    }


    @Override
    public MycatRowMetaData rowMetaData() {
        return null;
    }

    @Override
    public Type getType() {
        return type;
    }

    public RelNode getRelNode() {
        return relNode1;
    }
}