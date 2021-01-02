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
package io.mycat.calcite.physical;

import io.mycat.calcite.Executor;
import io.mycat.calcite.ExecutorImplementor;
import io.mycat.calcite.ExplainWriter;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.logical.MycatView;
import lombok.Getter;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

@Getter
public class MycatQuery extends AbstractRelNode implements MycatRel {


    private final MycatView view;

    public MycatQuery(MycatView view) {
        super(view.getCluster(), view.getTraitSet());
        this.view = view;
        this.rowType = view.getRowType();
        this.traitSet = view.getTraitSet();
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        return null;
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return super.computeSelfCost(planner, mq).multiplyBy(0.1);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        throw new UnsupportedOperationException();
    }
}