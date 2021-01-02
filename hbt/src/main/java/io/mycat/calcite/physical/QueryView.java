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
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Union;

import java.util.List;

public class QueryView extends Union implements MycatRel {
    protected QueryView(RelOptCluster cluster, RelTraitSet traits, List<RelNode> inputs) {
        super(cluster, traits, inputs, true);
    }

    @Override
    public QueryView copy(RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new QueryView(getCluster(),traitSet,inputs);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
         writer.name("Query").into();
        List<RelNode> inputs = getInputs();
        for (RelNode input : inputs) {
            MycatRel rel = (MycatRel) input;
            rel.explain(writer);
        }
       return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        return null;
    }
}