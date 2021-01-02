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


import io.mycat.calcite.*;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableMinus;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;

import java.util.List;

import static io.mycat.calcite.physical.MycatIntersect.convertList;

/**
 * Minus operator implemented in Mycat convention.
 */
public class MycatMinus extends Minus implements MycatRel {
    protected MycatMinus(RelOptCluster cluster, RelTraitSet traitSet,
                         List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    public static MycatMinus create(RelTraitSet traitSet,
                                    List<RelNode> inputs, boolean all) {
        return new MycatMinus(inputs.get(0).getCluster(), traitSet.replace(MycatConvention.INSTANCE), inputs, all);
    }

    public MycatMinus copy(RelTraitSet traitSet, List<RelNode> inputs,
                           boolean all) {
        return new MycatMinus(getCluster(), traitSet, inputs, all);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatMinus").into();
        for (RelNode input : getInputs()) {
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
        final Minus minus = (Minus) this;
        final EnumerableConvention out = EnumerableConvention.INSTANCE;
        final RelTraitSet traitSet =
                this.getTraitSet().replace(
                        EnumerableConvention.INSTANCE);

        EnumerableRel res = (EnumerableRel) new EnumerableMinus(this.getCluster(), traitSet,
                convertList(minus.getInputs(), out), minus.all);
        return res.implement(implementor, pref);
    }
}