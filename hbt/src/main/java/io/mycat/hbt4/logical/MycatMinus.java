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
package io.mycat.hbt4.logical;



import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExecutorImplementor;
import io.mycat.hbt4.ExplainWriter;
import io.mycat.hbt4.MycatRel;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Minus;

import java.util.List;

/**
 * Minus operator implemented in Mycat convention.
 */
public class MycatMinus extends Minus implements MycatRel {
    public MycatMinus(RelOptCluster cluster, RelTraitSet traitSet,
                      List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
        assert !all;
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
}