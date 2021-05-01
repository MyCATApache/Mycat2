/**
 * Copyright (C) <2021>  <chen junwen>
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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;

import java.util.List;
import java.util.Objects;

public class MycatGather extends SingleRel implements MycatRel {

    protected MycatGather(RelOptCluster cluster, RelTraitSet traitSet, RelNode input) {
        super(cluster, Objects.requireNonNull(traitSet).replace(MycatConvention.INSTANCE), input);
    }


    public static MycatGather create(RelNode input) {
        RelOptCluster cluster = input.getCluster();
        return new MycatGather(cluster, input.getTraitSet().replace(MycatConvention.INSTANCE), input);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatGather");
        for (RelNode input : getInputs()) {
            MycatRel mycatRel = (MycatRel) input;
            mycatRel.explain(writer);
        }
        return writer.ret();
    }


    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MycatGather(getCluster(), traitSet, inputs.get(0));
    }

    @Override
    public boolean isSupportStream() {
        return true;
    }
}