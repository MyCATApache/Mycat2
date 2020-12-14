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
package io.mycat.hbt4.logical.rel;


import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rex.RexNode;

public class MycatTopN extends Sort implements MycatRel {

    protected MycatTopN(RelOptCluster cluster, RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        super(cluster, traits, child, collation, offset, fetch);
    }

    public static MycatTopN create(RelTraitSet traits, RelNode child, RelCollation collation, RexNode offset, RexNode fetch) {
        RelOptCluster cluster = child.getCluster();
        return new MycatTopN(
                cluster,
                traits.replace(MycatConvention.INSTANCE),
                child,
                collation,
                offset,
                fetch
        );
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatTopN").into();
        MycatRel input = (MycatRel) this.getInput();

        if (fetch != null) {
            writer.item("fetch", fetch);
        }

        if (offset != null) {
            writer.item("offset", offset);
        }

        input.explain(writer);
        return writer.ret();
    }

    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }

    @Override
    public Sort copy(RelTraitSet traitSet, RelNode newInput, RelCollation newCollation, RexNode offset, RexNode fetch) {
        return new MycatTopN(getCluster(), traitSet, newInput, newCollation, offset, fetch);
    }
}