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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

/**
 * Implementation of {@link Filter} in
 * {@link MycatConvention Mycat calling convention}.
 */
public class MycatFilter extends Filter implements MycatRel {
    protected MycatFilter(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexNode condition) {
        super(cluster, traitSet, input, condition);
        assert getConvention() instanceof MycatConvention;
    }
    public static MycatFilter  create(
            RelTraitSet traitSet,
            RelNode input,
            RexNode condition) {
        RelOptCluster cluster = input.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();
        traitSet = traitSet.replace(MycatConvention.INSTANCE);
        traitSet = traitSet.replaceIfs(RelCollationTraitDef.INSTANCE,
                () -> RelMdCollation.filter(mq, input));
        return new MycatFilter(cluster,traitSet,input,condition);
    }
    public MycatFilter copy(RelTraitSet traitSet, RelNode input,
                            RexNode condition) {
        return new MycatFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatFilter").item("condition", condition).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }


    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}