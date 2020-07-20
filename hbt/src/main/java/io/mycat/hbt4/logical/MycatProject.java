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

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.*;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Implementation of {@link Project} in
 * {@link MycatConvention Mycat calling convention}.
 */
public class MycatProject
        extends Project
        implements MycatRel {
    public MycatProject(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            List<? extends RexNode> projects,
            RelDataType rowType) {
        super(cluster, traitSet, ImmutableList.of(), input, projects, rowType);
        assert getConvention() instanceof MycatConvention;
    }

    /** Creates an MycatProject, specifying row type rather than field
     * names. */
    public static MycatProject create(final RelNode input,
                                           final List<? extends RexNode> projects, RelDataType rowType) {
        final RelOptCluster cluster = input.getCluster();
        final RelMetadataQuery mq = cluster.getMetadataQuery();
        final RelTraitSet traitSet =
                cluster.traitSet().replace(MycatConvention.INSTANCE)
                        .replaceIfs(RelCollationTraitDef.INSTANCE,
                                () -> RelMdCollation.project(mq, input, projects));
        return new MycatProject(cluster, traitSet, input, projects, rowType);
    }


    @Override
    public MycatProject copy(RelTraitSet traitSet, RelNode input,
                             List<RexNode> projects, RelDataType rowType) {
        return new MycatProject(getCluster(), traitSet, input, projects, rowType);
    }


    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        writer.name("MycatProject").item("projects", this.exps).into();
        ((MycatRel) getInput()).explain(writer);
        return writer.ret();
    }


    @Override
    public Executor implement(ExecutorImplementor implementor) {
        return implementor.implement(this);
    }
}